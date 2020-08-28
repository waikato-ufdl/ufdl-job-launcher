from glob import glob
import shlex
from ufdl.joblauncher import AbstractDockerJobExecutor
from ufdl.pythonclient.functional.image_classification.dataset import download as dataset_download

KEY_GENERATE_STATS = 'generate-stats'


class ImageClassificationTrain_TF_1_14(AbstractDockerJobExecutor):
    """
    For executing Tensorflow image classification jobs.
    """

    def __init__(self, context, work_dir, cache_dir, use_sudo=False, ask_sudo_pw=False, use_current_user=True):
        """
        Initializes the executor with the backend context.

        :param context: the server context
        :type context: UFDLServerContext
        :param work_dir: the working directory to use
        :type work_dir: str
        :param cache_dir: the cache directory to use for models etc
        :type cache_dir: str
        :param use_sudo: whether to prefix commands with sudo
        :type use_sudo: bool
        :param ask_sudo_pw: whether to prompt user in console for sudo password
        :type ask_sudo_pw: bool
        """
        super(ImageClassificationTrain_TF_1_14, self).__init__(
            context, work_dir, cache_dir, use_sudo=use_sudo, ask_sudo_pw=ask_sudo_pw, use_current_user=use_current_user)

    def _pre_run(self, template, job):
        """
        Hook method before the actual job is run.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        :return: whether successful
        :rtype: bool
        """
        if not super()._pre_run(template, job):
            return False

        # download dataset
        data = self.job_dir + "/data.zip"
        pk = int(self._input("data", job, template)["value"])
        options = self._input("data", job, template)["options"]
        self._log_msg("Downloading dataset:", pk, "-> options='" + str(options) + "'", "->", data)
        with open(data, "wb") as zip_file:
            for b in dataset_download(self.context, pk, annotations_args=options):
                zip_file.write(b)

        # decompress dataset
        output_dir = self.job_dir + "/data"
        msg = self._decompress(data, output_dir)
        if msg is not None:
            raise Exception("Failed to extract dataset pk=%d!\n%s" % (pk, msg))

        # create remaining directories
        self._mkdir(self.job_dir + "/output")
        self._mkdir(self.job_dir + "/models")
        return True

    def _do_run(self, template, job):
        """
        Executes the actual job. Only gets run if pre-run was successful.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        """

        image = self._docker_image['url']
        volumes=[
            self.job_dir + "/data" + ":/data",
            self.job_dir + "/output" + ":/output",
            self.cache_dir + ":/models",
        ]

        # build model
        res = self._run_image(
            image,
            volumes=volumes,
            image_args=shlex.split(self._expand_template(job, template))
        )

        # stats?
        if (res is None) and bool(self._parameter(KEY_GENERATE_STATS, job, template)['value']):
            for t in ["training", "testing", "validation"]:
                self._run_image(
                    image,
                    volumes=volumes,
                    image_args=[
                        "tfic-stats",
                        "--image_dir", "/data",
                        "--image_list", "/output/%s.json" % t,
                        "--graph", "/output/graph.pb",
                        "--info", "/output/info.json",
                        "--output_preds", "/output/%s-predictions.csv" % t,
                        "--output_stats", "/output/%s-stats.csv" % t,
                    ]
                )


    def _post_run(self, template, job, pre_run_success, do_run_success):
        """
        Hook method after the actual job has been run. Will always be executed.

        :param template: the job template that was applied
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        :param pre_run_success: whether the pre_run code was successfully run
        :type pre_run_success: bool
        :param do_run_success: whether the do_run code was successfully run (only gets run if pre-run was successful)
        :type do_run_success: bool
        """

        pk = int(job['pk'])

        # zip+upload model (output_graph.pb and output_labels.txt)
        if do_run_success:
            self._compress_and_upload(
                pk, "model", "tfmodel",
                [
                    self.job_dir + "/output/graph.pb",
                    self.job_dir + "/output/labels.txt",
                    self.job_dir + "/output/info.json"
                ],
                self.job_dir + "/model.zip")

        # zip+upload checkpoint (retrain_checkpoint.*)
        if do_run_success:
            self._compress_and_upload(
                pk, "checkpoint", "tfcheckpoint",
                glob(self.job_dir + "/output/retrain_checkpoint.*"),
                self.job_dir + "/checkpoint.zip")

        # zip+upload train/val tensorboard (retrain_logs)
        self._compress_and_upload(
            pk, "log_train", "tensorboard",
            glob(self.job_dir + "/output/retrain_logs/train/events*"),
            self.job_dir + "/tensorboard_train.zip")
        self._compress_and_upload(
            pk, "log_validation", "tensorboard",
            glob(self.job_dir + "/output/retrain_logs/validation/events*"),
            self.job_dir + "/tensorboard_validation.zip")

        # zip+upload train/test/val image list files (*.json)
        self._compress_and_upload(
            pk, "image_lists", "json",
            glob(self.job_dir + "/output/*.json"),
            self.job_dir + "/image_lists.zip")

        # zip+upload predictions/stats
        if do_run_success and bool(self._parameter(KEY_GENERATE_STATS, job, template)['value']):
            self._compress_and_upload(
                pk, "statistics", "csv",
                glob(self.job_dir + "/output/*.csv"),
                self.job_dir + "/statistics.zip")

        super()._post_run(template, job, pre_run_success, do_run_success)
