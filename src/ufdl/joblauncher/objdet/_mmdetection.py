from glob import glob
from ufdl.joblauncher import AbstractDockerJobExecutor
from ufdl.pythonclient.functional.image_classification.dataset import download as dataset_download


class ObjectDetectionTrain_MMDet_1_2(AbstractDockerJobExecutor):
    """
    For executing Tensorflow image classification jobs.
    """

    def __init__(self, context, workdir, use_sudo=False, ask_sudo_pw=False, use_current_user=True):
        """
        Initializes the executor with the backend context.

        :param context: the server context
        :type context: UFDLServerContext
        :param workdir: the working directory to use
        :type workdir: str
        :param use_sudo: whether to prefix commands with sudo
        :type use_sudo: bool
        :param ask_sudo_pw: whether to prompt user in console for sudo password
        :type ask_sudo_pw: bool
        """
        super(ObjectDetectionTrain_MMDet_1_2, self).__init__(
            context, workdir, use_sudo=use_sudo, ask_sudo_pw=ask_sudo_pw, use_current_user=use_current_user)

    def _pre_run(self, template, job):
        """
        Hook method before the actual job is run.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        """
        super()._pre_run(template, job)

        # create directories
        self._mkdir(self.jobdir + "/output")
        self._mkdir(self.jobdir + "/models")

        # download dataset
        data = self.jobdir + "/data.zip"
        pk = int(self._input("data", job, template)["value"])
        options = self._input("data", job, template)["options"]
        self._log_msg("Downloading dataset:", pk, "-> options=", options, "->", data)
        with open(data, "wb") as zip_file:
            for b in dataset_download(self.context, pk, annotations_args=options):
                zip_file.write(b)

        # decompress dataset
        output_dir = self.jobdir + "/data"
        msg = self._decompress(data, output_dir)
        if msg is not None:
            raise Exception("Failed to extract dataset pk=%d!\n%s" % (pk, msg))

        # replace parameters in template and save it to disk
        template_code = self._expand_template(job, template)
        template_file = self.jobdir + "/output/config.py"
        with open(template_file, "w") as tf:
            tf.write(template_code)

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
            self.jobdir + "/data" + ":/data",
            self.jobdir + "/output" + ":/output",
            self.jobdir + "/models" + ":/models",
        ]

        # build model
        self._run_image(
            image,
            docker_args=[
                "-e", "MMDET_CLASSES=/data/labels.txt",
                "-e", "MMDET_OUTPUT=/output/",
                "-e", "MMDET_SETUP=/output/config.py",
                "-e", "MMDET_DATA=/data"
            ],
            volumes=volumes,
            image_args=[
                "mmdet_train",
                "/output/config.py",
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
        self._compress_and_upload(
            pk, "model", "mmdetmodel",
            [
                self.jobdir + "/output/latest.pth",
                self.jobdir + "/output/config.py",
                self.jobdir + "/data/labels.txt"
            ],
            self.jobdir + "/model.zip")

        # zip+upload training logs
        self._compress_and_upload(
            pk, "log", "json",
            glob(self.jobdir + "/output/*.log.json"),
            self.jobdir + "/log.zip")

        super()._post_run(template, job, pre_run_success, do_run_success)
