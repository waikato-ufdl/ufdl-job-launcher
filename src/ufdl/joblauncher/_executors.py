from datetime import datetime
import getpass
import json
import os
import shutil
import subprocess
from subprocess import CompletedProcess
import tempfile
import traceback
from zipfile import ZipFile, ZIP_STORED
from ._logging import logger
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.nodes.docker import retrieve as docker_retrieve
from ufdl.pythonclient.functional.core.jobs.job import add_output as job_add_output
from ufdl.pythonclient.functional.core.jobs.job import acquire_job, start_job, finish_job

KEY_CPU = 'cpu'

KEY_IMAGE_URL = 'url'

KEY_REGISTRY_PASSWORD = 'registry_password'

KEY_REGISTRY_USERNAME = 'registry_username'

KEY_REGISTRY_URL = 'registry_url'

KEY_DOCKER_IMAGE = "docker_image"


class AbstractJobExecutor(object):
    """
    Ancestor for classes executing jobs.
    """

    def __init__(self, context, work_dir, use_sudo=False, ask_sudo_pw=False):
        """
        Initializes the executor with the backend context.

        :param context: the server context
        :type context: UFDLServerContext
        :param work_dir: the working directory to use
        :type work_dir: str
        :param use_sudo: whether to prefix commands with sudo
        :type use_sudo: bool
        :param ask_sudo_pw: whether to prompt user in console for sudo password
        :type ask_sudo_pw: bool
        """
        self._debug = False
        self._context = context
        self._work_dir = work_dir
        self._job_dir = None
        self._use_sudo = use_sudo
        self._ask_sudo_pw = ask_sudo_pw
        self._log = list()
        self._compression = ZIP_STORED

    @property
    def debug(self):
        """
        Returns the debugging mode flag.

        :return: True if debugging mode on
        :rtype: bool
        """
        return self._debug

    @debug.setter
    def debug(self, value):
        """
        Sets the debug flag.

        :param value: True to turn on debugging mode
        :type value: bool
        """
        self._debug = value

    @property
    def compression(self):
        """
        Returns the compression in use.

        :return: the compression (see zipfile, ZIP_STORED/0 = no compression)
        :rtype: int
        """
        return self._compression

    @compression.setter
    def compression(self, value):
        """
        Sets the compression to use.

        :param value: the compression (see zipfile, ZIP_STORED/0 = no compression)
        :type value: int
        """
        self._compression = value

    @property
    def context(self):
        """
        Returns the UFDL server context.

        :return: the context
        :rtype: UFDLServerContext
        """
        return self._context

    @property
    def work_dir(self):
        """
        Returns the working directory. Used for temp files and dirs.

        :return: the directory
        :rtype: str
        """
        return self._work_dir

    @property
    def job_dir(self):
        """
        Returns the working directory. Used for temp files and dirs.

        :return: the directory for the job (gets created before actual run and deleted afterwards again)
        :rtype: str
        """
        return self._job_dir

    @property
    def use_sudo(self):
        """
        Returns whether commands are prefixed with sudo.

        :return: true if prefixing commands
        :rtype: bool
        """
        return self._use_sudo

    @property
    def ask_sudo_pw(self):
        """
        Returns whether to prompt user in console for sudo password.

        :return: true if prompting
        :rtype: bool
        """
        return self._ask_sudo_pw

    def _add_log(self, data):
        """
        Adds the data under a new timestamp to the internal log.

        :param data: the object to add
        :type data: object
        """
        entry = dict()
        entry[str(datetime.now())] = data
        self._log.append(entry)

    def _obscure(self, args, hide):
        """
        Obscures/masks the specified list of strings in the

        :param args: the list of string arguments to process
        :type args: list
        :param hide: the list of strings to hide
        :type hide: list
        :return: the obscured list of strings
        :rtype: list
        """

        if (hide is None) or (len(hide) == 0):
            return args

        result = []
        to_hide = set(hide)
        for arg in args:
            if arg in to_hide:
                result.append("*" * 3)
            else:
                result.append(arg)
        return result

    def _log_msg(self, *args):
        """
        For logging debugging messages.

        :param args: the arguments to log, get turned into a string, blank separated (similar to print)
        """
        str_args = [str(x) for x in args]
        data = dict()
        data['msg'] = " ".join(str_args).split("\n")
        self._add_log(data)

    def _mktmpdir(self):
        """
        Creates a temp directory in the working directory and returns the absolute path.

        :return: the tmp directory that has been created
        :rtype: str
        """
        return tempfile.mkdtemp(suffix="", prefix="", dir=self.work_dir)

    def _mkdir(self, directory):
        """
        Creates the specified directory.

        :param directory: the directory to create
        :type directory: str
        """
        self._log_msg("mkdir:", directory)
        os.mkdir(directory)

    def _rmdir(self, directory):
        """
        Removes the directory recursively.

        :param directory: the directory to delete
        :type directory: str
        """
        self._log_msg("rmdir:", directory)
        shutil.rmtree(directory, ignore_errors=True)

    def _execute_can_use_stdin(self, no_sudo=None):
        """
        Returns whether the _execute function can use stdin.
        Not possible if sudo is asking for password.

        :return: True if it can make use of stdin
        """
        if self.use_sudo:
            if no_sudo is None or no_sudo is False:
                if self.ask_sudo_pw:
                    return False
        return True

    def _execute(self, cmd, always_return=True, no_sudo=None, capture_output=True, stdin=None, hide=None):
        """
        Executes the command.

        :param cmd: the command as list of strings
        :type cmd: list
        :param always_return: whether to always return the subprocess.CompletedProcess object or only
                              if the returncode is non-zero
        :type always_return: bool
        :param no_sudo: temporarily disable sudo
        :type no_sudo: bool
        :param capture_output: whether to capture the output from stdout and stderr
        :type capture_output: bool
        :param stdin: the text to feed into the process via stdin
        :type stdin: str
        :param hide: the list of strings to obscure in the log message
        :type hide: list
        :return: the CompletedProcess object from executing the command, uses 255 as return code in case of an
                 exception and stores the stack trace in stderr
        :rtype: subprocess.CompletedProcess
        """

        if (stdin is not None) and (not self._execute_can_use_stdin(no_sudo)):
            raise Exception("Cannot feed data into stdin of process! E.g., when sudo is asking for password.")

        full = []
        if self.use_sudo:
            if no_sudo is None or no_sudo is False:
                full.append("sudo")
                if self.ask_sudo_pw:
                    full.append("-S")
        full.extend(cmd)

        self._log_msg("Executing:", " ".join(self._obscure(full, hide)))

        try:
            if stdin is not None:
                if not stdin.endswith("\n"):
                    stdin = stdin + "\n"
                result = subprocess.run(full, capture_output=capture_output, stdin=subprocess.PIPE)
                result.stdin.write(stdin)
            else:
                result = subprocess.run(full, capture_output=capture_output)
        except:
            result = CompletedProcess(full, 255, stdout=None, stderr=traceback.format_exc())

        log_data = dict()
        log_data['cmd'] = self._obscure(result.args, hide)
        if result.stdout is not None:
            log_data['stdout'] = result.stdout.decode().split("\n")
        if result.stdout is not None:
            log_data['stderr'] = result.stderr.decode().split("\n")
        log_data['returncode'] = result.returncode
        self._add_log(log_data)

        if always_return or (result.returncode > 0):
            return result
        else:
            return None
        
    def _compress(self, files, zipfile, strip_path=None):
        """
        Compresses the files and stores them in the zip file.
        
        :param files: the list of files to compress
        :type files: list  
        :param zipfile: the zip file to create
        :type zipfile: str
        :param strip_path: whether to strip the path: True for removing completely, or prefix string to remove
        :type strip_path: bool or str
        :return: None if successful, otherwise error message
        :rtype: str
        """

        self._log_msg("Compressing:", files, "->", zipfile)

        try:
            with ZipFile(zipfile, "w", compression=self._compression) as zf:
                for f in files:
                    arcname = None
                    if strip_path is not None:
                        if isinstance(strip_path, bool):
                            arcname = os.path.basename(f)
                        elif f.startswith(strip_path):
                            arcname = f[len(strip_path):]
                        if arcname.startswith("/"):
                            arcname = arcname[1:]
                    zf.write(f, arcname=arcname)
            return None
        except:
            return "Failed to compress files '%s' into '%s':\n%s" \
                   % (",".join(files), zipfile, traceback.format_exc())

    def _decompress(self, zipfile, output_dir):
        """
        Decompresses a zipfile into the specified output directory.

        :param zipfile: the zip file to decompress
        :type zipfile: str
        :param output_dir: the output directory
        :type output_dir: str
        """

        self._log_msg("Decompressing:", zipfile, "->", output_dir)

        try:
            with ZipFile(zipfile, "r") as zf:
                zf.extractall(output_dir)
        except:
            return "Failed to decompress '%s' to '%s':\n%s" \
                   % (zipfile, output_dir, traceback.format_exc())

    def _input(self, name, job, template):
        """
        Returns the input variable description.
        Raises an exception if the template doesn't define it.

        :param job: the job dictionary
        :type job: dict
        :param template: the template dictionary (for defaults)
        :type template: dict
        :param name: the name of the input to retrieve
        :return: the dictionary with name, type, value
        :rtype: dict
        """
        default = None
        for _input in template['inputs']:
            if _input['name'] == name:
                default = _input
                break
        if default is None:
            raise Exception("Input '%s' not found in template!\n%s" % (name, str(template)))
        supplied = None
        if name in job['input_values']:
            supplied = job['input_values'][name]

        result = dict()
        result['name'] = name
        result['type'] = default['type']
        if supplied is None:
            result['value'] = default['default']
            if 'options' in default:
                result['options'] = default['options']
        else:
            result['value'] = supplied
            result['options'] = default['options']
        if 'options' not in result:
            result['options'] = ''

        return result

    def _parameter(self, name, job, template):
        """
        Returns the parameter description.
        Raises an exception if the template doesn't define it.

        :param job: the job dictionary
        :type job: dict
        :param template: the template dictionary (for defaults)
        :type template: dict
        :param name: the name of the parameter to retrieve
        :return: the dictionary with name, type, value
        :rtype: dict
        """
        default = None
        for param in template['parameters']:
            if param['name'] == name:
                default = param
                break
        if default is None:
            raise Exception("Parameter '%s' not found in template!\n%s" % (name, str(template)))
        supplied = None
        if name in job['parameter_values']:
            supplied = job['parameter_values'][name]

        result = dict()
        result['name'] = name
        result['type'] = default['type']
        if supplied is None:
            result['value'] = default['default']
        else:
            result['value'] = supplied

        return result

    def _expand_template(self, job, template):
        """
        Expands all parameters in the template code and returns the updated template string.

        :param job: the job dictionary
        :type job: dict
        :param template: the template dictionary (for defaults)
        :type template: dict
        :return: the expanded template
        :rtype: str
        """
        result = "".join(template["body"])

        for name in job['parameter_values']:
            value = self._parameter(name, job, template)['value']
            result = result.replace("${" + name + "}", value)

        return result

    def _any_present(self, files):
        """
        Checks whether at least one of the files listed is present in the file system.

        :param files: the file names to check (absolute paths)
        :type files: list
        :return: True if at least one present
        :rtype: bool
        """
        result = False

        for f in files:
            if os.path.exists(f) and os.path.isfile(f):
                result = True
                break

        return result

    def _compress_and_upload(self, job_pk, output_name, output_type, files, zipfile, strip_path=True):
        """
        Compresses the files as zip file and uploads them as job output under the specified name.

        :param job_pk: the PK of the job this output is for
        :type job_pk: int
        :param output_name: the job output name to use
        :type output_name: str
        :param output_type: the job output type to use (eg model, tensorboard, json)
        :type output_type: str
        :param files: the list of files to compress
        :type files: list
        :param zipfile: the zip file to store the files in
        :type zipfile: str
        :param strip_path: whether to strip the path from the files (None, True or path-prefix to remove)
        :type strip_path: bool or str
        """
        if len(files) == 0:
            self._log_msg("No files supplied, cannot generate zip file %s:" % zipfile)
            return

        if not self._any_present(files):
            self._log_msg("None of the files are present, cannot generate zip file %s:" % zipfile, files)
            return

        self._compress(files, zipfile, strip_path=strip_path)
        try:
            with open(zipfile, "rb") as zf:
                job_add_output(self.context, job_pk, output_name, output_type, zf)
        except:
            self._log_msg("Failed to upload zipfile (%s/%s/%s) to backend:\n%s" % (output_name, output_type, zipfile, traceback.format_exc()))

    def _pre_run(self, template, job):
        """
        Hook method before the actual job is run.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        """
        self._job_dir = self._mktmpdir()
        self._log_msg("Created jobdir:", self.job_dir)
        # TODO reenable
        # try:
        #     acquire_job(self.context, job['pk'])
        # except:
        #     self._log_msg("Failed to acquire job %d!\n%s" % (job['pk'], traceback.format_exc()))
        # try:
        #     start_job(self.context, job['pk'], "")  # TODO retrieve notification type from user
        # except:
        #     self._log_msg("Failed to start job %d!\n%s" % (job['pk'], traceback.format_exc()))

    def _do_run(self, template, job):
        """
        Executes the actual job. Only gets run if pre-run was successful.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        """
        raise NotImplemented()

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
        log = self.job_dir + "/log.json"
        try:
            with open(self.job_dir + "/log.json", "w") as log_file:
                json.dump(self._log, log_file, indent=2)
            self._compress_and_upload(int(job['pk']), "log", "json", [log], self.job_dir + "/log.zip")
        except:
            logger().error("Failed to write log data to: %s" % log)
            logger().error(traceback.format_exc())

        if not self._debug:
            self._rmdir(self.job_dir)
        self._job_dir = None
        # TODO reenable
        # try:
        #     finish_job(self.context, job['pk'], "")  # TODO retrieve notification type from user
        # except:
        #     self._log_msg("Failed to finish job %d!\n%s" % (job['pk'], traceback.format_exc()))

    def run(self, template, job):
        """
        Applies the template and executes the job. Raises an exception if it fails.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        :return:
        """
        pre_run_success = False
        do_run_success = False
        try:
            self._pre_run(template, job)
            pre_run_success = True
        except:
            self._log_msg("Failed to execute pre-run code:\n%s" % traceback.format_exc())

        if pre_run_success:
            try:
                self._do_run(template, job)
                do_run_success = True
            except:
                self._log_msg("Failed to execute do-run code:\n%s" % traceback.format_exc())

        try:
            self._post_run(template, job, pre_run_success, do_run_success)
        except:
            self._log_msg("Failed to execute post-run code:\n%s" % traceback.format_exc())

    def __str__(self):
        """
        Returns a short description of itself.

        :return: the short description
        :rtype: str
        """
        return "context=" + str(self.context) + ", workdir=" + self.work_dir


class AbstractDockerJobExecutor(AbstractJobExecutor):
    """
    For executing jobs via docker images.
    """

    def __init__(self, context, work_dir, use_sudo=False, ask_sudo_pw=False, use_current_user=True):
        """
        Initializes the executor with the backend context.

        :param context: the server context
        :type context: UFDLServerContext
        :param work_dir: the working directory to use
        :type work_dir: str
        :param use_sudo: whether to prefix commands with sudo
        :type use_sudo: bool
        :param ask_sudo_pw: whether to prompt user in console for sudo password
        :type ask_sudo_pw: bool
        :param use_current_user: whether to run as root (False) or as user launching the image (True)
        :type use_current_user: bool
        """
        super(AbstractDockerJobExecutor, self).__init__(
            context, work_dir, use_sudo=use_sudo, ask_sudo_pw=ask_sudo_pw)
        self._use_current_user = use_current_user
        self._use_gpu = False
        self._docker_image = None

    @property
    def use_current_user(self):
        """
        Returns whether the image is run as root (False) or as current user (True).

        :return: how the image is run
        :rtype: bool
        """
        return self._use_current_user

    def _version(self, include_patch=True):
        """
        Returns the docker version.

        :param include_patch: whether to include the patch version as well next to major/minor
        :type include_patch: bool
        :return: the version string, None if failed to obtain
        :rtype: str
        """

        res = self._execute(["docker", "--version"], no_sudo=True, capture_output=True)
        if res.returncode > 0:
            return None

        result = res.stdout.decode().strip()
        if result.startswith("Docker version"):
            result = result.replace("Docker version ", "")
        if "," in result:
            result = result.split(",")[0]
        if not include_patch:
            if "." in result:
                parts = result.split(".")
                if len(parts) >= 2:
                    result = parts[0] + "." + parts[1]

        return result

    def _gpu_flags(self):
        """
        If the GPU is to be used, returns the relevant flags as list.

        :return: the list of flags, empty list if none required
        :rtype: list
        """
        result = []

        if self._use_gpu:
            version = self._version(include_patch=False)
            if version is not None:
                version_num = float(version)
                if version_num >= 19.03:
                    result.append("--gpus=all")
                else:
                    result.append("--runtime=nvidia")

        return result

    def _login_registry(self, registry, user, password):
        """
        Logs into the specified registry.

        :param registry: the registry URL to log into
        :type registry: str
        :param user: the user name for the registry
        :type user: str
        :param password: the password for the registry
        :type password: str
        :return: None if successfully logged in, otherwise subprocess.CompletedProcess
        :rtype: subprocess.CompletedProcess
        """
        if self._execute_can_use_stdin(no_sudo=(not self.use_sudo)):
            return self._execute(["docker", "login", "-u", user, "--password-stdin", registry], always_return=False, stdin=password, hide=[user])
        else:
            return self._execute(["docker", "login", "-u", user, "-p", password, registry], always_return=False, hide=[user, password])

    def _logout_registry(self, registry):
        """
        Logs out of the specified registry.

        :param registry: the registry URL to log out from
        :type registry: str
        :return: None if successfully logged out, otherwise subprocess.CompletedProcess
        :rtype: subprocess.CompletedProcess
        """
        return self._execute(["docker", "logout", registry], always_return=False)

    def _pull_image(self, image):
        """
        Pulls the requested image.

        :param image: the image to pull
        :type image: str
        :return: None if successfully pulled, otherwise subprocess.CompletedProcess
        :rtype: subprocess.CompletedProcess
        """
        return self._execute(["docker", "pull", image], always_return=False)

    def _run_image(self, image, docker_args=None, volumes=None, image_args=None):
        """
        Runs the image with the specified parameters.

        :param image: the URL of the image to run
        :type image: str
        :param docker_args: the arguments for docker (eg: --gpus=all --shm-size 8G -ti -u $(id -u):$(id -g) -e USER=$USER)
        :type docker_args: list
        :param volumes: the volumes to map (eg: /some/where/models:/models)
        :type volumes: list
        :param image_args: the command and arguments to supply to the docker image for execution
        :type image_args: list
        :return: None if successfully executed, otherwise subprocess.CompletedProcess
        :rtype: subprocess.CompletedProcess
        """
        cmd = ["docker", "run"]
        if docker_args is None:
            docker_args = []
        docker_args.extend(self._gpu_flags())
        if self.use_current_user:
            docker_args.extend([
                "-u", "%d:%d" % (os.getuid(), os.getgid()),
                "-e", "USER=%s" % getpass.getuser(),
            ])
        cmd.extend(docker_args)
        if volumes is not None:
            for volume in volumes:
                cmd.extend(["-v", volume])
        cmd.append(image)
        if image_args is not None:
            cmd.extend(image_args)
        return self._execute(cmd, always_return=False)

    def _pre_run(self, template, job):
        """
        Hook method before the actual job is run.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        """
        super()._pre_run(template, job)

        # docker image
        if KEY_DOCKER_IMAGE not in job:
            raise Exception("Docker image PK not defined in job (key: %s)!\n%s" % (KEY_DOCKER_IMAGE, str(job)))
        self._docker_image = docker_retrieve(self.context, int(job[KEY_DOCKER_IMAGE]['pk']))
        self._login_registry(
            self._docker_image[KEY_REGISTRY_URL],
            self._docker_image[KEY_REGISTRY_USERNAME],
            self._docker_image[KEY_REGISTRY_PASSWORD])
        self._use_gpu = not bool(self._docker_image[KEY_CPU])
        self._pull_image(self._docker_image[KEY_IMAGE_URL])

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
        if self._docker_image is not None:
            self._logout_registry(self._docker_image[KEY_REGISTRY_URL])
            self._docker_image = None

        super()._post_run(template, job, pre_run_success, do_run_success)
