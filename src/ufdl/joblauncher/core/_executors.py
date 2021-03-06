from datetime import datetime
import getpass
import json
import os
import shutil
import subprocess
from subprocess import CompletedProcess
import tempfile
import traceback
from requests.exceptions import HTTPError
from zipfile import ZipFile
from ._descriptors import Parameter
from ._logging import logger
from ._node import get_ipv4
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.nodes.node import ping as node_ping
from ufdl.pythonclient.functional.core.nodes.cuda import retrieve as cuda_retrieve
from ufdl.pythonclient.functional.core.nodes.docker import retrieve as docker_retrieve
from ufdl.pythonclient.functional.core.nodes.hardware import retrieve as hardware_retrieve
from ufdl.pythonclient.functional.core.jobs.job import add_output as job_add_output, retrieve as job_retrieve
from ufdl.pythonclient.functional.core.jobs.job import acquire_job, start_job, finish_job, progress_job
from wai.json.object import Absent

KEY_CPU = 'cpu'

KEY_IMAGE_URL = 'url'

KEY_REGISTRY_PASSWORD = 'registry_password'

KEY_REGISTRY_USERNAME = 'registry_username'

KEY_REGISTRY_URL = 'registry_url'

KEY_DOCKER_IMAGE = "docker_image"

DOCKER_IMAGE_TYPE = "pk<docker_image>"


class AbstractJobExecutor(object):
    """
    Ancestor for classes executing jobs.
    """

    def __init__(self, context, config):
        """
        Initializes the executor with the backend context and configuration.

        :param context: the server context
        :type context: UFDLServerContext
        :param config: the configuration to use
        :type config: configparser.ConfigParser
        """
        self._debug = (config['general']['debug'] == "true")
        self._context = context
        self._work_dir = config['docker']['work_dir']
        self._cache_dir = config['docker']['cache_dir']
        self._job_dir = None
        self._use_sudo = (config['docker']['use_sudo'] == "true")
        self._ask_sudo_pw = (config['docker']['ask_sudo_pw'] == "true")
        self._log = list()
        self._compression = int(config['general']['compression'])
        self._notification_type = None
        self._template = None
        self._job = None
        self._last_cancel_check = None
        self._cancel_check_wait = int(config['general']['cancel_check_wait'])
        self._job_is_cancelled = False

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
    def cache_dir(self):
        """
        Returns the cache directory. Used for base models etc.

        :return: the directory
        :rtype: str
        """
        return self._cache_dir

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

    @property
    def notification_type(self):
        """
        Returns the type of notification to send out.

        :return: the type of notification (eg email)
        :rtype: str
        """
        return self._notification_type

    @property
    def job(self):
        """
        Returns the job of this executor.

        :return: the job
        :rtype: dict
        """
        return self._job

    @job.setter
    def job(self, value):
        """
        Sets the job of this executor.

        :param value: the job
        :type value: dict
        """
        self._job = value

    @property
    def template(self):
        """
        Returns the template of this executor.

        :return: the template
        :rtype: dict
        """
        return self._template

    @template.setter
    def template(self, value):
        """
        Sets the template of this executor.

        :param value: the template
        :type value: dict
        """
        self._template = value

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

    def log_msg(self, *args):
        """
        For logging debugging messages.

        :param args: the arguments to log, get turned into a string, blank separated (similar to print)
        """
        str_args = [str(x) for x in args]
        data = dict()
        data['msg'] = " ".join(str_args).split("\n")
        self._add_log(data)
        if self.debug:
            logger().debug("\n".join(data['msg']))
        # write to disk
        if self.job_dir is not None:
            log = self.job_dir + "/log.json"
            try:
                with open(log, "w") as log_file:
                    json.dump(self._log, log_file, indent=2)
            except:
                logger().error("Failed to write log data to: %s" % log)
                logger().error(traceback.format_exc())

    def log_file(self, msg, filename):
        """
        Reads the specified file and then logs the message and its content.

        :param msg: the message to prepend the file content
        :type msg: str
        :param filename: the file to read
        :type filename: str
        """
        try:
            with open(filename, "r") as lf:
                lines = lf.readlines()
            self.log_msg("%s\n%s" % (msg, "".join(lines)))
        except:
            self.log_msg("Failed to read file: %s\n%s" % (filename, traceback.format_exc()))

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
        self.log_msg("mkdir:", directory)
        os.mkdir(directory)

    def _rmdir(self, directory):
        """
        Removes the directory recursively.

        :param directory: the directory to delete
        :type directory: str
        """
        self.log_msg("rmdir:", directory)
        shutil.rmtree(directory, ignore_errors=True)

    def _to_logentry(self, completed, hide):
        """
        Turns the CompletedProcess object into a log entry.

        :param completed: the CompletedProcess object to convert
        :type completed: CompletedProcess
        :param hide: the list of strings to obscure in the log message
        :type hide: list
        :return: the log entry
        :rtype: dict
        """
        result = dict()
        result['cmd'] = self._obscure(completed.args, hide)
        if completed.stdout is not None:
            if isinstance(completed.stdout, str):
                result['stdout'] = completed.stdout.split("\n")
            elif isinstance(completed.stdout, list):
                result['stdout'] = completed.stdout[:]
            else:
                result['stdout'] = completed.stdout.decode().split("\n")
        if completed.stderr is not None:
            if isinstance(completed.stderr, str):
                result['stderr'] = completed.stderr.split("\n")
            elif isinstance(completed.stderr, list):
                result['stderr'] = completed.stderr[:]
            else:
                result['stderr'] = completed.stderr.decode().split("\n")
        result['returncode'] = completed.returncode
        return result

    def _ping_backend(self):
        """
        Ensuring that the connection is still live.
        """
        try:
            node_ping(self.context)
        except:
            self.log_msg("Failed to ping backend:\n%s" % traceback.format_exc())

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

    def _execute(self, cmd, always_return=True, no_sudo=None, capture_output=True, stdin=None, hide=None, command_progress_parser=None, job=None):
        """
        Executes the command.
        For updating a job's progress, a progress parser method can be supplied. For a dummy implemented and
        explanation of parameters see: dummy_command_progress_parser

        :param cmd: the command as list of strings
        :type cmd: list
        :param always_return: whether to always return the subprocess.CompletedProcess object or only
                              if the return code is non-zero
        :type always_return: bool
        :param no_sudo: temporarily disable sudo
        :type no_sudo: bool
        :param capture_output: whether to capture the output from stdout and stderr
        :type capture_output: bool
        :param stdin: the text to feed into the process via stdin
        :type stdin: str
        :param hide: the list of strings to obscure in the log message
        :type hide: list
        :param command_progress_parser: the parser for the command output for updating the progress in the backend
        :type command_progress_parser: object
        :param job: the job for which to post progress updates
        :type job: dict
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

        self.log_msg("Executing:", " ".join(self._obscure(full, hide)))

        try:
            if stdin is not None:
                if not stdin.endswith("\n"):
                    stdin = stdin + "\n"
                process = subprocess.Popen(full, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate(input=stdin.encode())
                result = CompletedProcess(full, process.returncode, stdout=stdout, stderr=stderr)
            else:
                stdout_list = []
                process = subprocess.Popen(full, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1)
                last_progress = 0.0
                while True:
                    # terminate job if canceled
                    if self.is_job_cancelled(job):
                        process.terminate()
                        break
                    line = process.stdout.readline()
                    if not line:
                        break
                    if capture_output:
                        stdout_list.append(line)
                    if capture_output and (job is not None) and (command_progress_parser is not None):
                        try:
                            last_progress = command_progress_parser(self, job, line, last_progress)
                        except:
                            command_progress_parser = None
                            self.log_msg("Failed to parse progress output, disabling!", traceback.format_exc())
                if self.is_job_cancelled(job):
                    stdout_list.append("Job was cancelled")
                    result = CompletedProcess(full, 255, stdout=stdout_list, stderr=None)
                else:
                    retcode = process.wait()
                    result = CompletedProcess(full, retcode, stdout=stdout_list, stderr=None)
        except:
            result = CompletedProcess(full, 255, stdout=None, stderr=traceback.format_exc())

        self._add_log(self._to_logentry(result, hide))

        if always_return or (result.returncode > 0):
            return result
        else:
            return None

    def _fail_on_error(self, completed):
        """
        Raises an exception if the CompletedProcess object has a non-zero exit code.

        :param completed: the CompletedProcess object to inspect
        :type completed: CompletedProcess
        :raises: Exception
        """
        if isinstance(completed, CompletedProcess):
            if completed.returncode != 0:
                error = "Failed to execute command:\n%s" % " ".join(completed.args)
                error += "\nreturn code:\n%d" % completed.returncode
                if completed.stdout is not None:
                    error += "\nstdout:\n%s" % completed.stdout
                if completed.stderr is not None:
                    error += "\nstderr:\n%s" % completed.stderr
                raise Exception(error)

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

        self.log_msg("Compressing:", files, "->", zipfile)

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
            msg = "Failed to compress files '%s' into '%s':\n%s" \
                   % (",".join(files), zipfile, traceback.format_exc())
            self.log_msg(msg)
            return msg

    def _decompress(self, zipfile, output_dir):
        """
        Decompresses a zipfile into the specified output directory.

        :param zipfile: the zip file to decompress
        :type zipfile: str
        :param output_dir: the output directory
        :type output_dir: str
        """

        self.log_msg("Decompressing:", zipfile, "->", output_dir)

        try:
            with ZipFile(zipfile, "r") as zf:
                zf.extractall(output_dir)
        except:
            return "Failed to decompress '%s' to '%s':\n%s" \
                   % (zipfile, output_dir, traceback.format_exc())

    def _input(self, name, job=None, template=None):
        """
        Returns the input variable description.
        Raises an exception if the template doesn't define it.

        :param job: the job dictionary
        :type job: dict|None
        :param template: the template dictionary (for defaults)
        :type template: dict|None
        :param name: the name of the input to retrieve
        :return: the dictionary with name, type, value
        :rtype: dict
        """
        if job is None:
            job = self._job
        if template is None:
            template = self._template
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
        if supplied is None:
            raise Exception("Input '%s' not supplied!" % name)
        else:
            result['type'] = supplied['type']
            result['value'] = supplied['value']
            result['options'] = default['options']
        if 'options' not in result:
            result['options'] = ''

        return result

    def _parameter(self, name, job=None, template=None, allowed_types=None):
        """
        Returns the parameter description.
        Raises an exception if the template doesn't define it.

        :param job: the job dictionary
        :type job: dict|None
        :param template: the template dictionary (for defaults)
        :type template: dict|None
        :param name: the name of the parameter to retrieve
        :param allowed_types: a tuple of allowed types for the parameter
        :type allowed_types: tuple|None
        :return: the dictionary with name, type, value
        :rtype: dict
        """
        if job is None:
            job = self._job
        if template is None:
            template = self._template
        default = None
        for param in template['parameters']:
            if param['name'] == name:
                default = param
                break
        if default is None:
            raise Exception("Parameter '%s' not found in template!\n%s" % (name, str(template)))
        if allowed_types is not None and default['type'] not in allowed_types:
            raise Exception(
                f"Parameter '{name}' is not one of the allowed types!\n"
                f"Specified type is '{default['type']}'\n"
                f"Allowed types are: {', '.join(allowed_types)}\n"
                f"{template}"
            )
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

    def _is_true(self, name, job=None, template=None):
        """
        Checks whether the boolean parameter is true.

        :param job: the job dictionary
        :type job: dict|None
        :param template: the template dictionary (for defaults)
        :type template: dict|None
        :param name: the name of the parameter to retrieve
        :return: the boolean value of the parameter
        :rtype: bool
        """
        return self._parameter(name, job, template)['value'].lower() == "true"

    def _is_false(self, name, job=None, template=None):
        """
        Checks whether the boolean parameter is false.

        :param job: the job dictionary
        :type job: dict|None
        :param template: the template dictionary (for defaults)
        :type template: dict|None
        :param name: the name of the parameter to retrieve
        :return: the boolean value of the parameter
        :rtype: bool
        """
        return self._parameter(name, job, template)['value'].lower() == "false"

    def _expand_template(self, job=None, template=None, body=None, bool_to_python=False):
        """
        Expands all parameters in the template code and returns the updated template string.

        :param job: the job dictionary
        :type job: dict|None
        :param template: the template dictionary (for defaults)
        :type template: dict|None
        :param body: the template body to expand, if None, use the "body" value from the template
        :type body: str
        :param bool_to_python: whether to convert boolean values true|false to Python's True|False
        :type bool_to_python: bool
        :return: the expanded template
        :rtype: str
        """
        if job is None:
            job = self._job
        if template is None:
            template = self._template
        if body is None:
            result = "".join(template["body"])
        else:
            result = body

        for parameter in template['parameters']:
            name = parameter['name']
            param = self._parameter(name, job, template)
            value = param['value']
            if bool_to_python and (param['type'] == 'bool'):
                if value == 'true':
                    value = 'True'
                else:
                    value = 'False'
            result = result.replace("${" + name + "}", value)

        return result

    def _pk_from_joboutput(self, joboutput):
        """
        Determines PK from the job output string (PK|name|type).

        :param joboutput: the joboutput string
        :type joboutput: str
        :return: the extracted PK, -1 if failed to determine
        :rtype: int
        """
        try:
            if "|" in joboutput:
                result = int(joboutput[0:joboutput.index("|")])
            else:
                result = int(joboutput)
        except:
            self.log_msg("Failed to determine job ID from: %s\n%s" % (joboutput, traceback.format_exc()))
            result = -1
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

    def progress(self, job_pk, progress, **data):
        """
        Updates the server on the progress of the job.

        :param job_pk: the PK of the job this output is for
        :type job_pk: int
        :param progress: the progress amount in [0.0, 1.0]
        :type progress: float
        :param data: other JSON meta-data about the progress
        :type data: RawJSONElement
        """
        # Progress updates to a cancelled job will fail, so skip updating
        # the progress if we know we're cancelled already
        if self.is_job_cancelled():
            return

        try:
            progress_job(self.context, job_pk, progress, **data)
        except:
            self.log_msg("Failed to update backend on progress to backend:\n%s" % (traceback.format_exc()))

            # Assume the progress update failed because the job was cancelled,
            # and perform an check to see if this is the case, so all
            # future checks will show this immediately
            self.is_job_cancelled(immediate=True)

    def _upload(self, job_pk, output_name, output_type, localfile):
        """
        Uploads the specified file to the backend as job output.

        :param job_pk: the PK of the job this output is for
        :type job_pk: int
        :param output_name: the job output name to use
        :type output_name: str
        :param output_type: the job output type to use (eg model, tensorboard, json)
        :type output_type: str
        :param localfile: the file to upload
        :type localfile: str
        """
        try:
            with open(localfile, "rb") as lf:
                job_add_output(self.context, job_pk, output_name, output_type, lf)
        except:
            self.log_msg("Failed to upload file (%s|%s|%s) to backend:\n%s" % (output_name, output_type, localfile, traceback.format_exc()))

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
            self.log_msg("No files supplied, cannot generate zip file %s:" % zipfile)
            return

        if not self._any_present(files):
            self.log_msg("None of the files are present, cannot generate zip file %s:" % zipfile, files)
            return

        if self._compress(files, zipfile, strip_path=strip_path) is None:
            self._upload(job_pk, output_name, output_type, zipfile)

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
        # TODO retrieve notification type from user
        self._notification_type = "email"

        # basic info
        self.log_msg("IP:", get_ipv4())
        self.log_msg("Use sudo:", str(self.use_sudo))
        self.log_msg("Ask sudo password:", str(self.ask_sudo_pw))
        self.log_msg("Notification:", self._notification_type)
        self.log_msg("Job:\n" + str(job))
        self.log_msg("Template:\n" + str(template))

        # jobdir
        self._job_dir = self._mktmpdir()
        self.log_msg("Created jobdir:", self.job_dir)

        # acquire
        try:
            acquire_job(self.context, job['pk'])
        except HTTPError as e:
            self.log_msg("Failed to acquire job %d!\n%s\n%s" % (job['pk'], str(e.response.text), traceback.format_exc()))
            return
        except:
            self.log_msg("Failed to acquire job %d!\n%s" % (job['pk'], traceback.format_exc()))
            return False
        # start
        try:
            start_job(self.context, job['pk'], self.notification_type)
        except HTTPError as e:
            self.log_msg("Failed to start job %d!\n%s\%s" % (job['pk'], str(e.response.text), traceback.format_exc()))
            return False
        except:
            self.log_msg("Failed to start job %d!\n%s" % (job['pk'], traceback.format_exc()))
        return True

    def _do_run(self, template, job):
        """
        Executes the actual job. Only gets run if pre-run was successful.

        :param template: the job template to apply
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        """
        raise NotImplemented()

    def _post_run(self, template, job, pre_run_success, do_run_success, error):
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
        :param error: any error that may have occurred, None if none occurred
        :type error: str
        """

        # zip+upload log
        self._compress_and_upload(int(job['pk']), "log", "json", [self.job_dir + "/log.json"], self.job_dir + "/log.zip")

        # finish job
        try:
            if error is None:
                error = Absent
                if not pre_run_success:
                    error = "An error occurred during pre-run, check log!"
                elif not do_run_success:
                    error = "An error occurred during run, check log!"
            finish_job(self.context, job['pk'], pre_run_success and do_run_success, self.notification_type, error=error)
        except HTTPError as e:
            self.log_msg("Failed to finish job %d!\n%s\n%s" % (job['pk'], str(e.response.text), traceback.format_exc()))
        except:
            self.log_msg("Failed to finish job %d!\n%s" % (job['pk'], traceback.format_exc()))

        # clean up job dir
        if not self._debug:
            self._rmdir(self.job_dir)
        self._job_dir = None

    def can_run(self, job, template, hardware_info):
        """
        Checks if this job-executor is capable of running on the current node.

        :param template: the job template that was applied
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        :param hardware_info: the hardware info to use
        :type hardware_info: dict
        :return: The reason the job can't be run, or None if it can.
        :rtype: str|None
        """
        return None

    def is_job_cancelled(self, job=None, immediate=False):
        """
        Checks if this job has been cancelled. If the _job_is_cancelled flag is not set, then queries the backend
        (at most every self._cancel_check_wait seconds).

        :param job: the job object to check for
        :type job: dict
        :param immediate: whether to ignore the check throttling and check immediately
        :type immediate: bool
        :return: Whether the job has been cancelled
        :rtype: bool
        """
        result = self._job_is_cancelled
        if not result:
            now = datetime.now()
            if immediate or (self._last_cancel_check is None) or ((now - self._last_cancel_check).total_seconds() >= self._cancel_check_wait):
                if job is None:
                    job = self.job
                job_pk = int(job['pk'])
                updated_job = job_retrieve(self.context, job_pk)
                result = self._job_is_cancelled = updated_job['is_cancelled']
                self._last_cancel_check = now

        return result

    def run(self, template=None, job=None):
        """
        Applies the template and executes the job. Raises an exception if it fails.

        :param template: the job template to apply
        :type template: dict|None
        :param job: the job with the actual values for inputs and parameters
        :type job: dict|None
        :return:
        """
        if job is None:
            job = self._job
        if template is None:
            template = self._template
        error = None
        do_run_success = False
        try:
            pre_run_success = self._pre_run(template, job)
        except:
            pre_run_success = False
            error = "Failed to execute pre-run code:\n%s" % traceback.format_exc()
            self.log_msg(error)

        if pre_run_success:
            try:
                self._ping_backend()  # make sure we still have a connection
                self._do_run(template, job)
                do_run_success = True
            except:
                error = "Failed to execute do-run code:\n%s" % traceback.format_exc()
                self.log_msg(error)

        try:
            self._ping_backend()  # make sure we still have a connection
            self._post_run(template, job, pre_run_success, do_run_success, error)
        except:
            self.log_msg("Failed to execute post-run code:\n%s" % traceback.format_exc())

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
    # The docker image to execute the job
    docker_image = Parameter({
        DOCKER_IMAGE_TYPE:
            lambda self, value: docker_retrieve(self.context, int(value))
    })

    def __init__(self, context, config):
        """
        Initializes the executor with the backend context and configuration.

        :param context: the server context
        :type context: UFDLServerContext
        :param config: the configuration to use
        :type config: configparser.ConfigParser
        """
        super(AbstractDockerJobExecutor, self).__init__(context, config)
        self._use_current_user = (config['docker']['use_current_user'] == "true")
        self._use_gpu = False
        self._gpu_id = int(config['general']['gpu_id'])
        self._additional_gpu_flags = []

    @property
    def use_current_user(self):
        """
        Returns whether the image is run as root (False) or as current user (True).

        :return: how the image is run
        :rtype: bool
        """
        return self._use_current_user

    @property
    def gpu_id(self):
        """
        Returns the GPU ID for this executor to use (corresponds to GPU index).

        :return: the GPU ID
        :rtype: int
        """
        return self._gpu_id

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
        Additional GPU flags are appended from self._additional_gpu_flags (if GPU used).

        :return: the list of flags, empty list if none required
        :rtype: list
        """
        result = []

        if self._use_gpu:
            version = self._version(include_patch=False)
            if version is not None:
                version_num = float(version)
                if version_num >= 19.03:
                    result.append('--gpus="device=%s"' % str(self.gpu_id))
                else:
                    result.append("--runtime=nvidia")
            result.extend(self._additional_gpu_flags)

        return result

    def _registry_login_required(self):
        """
        Returns whether it is necessary to log into the registry.

        :return: True if necessary to log in
        :rtype: bool
        """
        return (self.docker_image[KEY_REGISTRY_USERNAME] is not None) \
               and (self.docker_image[KEY_REGISTRY_USERNAME] != "")

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
        if self._execute_can_use_stdin():
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

    def _run_image(self, image, docker_args=None, volumes=None, image_args=None, command_progress_parser=None, job=None):
        """
        Runs the image with the specified parameters.
        For updating a job's progress, a progress parser method can be supplied. For a dummy implemented and
        explanation of parameters see: dummy_command_progress_parser

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
        return self._execute(cmd, always_return=False, command_progress_parser=command_progress_parser, job=job)

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

        # docker image
        if self._registry_login_required():
            res = self._login_registry(
                self.docker_image[KEY_REGISTRY_URL],
                self.docker_image[KEY_REGISTRY_USERNAME],
                self.docker_image[KEY_REGISTRY_PASSWORD])
            if res is not None:
                logger().fatal("Failed to log into registry")
                raise Exception(self._to_logentry(res, [self.docker_image[KEY_REGISTRY_USERNAME], self.docker_image[KEY_REGISTRY_PASSWORD]]))
        self._use_gpu = not (str(self.docker_image[KEY_CPU]).lower() == "true")
        self._fail_on_error(self._pull_image(self.docker_image[KEY_IMAGE_URL]))
        return True

    def _post_run(self, template, job, pre_run_success, do_run_success, error):
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
        :param error: any error that may have occurred, None if none occurred
        :type error: str
        """
        if self.docker_image is not None:
            if self._registry_login_required():
                self._logout_registry(self.docker_image[KEY_REGISTRY_URL])
            #self.docker_image = None

        super()._post_run(template, job, pre_run_success, do_run_success, error)

    def can_run(self, job, template, hardware_info):
        """
        Checks if this job-executor is capable of running on the current node.

        :param template: the job template that was applied
        :type template: dict
        :param job: the job with the actual values for inputs and parameters
        :type job: dict
        :param hardware_info: the hardware info to use
        :type hardware_info: dict
        :return: The reason the job can't be run, or None if it can.
        :rtype: str|None
        """
        # Check any super-conditions
        super_reason = super().can_run(job, template, hardware_info)
        if super_reason is not None:
            return super_reason

        # If we have no GPU or compatible software, the image must be CPU-runnable
        no_gpu_reason = (
            f"Node has no GPUs"
            if 'gpus' not in hardware_info or len(hardware_info['gpus']) == 0 else
            f"Node has no GPU driver"
            if 'driver' not in hardware_info else
            f"Node has no CUDA version"
            if 'cuda' not in hardware_info else
            f"Node GPU has no compute capability"
            if 'compute' not in hardware_info["gpus"][0] else
            None
        )
        if no_gpu_reason is not None:
            if not self.docker_image['cpu']:
                return no_gpu_reason + " and Docker image is not CPU-only"
            else:
                return None

        # Get the information about the CUDA version in the Docker image
        cuda = cuda_retrieve(self.context, self.docker_image['cuda_version'])

        # Make sure the node supports the CUDA version and driver version
        if cuda['version'] > hardware_info["cuda"]:
            return f"Node's CUDA version ({hardware_info['cuda']}) is too low for Docker image (requires >= {cuda['version']})"
        elif cuda['min_driver_version'] > hardware_info["driver"]:
            return f"Node's driver version ({hardware_info['driver']}) is too low for Docker image (requires >= {cuda['min_driver_version']})"

        # Get the minimum hardware generation required by the Docker image
        min_hardware_generation = hardware_retrieve(self.context, self.docker_image['min_hardware_generation'])

        # Make sure our hardware is up-to-date
        if min_hardware_generation['min_compute_capability'] > hardware_info["gpus"][0]["compute"]:
            return f"Node's GPU compute capability ({hardware_info['gpus'][0]['compute']}) is too low for Docker image (requires >= {min_hardware_generation['min_compute_capability']})"

        return None


def dummy_command_progress_parser(job_executor, job, cmd_output, last_progress):
    """
    Dummy implementation of a command progress parser for providing feedback to the backend about the progress.
    Doesn't do anything, just returns the last progress.

    :param job_executor: the reference to the job executor calling this method
    :type job_executor: AbstractJobExecutor
    :param job: the current job
    :type job: dict
    :param cmd_output: the command output string to process
    :type cmd_output: str
    :param last_progress: the last reported progress (0-1)
    :type last_progress: float
    :return: returns the progress (0-1)
    :rtype: float
    """
    return last_progress
