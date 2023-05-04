from datetime import datetime
import json
import os
from requests.exceptions import HTTPError
import shutil
import subprocess
from subprocess import CompletedProcess
import tempfile
import traceback
from typing import Any, Dict, Generic, Iterator, Optional, Tuple, Union, List
from zipfile import ZipFile

from ufdl.jobcontracts.base import UFDLJobContract, Input, Output
from ufdl.jobcontracts.initialise import initialise_server as initialise_contracts
from ufdl.jobcontracts.util import parse_contract

from ufdl.jobtypes.base import UFDLType, UFDLJSONType, InputType, OutputType
from ufdl.jobtypes.initialise import (
    initialise_server as initialise_types,
)
from ufdl.jobtypes.standard.util import Compressed, JSON
from ufdl.jobtypes.util import parse_type

from ufdl.json.core.filter import FilterSpec

from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.nodes.node import ping as node_ping
from ufdl.pythonclient.functional.core.jobs.job import add_output as job_add_output, retrieve as job_retrieve
from ufdl.pythonclient.functional.core.jobs.job import acquire_job, start_job, finish_job, progress_job

from wai.json.object import Absent
from wai.json.raw import RawJSONObject, RawJSONElement

from ..config import UFDLJobLauncherConfig
from ..types import Job, Template
from .._logging import logger
from .._node import get_ipv4, HardwareInfo
from .._utils import load_class
from .descriptors import ExtraOutput, Parameter
from .parsers import CommandProgressParser
from ._types import ContractType


class AbstractJobExecutor(Generic[ContractType]):
    """
    Ancestor for classes executing jobs.
    """
    _cls_contract: ContractType

    @classmethod
    def cls_contract(cls) -> UFDLJobContract:
        return cls._cls_contract

    def __init__(
            self,
            context: UFDLServerContext,
            config: UFDLJobLauncherConfig,
            template: Template,
            job: Job
    ):
        """
        Initializes the executor with the backend context and configuration.

        :param context: the server context
        :param config: the configuration to use
        """
        self._debug = config.general.debug
        self._keep_job_dirs = config.general.keep_job_dirs
        self._context = context
        self._work_dir = config.docker.work_dir
        self._cache_dir = config.docker.cache_dir
        self._job_dir = None
        self._use_sudo = config.docker.use_sudo
        self._ask_sudo_pw = config.docker.ask_sudo_pw
        self._log: List[Dict[str, RawJSONObject]] = []
        self._compression = config.general.compression
        self._notification_type = None
        self._template = template
        self._job = job
        self._last_cancel_check = None
        self._cancel_check_wait = config.general.cancel_check_wait
        self._job_is_cancelled = False
        self._input_value_cache = {}

        self._initialise_contracts_and_types()

        # Create an instance descriptor for the log-output as it relies on per-instance 'compression'
        self.log = ExtraOutput(Compressed(JSON(), self._compression))

        # Parse the contract-type declared in the template
        contract = parse_contract(template["type"])
        cls_contract = self.cls_contract()
        if not contract.is_subtype_of(cls_contract):
            raise Exception(f"Declared contract-type ${contract} is not usable as ${cls_contract}")
        self._contract: ContractType = contract

    @property
    def debug(self) -> bool:
        """
        Returns the debugging mode flag.

        :return: True if debugging mode on
        :rtype: bool
        """
        return self._debug

    @debug.setter
    def debug(self, value: bool):
        """
        Sets the debug flag.

        :param value: True to turn on debugging mode
        """
        self._debug = value

    @property
    def compression(self) -> int:
        """
        Returns the compression in use.

        :return: the compression (see zipfile, ZIP_STORED/0 = no compression)
        """
        return self._compression

    @compression.setter
    def compression(self, value: int):
        """
        Sets the compression to use.

        :param value: the compression (see zipfile, ZIP_STORED/0 = no compression)
        """
        self._compression = value

    @property
    def context(self) -> UFDLServerContext:
        """
        Returns the UFDL server context.

        :return: the context
        """
        return self._context

    @property
    def contract(self) -> ContractType:
        """
        Gets the contract that this executor is fulfilling.
        """
        return self._contract

    @property
    def work_dir(self) -> str:
        """
        Returns the working directory. Used for temp files and dirs.

        :return: the directory
        """
        return self._work_dir

    @property
    def cache_dir(self) -> str:
        """
        Returns the cache directory. Used for base models etc.

        :return: the directory
        """
        return self._cache_dir

    @property
    def job_dir(self) -> str:
        """
        Returns the working directory. Used for temp files and dirs.

        :return: the directory for the job (gets created before actual run and deleted afterwards again)
        """
        return self._job_dir

    @property
    def use_sudo(self) -> bool:
        """
        Returns whether commands are prefixed with sudo.

        :return: true if prefixing commands
        """
        return self._use_sudo

    @property
    def ask_sudo_pw(self) -> bool:
        """
        Returns whether to prompt user in console for sudo password.

        :return: true if prompting
        """
        return self._ask_sudo_pw

    @property
    def notification_type(self) -> str:
        """
        Returns the type of notification to send out.

        :return: the type of notification (eg email)
        """
        return self._notification_type

    @property
    def job(self) -> Job:
        """
        Returns the job of this executor.

        :return: the job
        """
        return self._job

    @property
    def job_pk(self) -> int:
        return int(self._job['pk'])

    @property
    def template(self) -> Template:
        """
        Returns the template of this executor.

        :return: the template
        """
        return self._template

    def __getattribute__(self, item: str) -> Any:
        # Hack to make instance descriptors work like class descriptors
        attribute = super().__getattribute__(item)
        if isinstance(attribute, Parameter):
            return attribute.__get__(self, type(self))
        return attribute

    def __setattr__(self, key: str, value: Any) -> None:
        # Hack to make instance descriptors work like class descriptors
        try:
            attribute = super().__getattribute__(key)
            if isinstance(attribute, ExtraOutput):
                return attribute.__set__(self, value)
        except AttributeError:
            pass

        # Need to call __set_name__ manually
        if isinstance(value, (Parameter, ExtraOutput)):
            value.__set_name__(type(self), key)

        return super().__setattr__(key, value)

    def __getitem__(self, item: Input[InputType]) -> InputType:
        # Make sure we own the input
        our_input = self.contract.inputs.get(item.name, None)
        if item is not our_input:
            raise Exception(f"Unowned input '{item.name}'")

        if item in self._input_value_cache:
            return self._input_value_cache[item]

        # Get the JSON value and type from the job description
        input_value_and_type = self.job['input_values'][item.name]
        input_value_json = input_value_and_type['value']
        input_type_string = input_value_and_type['type']

        # Parse the type
        input_type = parse_type(input_type_string)
        assert isinstance(input_type, UFDLJSONType)

        # Use the type to parse the value
        parsed_value = input_type.parse_json_value(input_value_json)

        self._input_value_cache[item] = parsed_value

        return parsed_value

    def __setitem__(self, key: Union[Output[OutputType], ExtraOutput[OutputType]], value: OutputType):
        # Special handling for extra outputs
        if isinstance(key, ExtraOutput):
            return key.__set__(self, value)

        # Make sure we own the output
        our_output = self.contract.outputs.get(key.name, None)
        if key is not our_output:
            raise Exception(f"Unowned output '{key.name}'")

        self._add_output_to_job(key.name, key.type, value)

    def _initialise_contracts_and_types(self) -> None:
        def list_function(table_name: str, filters: FilterSpec) -> List[RawJSONObject]:
            from ufdl.pythonclient.functional._base_actions import list as list_server
            return list_server(self.context, f"v1/{table_name}", filters)

        def download_function(table_name: str, pk: int) -> Union[bytes, Iterator[bytes]]:
            from ufdl.pythonclient.functional.core._mixin_actions import download
            return download(self.context, f"v1/{table_name}", pk)

        initialise_types(
            list_function,
            download_function,
            {
                job_type['name']: load_class(
                    job_type['cls'],
                    UFDLType,
                    required_packages=[job_type['pkg']],
                    debug=True
                )
                for job_type in list_function('job-types', FilterSpec())
            }
        )

        initialise_contracts({
            job_contract['name']: load_class(
                job_contract['cls'],
                UFDLJobContract,
                required_packages=[job_contract['pkg']],
                debug=True
            )
            for job_contract in list_function('job-contracts', FilterSpec())
        })

    def _parameters(self) -> Iterator[Tuple[str, bool]]:
        """
        Iterates over the parameters of this executor. Each pair is the name of the
        parameter, and whether it is defined in the server template.

        :return:
                    Iterator of (parameter name, template-defined) pairs.
        """
        for attr_name in dir(self):
            # Need to use super __getattribute__ method to return parameter descriptors,
            # not values
            attr_value = super().__getattribute__(attr_name)
            if isinstance(attr_value, Parameter):
                is_defined_on_template = attr_name in self._template['parameters']
                yield attr_name, is_defined_on_template

    def _add_output_to_job(self, name: str, type: UFDLType[tuple, Any, OutputType], value: OutputType):
        job_add_output(
            self.context,
            self.job_pk,
            name,
            str(type),
            type.format_python_value(value)
        )

    def _add_log(self, data: RawJSONObject) -> None:
        """
        Adds the data under a new timestamp to the internal log.

        :param data: the object to add
        """
        self._log.append(
            {
                str(datetime.now()): data
            }
        )

    def _obscure(
            self,
            args: List[str],
            hide: Optional[List[str]]
    ) -> List[str]:
        """
        Obscures/masks the specified list of strings in the

        :param args: the list of string arguments to process
        :param hide: the list of strings to hide
        :return: the obscured list of strings
        """
        if (hide is None) or (len(hide) == 0):
            return args

        to_hide = set(hide)
        return [
            "***" if arg in to_hide else arg
            for arg in args
        ]

    def log_msg(self, *args: Any) -> None:
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

    def log_file(self, msg: str, filename: str) -> None:
        """
        Reads the specified file and then logs the message and its content.

        :param msg: the message to prepend the file content
        :param filename: the file to read
        """
        try:
            with open(filename, "r") as lf:
                lines = lf.readlines()

            joined_lines = "".join(lines)

            self.log_msg(
                f"{msg}\n"
                f"{joined_lines}"
            )
        except:
            self.log_msg(
                f"Failed to read file: {filename}\n"
                f"{traceback.format_exc()}"
            )

    def _mktmpdir(self) -> str:
        """
        Creates a temp directory in the working directory and returns the absolute path.

        :return: the tmp directory that has been created
        """
        return tempfile.mkdtemp(suffix="", prefix="", dir=self.work_dir)

    def _mkdir(self, directory: str):
        """
        Creates the specified directory.

        :param directory: the directory to create
        """
        self.log_msg("mkdir:", directory)
        os.mkdir(directory)

    def _rmdir(self, directory: str):
        """
        Removes the directory recursively.

        :param directory: the directory to delete
        """
        self.log_msg("rmdir:", directory)
        shutil.rmtree(directory, ignore_errors=True)

    def _to_logentry(self, completed: CompletedProcess, hide: List[str]) -> RawJSONObject:
        """
        Turns the CompletedProcess object into a log entry.

        :param completed: the CompletedProcess object to convert
        :param hide: the list of strings to obscure in the log message
        :return: the log entry
        """
        result: RawJSONObject = dict()
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

    def _ping_backend(self) -> None:
        """
        Ensuring that the connection is still live.
        """
        try:
            node_ping(self.context)
        except:
            self.log_msg(
                f"Failed to ping backend:\n"
                f"{traceback.format_exc()}"
            )

    def _execute_can_use_stdin(self, no_sudo: bool = False) -> bool:
        """
        Returns whether the _execute function can use stdin.
        Not possible if sudo is asking for password.

        :return: True if it can make use of stdin
        """
        return not (self.use_sudo and not no_sudo and self.ask_sudo_pw)

    def _execute(
            self,
            cmd: List[str],
            always_return: bool = True,
            no_sudo: bool = False,
            capture_output: bool = True,
            stdin: Optional[str] = None,
            hide: Optional[List[str]] = None,
            command_progress_parser: Optional[CommandProgressParser] = None
    ) -> Optional[CompletedProcess]:  # CompletedProcess[Union[bytes, str, List[str], None]]
        """
        Executes the command.
        For updating a job's progress, a progress parser method can be supplied. For a dummy implemented and
        explanation of parameters see: dummy_command_progress_parser

        :param cmd: the command as list of strings
        :param always_return: whether to always return the subprocess.CompletedProcess object or only
                              if the return code is non-zero
        :param no_sudo: temporarily disable sudo
        :param capture_output: whether to capture the output from stdout and stderr
        :param stdin: the text to feed into the process via stdin
        :param hide: the list of strings to obscure in the log message
        :param command_progress_parser: the parser for the command output for updating the progress in the backend
        :return: the CompletedProcess object from executing the command, uses 255 as return code in case of an
                 exception and stores the stack trace in stderr
        """
        if (stdin is not None) and (not self._execute_can_use_stdin(no_sudo)):
            raise Exception("Cannot feed data into stdin of process! E.g., when sudo is asking for password.")

        full = []
        if self.use_sudo and not no_sudo:
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
                result = CompletedProcess(full, process.returncode, stdout=stdout, stderr=stderr)  # CompletedProcess[bytes]
            else:
                stdout_list = []
                process = subprocess.Popen(full, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1)
                last_progress = 0.0
                while True:
                    # terminate job if canceled
                    if self.is_job_cancelled():
                        process.terminate()
                        break
                    line = process.stdout.readline()
                    if not line:
                        break
                    if capture_output:
                        stdout_list.append(line)
                    if capture_output and (command_progress_parser is not None):
                        try:
                            progress, progress_metadata = command_progress_parser.parse(line, last_progress)
                        except:
                            command_progress_parser = None
                            self.log_msg("Failed to parse progress output, disabling!", traceback.format_exc())
                        else:
                            if progress != last_progress or progress_metadata is not None:
                                if progress_metadata is None:
                                    progress_metadata = {}
                                self.progress(progress, **progress_metadata)
                            last_progress = progress
                if self.is_job_cancelled():
                    stdout_list.append("Job was cancelled")
                    result = CompletedProcess(full, 255, stdout=stdout_list, stderr=None)  # CompletedProcess[Optional[List[str]]]
                else:
                    retcode = process.wait()
                    result = CompletedProcess(full, retcode, stdout=stdout_list, stderr=None)  # CompletedProcess[Optional[List[str]]]
        except:
            result = CompletedProcess(full, 255, stdout=None, stderr=traceback.format_exc())  # CompletedProcess[Optional[str]]

        self._add_log(self._to_logentry(result, hide))

        if always_return or (result.returncode > 0):
            return result
        else:
            return None

    def _fail_on_error(self, completed: CompletedProcess) -> None:
        """
        Raises an exception if the CompletedProcess object has a non-zero exit code.

        :param completed: the CompletedProcess object to inspect
        :raises: Exception
        """
        if isinstance(completed, CompletedProcess):
            if completed.returncode != 0:
                args = " ".join(completed.args)
                stdout = (
                    f"\n"
                    f"stdout:\n"
                    f"{completed.stdout}" if completed.stdout is not None
                    else ""
                )
                stderr = (
                    f"\n"
                    f"stderr:\n"
                    f"{completed.stderr}" if completed.stderr is not None
                    else ""
                )
                raise Exception(
                    f"Failed to execute command:\n"
                    f"{args}\n"
                    f"return code:\n"
                    f"{completed.returncode}"
                    f"{stdout}"
                    f"{stderr}"
                )

    def _compress(
            self,
            files: List[str],
            zipfile: str,
            strip_path: Union[None, bool, str] = None
    ) -> Optional[str]:
        """
        Compresses the files and stores them in the zip file.

        :param files: the list of files to compress
        :param zipfile: the zip file to create
        :param strip_path: whether to strip the path: True for removing completely, or prefix string to remove
        :return: None if successful, otherwise error message
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
            msg = (
                f"Failed to compress files '{''', '''.join(files)}' into '{zipfile}':\n"
                f"{traceback.format_exc()}"
            )
            self.log_msg(msg)
            return msg

    def _decompress(self, zipfile: str, output_dir: str) -> Optional[str]:
        """
        Decompresses a zipfile into the specified output directory.

        :param zipfile: the zip file to decompress
        :param output_dir: the output directory
        """

        self.log_msg("Decompressing:", zipfile, "->", output_dir)

        try:
            with ZipFile(zipfile, "r") as zf:
                zf.extractall(output_dir)
        except:
            return (
                f"Failed to decompress '{zipfile}' to '{output_dir}':\n"
                f"{traceback.format_exc()}"
            )

    def _any_present(self, files: List[str]) -> bool:
        """
        Checks whether at least one of the files listed is present in the file system.

        :param files: the file names to check (absolute paths)
        :return: True if at least one present
        """
        result = False

        for f in files:
            if os.path.exists(f) and os.path.isfile(f):
                result = True
                break

        return result

    def progress(self, progress: float, **data: RawJSONElement):
        """
        Updates the server on the progress of the job.

        :param progress: the progress amount in [0.0, 1.0]
        :param data: other JSON meta-data about the progress
        """
        # Progress updates to a cancelled job will fail, so skip updating
        # the progress if we know we're cancelled already
        if self.is_job_cancelled():
            return

        try:
            progress_job(self.context, self.job_pk, progress, **data)
        except:
            self.log_msg(
                f"Failed to update backend on progress to backend:\n"
                f"{traceback.format_exc()}"
            )

            # Assume the progress update failed because the job was cancelled,
            # and perform a check to see if this is the case, so all
            # future checks will show this immediately
            self.is_job_cancelled(immediate=True)

    def _upload(
            self,
            output: Union[Output[OutputType], ExtraOutput[OutputType]],
            localfile: str,
            file_type: Optional[UFDLType[tuple, OutputType, Any]] = None
    ):
        """
        Uploads the specified file to the backend as job output.

        :param localfile: the file to upload
        """
        if file_type is None:
            file_type = output.type

        try:
            with open(localfile, "rb") as lf:
                self[output] = file_type.parse_binary_value(lf.read())
        except:
            self.log_msg("Failed to upload file (%s|%s|%s) to backend:\n%s" % (output.name, str(output.type), localfile, traceback.format_exc()))

    def _compress_and_upload(
            self,
            output: Union[Output[OutputType], ExtraOutput[OutputType]],
            files,
            zipfile,
            file_type: Optional[UFDLType[tuple, OutputType, Any]] = None,
            strip_path=True):
        """
        Compresses the files as zip file and uploads them as job output under the specified name.

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
            self._upload(output, zipfile, file_type)

    def _pre_run(self) -> bool:
        """
        Hook method before the actual job is run.

        :return: whether successful
        """
        # TODO retrieve notification type from user
        self._notification_type = "email"

        # basic info
        self.log_msg(f"IP: {get_ipv4()}")
        self.log_msg(f"Use sudo: {self.use_sudo}")
        self.log_msg(f"Ask sudo password: {self.ask_sudo_pw}")
        self.log_msg(f"Notification: {self._notification_type}")
        self.log_msg(
            f"Job:\n"
            f"{self._job}"
        )
        self.log_msg(
            f"Template:\n"
            f"{self._template}"
        )

        # jobdir
        self._job_dir = self._mktmpdir()
        self.log_msg(f"Created jobdir: {self._job_dir}")

        # acquire
        try:
            acquire_job(self.context, self.job_pk)
        except HTTPError as e:
            self.log_msg(
                f"Failed to acquire job {self.job_pk}!\n"
                f"{e.response.text}\n"
                f"{traceback.format_exc()}"
            )
            return False
        except:
            self.log_msg(
                f"Failed to acquire job {self.job_pk}!\n"
                f"{traceback.format_exc()}"
            )
            return False

        # start
        try:
            start_job(self.context, self.job_pk, self.notification_type)
        except HTTPError as e:
            self.log_msg(
                f"Failed to start job {self.job_pk}!\n"
                f"{e.response.text}\n"
                f"{traceback.format_exc()}"
            )
            return False
        except:
            self.log_msg(
                f"Failed to start job {self.job_pk}!\n"
                f"{traceback.format_exc()}"
            )
            return False

        return True

    def _do_run(self) -> None:
        """
        Executes the actual job. Only gets run if pre-run was successful.
        """
        raise NotImplementedError(self._do_run.__qualname__)

    def _post_run(
            self,
            pre_run_success: bool,
            do_run_success: bool,
            error: Optional[str]
    ) -> None:
        """
        Hook method after the actual job has been run. Will always be executed.

        :param pre_run_success: whether the pre_run code was successfully run
        :param do_run_success: whether the do_run code was successfully run (only gets run if pre-run was successful)
        :param error: any error that may have occurred, None if none occurred
        """

        # zip+upload log
        self.log = self._log

        # finish job
        try:
            if error is None:
                error = Absent
                if not pre_run_success:
                    error = "An error occurred during pre-run, check log!"
                elif not do_run_success:
                    error = "An error occurred during run, check log!"
            finish_job(self.context, self.job_pk, pre_run_success and do_run_success, self.notification_type, error=error)
        except HTTPError as e:
            self.log_msg("Failed to finish job %d!\n%s\n%s" % (self.job_pk, str(e.response.text), traceback.format_exc()))
        except:
            self.log_msg("Failed to finish job %d!\n%s" % (self.job_pk, traceback.format_exc()))

        # clean up job dir?
        if not self._keep_job_dirs:
            self._rmdir(self.job_dir)
        self._job_dir = None

    def can_run(self, hardware_info: HardwareInfo):
        """
        Checks if this job-executor is capable of running on the current node.

        :param hardware_info: the hardware info to use
        :return: The reason the job can't be run, or None if it can.
        :rtype: str|None
        """
        return None

    def is_job_cancelled(self, immediate: bool = False) -> bool:
        """
        Checks if this job has been cancelled. Queries the backend if the _job_is_cancelled flag is not set,
        (at-most every self._cancel_check_wait seconds).

        :param immediate: whether to ignore the check throttling and check immediately
        :return: Whether the job has been cancelled
        """
        if self._job_is_cancelled:
            return True

        now = datetime.now()

        if not (
            immediate
            or (self._last_cancel_check is None)
            or ((now - self._last_cancel_check).total_seconds() >= self._cancel_check_wait)
        ):
            return False

        updated_job = job_retrieve(self.context, self.job_pk)
        self._job_is_cancelled = updated_job['is_cancelled']
        self._last_cancel_check = now

        return self._job_is_cancelled

    def run(self) -> None:
        """
        Applies the template and executes the job. Raises an exception if it fails.
        """
        error = None
        do_run_success = False
        try:
            pre_run_success = self._pre_run()
        except:
            pre_run_success = False
            error = "Failed to execute pre-run code:\n%s" % traceback.format_exc()
            self.log_msg(error)

        if pre_run_success:
            try:
                self._ping_backend()  # make sure we still have a connection
                self._do_run()
                do_run_success = True
            except:
                error = "Failed to execute do-run code:\n%s" % traceback.format_exc()
                self.log_msg(error)

        try:
            self._ping_backend()  # make sure we still have a connection
            self._post_run(pre_run_success, do_run_success, error)
        except:
            self.log_msg("Failed to execute post-run code:\n%s" % traceback.format_exc())

    def __str__(self) -> str:
        """
        Returns a short description of itself.

        :return: the short description
        """
        return f"context={self.context}, workdir={self.work_dir}"
