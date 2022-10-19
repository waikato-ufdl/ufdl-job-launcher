import getpass
import os
import re
from subprocess import CompletedProcess
from abc import abstractmethod
from typing import List, Optional, Tuple, Union

from ufdl.jobtypes.base import String, UFDLJSONType
from ufdl.jobtypes.standard import PK, Name
from ufdl.jobtypes.standard.container import Array
from ufdl.jobtypes.standard.server import DockerImage, DockerImageInstance

from ufdl.pythonclient import UFDLServerContext

from wai.json.object import Absent

from ..config import UFDLJobLauncherConfig
from ..types import Job, Template
from .._logging import logger
from .._node import HardwareInfo
from .descriptors import Parameter
from .parsers import CommandProgressParser
from ._AbstractJobExecutor import AbstractJobExecutor
from ._types import ContractType

KEY_CPU = 'cpu'

KEY_IMAGE_URL = 'url'

KEY_REGISTRY_PASSWORD = 'registry_password'

KEY_REGISTRY_USERNAME = 'registry_username'

KEY_REGISTRY_URL = 'registry_url'

# Regular expression which matches a boolean parameter replacement string
BOOL_TEMPLATE_PATTERN: str = '''
    \\$\\{                      # Opening brace
    (?P<use_case>[+-])          # Whether to use the value on true/false 
    (?P<param_name>.+)          # The name of the boolean parameter
    :                           # Colon
    (?P<value>.*)               # The string to use when the parameter is false
    \\}                         # Closing brace
    '''
BOOL_TEMPLATE_MATCHER = re.compile(BOOL_TEMPLATE_PATTERN, re.VERBOSE)


class AbstractDockerJobExecutor(AbstractJobExecutor[ContractType]):
    """
    For executing jobs via docker images.
    """
    # The configuration of the job's execution
    body: Union[str, Tuple[str, ...]] = Parameter(
        String(),
        Array(String())
    )

    def __init__(
            self,
            context: UFDLServerContext,
            config: UFDLJobLauncherConfig,
            template: Template,
            job: Job,
            docker_image_type: Optional[DockerImage] = None
    ):
        """
        Initializes the executor with the backend context and configuration.

        :param context: the server context
        :type context: UFDLServerContext
        :param config: the configuration to use
        :type config: configparser.ConfigParser
        """
        super(AbstractDockerJobExecutor, self).__init__(context, config, template, job)
        self._use_current_user = config.docker.use_current_user
        self._use_gpu = False
        self._gpu_id = config.general.gpu_id
        self._additional_gpu_flags = []

        if docker_image_type is None:
            docker_image_type = self._extract_docker_image_type_from_contract(self._contract)

        # The docker image to execute the job
        self.docker_image: DockerImageInstance = Parameter(
            docker_image_type,
            PK((docker_image_type,)),
            Name((docker_image_type,))
        )

    @classmethod
    @abstractmethod
    def _extract_docker_image_type_from_contract(cls, contract: ContractType) -> DockerImage:
        """
        Extracts the type of docker image this executor will use from its contract.

        :param contract:
                    The contract-type being implemented by the executor.
        :return:
                    The docker-image type required by the executor.
        """
        raise NotImplementedError(cls._extract_docker_image_type_from_contract.__qualname__)

    @property
    def use_current_user(self) -> bool:
        """
        Returns whether the image is run as root (False) or as current user (True).

        :return: how the image is run
        """
        return self._use_current_user

    @property
    def gpu_id(self) -> int:
        """
        Returns the GPU ID for this executor to use (corresponds to GPU index).

        :return: the GPU ID
        """
        return self._gpu_id

    def _version(self, include_patch: bool = True) -> Optional[str]:
        """
        Returns the docker version.

        :param include_patch: whether to include the patch version as well next to major/minor
        :return: the version string, None if failed to obtain
        """

        res = self._execute(["docker", "--version"], no_sudo=True, capture_output=True)
        if res.returncode > 0:
            return None

        stdout = res.stdout

        if stdout is None:
            return None

        result = (
            stdout.decode() if isinstance(stdout, bytes)
            else stdout if isinstance(stdout, str)
            else stdout[0]
        ).strip()
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

    def _gpu_flags(self) -> List[str]:
        """
        If the GPU is to be used, returns the relevant flags as list.
        Additional GPU flags are appended from self._additional_gpu_flags (if GPU used).

        :return: the list of flags, empty list if none required
        """
        result: List[str] = []

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

    def _registry_login_required(self) -> bool:
        """
        Returns whether it is necessary to log into the registry.

        :return: True if necessary to log in
        """
        return self.docker_image.registry_username not in (Absent, None, "")

    def _login_registry(self, registry: str, user: str, password: str) -> Optional[CompletedProcess]:
        """
        Logs into the specified registry.

        :param registry: the registry URL to log into
        :param user: the user name for the registry
        :param password: the password for the registry
        :return: None if successfully logged in, otherwise subprocess.CompletedProcess
        """
        if self._execute_can_use_stdin():
            return self._execute(["docker", "login", "-u", user, "--password-stdin", registry], always_return=False, stdin=password, hide=[user])
        else:
            return self._execute(["docker", "login", "-u", user, "-p", password, registry], always_return=False, hide=[user, password])

    def _logout_registry(self, registry: str) -> Optional[CompletedProcess]:
        """
        Logs out of the specified registry.

        :param registry: the registry URL to log out from
        :return: None if successfully logged out, otherwise subprocess.CompletedProcess
        """
        return self._execute(["docker", "logout", registry], always_return=False)

    def _pull_image(self, image: str) -> Optional[CompletedProcess]:
        """
        Pulls the requested image.

        :param image: the image to pull
        :return: None if successfully pulled, otherwise subprocess.CompletedProcess
        """
        return self._execute(["docker", "pull", image], always_return=False)

    def _expand_template(self) -> Union[str, Tuple[str, ...]]:
        """
        Expands all parameters in the template code and returns the updated template string.

        :return: the expanded template
        :rtype: str
        """
        # Get the body template
        result = self.body

        # Get the defined parameters and their values
        parameter_values = {
            parameter: getattr(self, parameter)
            for parameter, is_template_defined in self._parameters()
            if is_template_defined
        }

        # Parse any additional template-defined parameters
        parameter_values.update({
            parameter: Parameter.parse_parameter(parameter, (UFDLJSONType(),), self)
            for parameter in self.template['parameters']
            if parameter not in parameter_values
        })

        for parameter, value in parameter_values.items():
            # Bool parameters have the true/false replacements defined in the body itself
            if isinstance(value, bool):
                def replacer(string: str) -> Optional[str]:
                    matches = list(BOOL_TEMPLATE_MATCHER.finditer(string))
                    for match in reversed(matches):
                        if match.group('param_name') == parameter:
                            use_case = True if match.group('use_case') == '+' else False
                            replacement = match.group('value') if use_case == value else ""
                            string = f"{string[:match.start()]}{replacement}{string[match.end():]}"
                            if string == "":
                                return None
                    return string

            # Other types just replace the parameter name with its string representation
            else:
                def replacer(string: str) -> str:
                    return string.replace("${" + parameter + "}", str(value))

            if isinstance(result, str):
                result = replacer(result)
                if result is None:
                    result = ""
            else:  # result: Tuple[str, ...]
                result = tuple(
                    replaced_string
                    for replaced_string in (replacer(string) for string in result)
                    if replaced_string is not None
                )

        return result

    def _run_image(self, image, docker_args=None, volumes=None, image_args=None, command_progress_parser: Optional[CommandProgressParser] = None):
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
        return self._execute(cmd, always_return=False, command_progress_parser=command_progress_parser)

    def _pre_run(self) -> bool:
        """
        Hook method before the actual job is run.

        :return: whether successful
        """
        if not super()._pre_run():
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

    def _post_run(self, pre_run_success: bool, do_run_success: bool, error: Optional[str]) -> None:
        """
        Hook method after the actual job has been run. Will always be executed.

        :param pre_run_success: whether the pre_run code was successfully run
        :param do_run_success: whether the do_run code was successfully run (only gets run if pre-run was successful)
        :param error: any error that may have occurred, None if none occurred
        """
        if self.docker_image is not None:
            if self._registry_login_required():
                self._logout_registry(self.docker_image.registry_url)

        super()._post_run(pre_run_success, do_run_success, error)

    def can_run(self, hardware_info: HardwareInfo) -> Optional[str]:
        """
        Checks if this job-executor is capable of running on the current node.

        :param hardware_info: the hardware info to use
        :return: The reason the job can't be run, or None if it can.
        """
        # Check any super-conditions
        super_reason = super().can_run(hardware_info)
        if super_reason is not None:
            return super_reason

        # If we have no GPU or compatible software, the image must be CPU-runnable
        no_gpu_reason = (
            f"Node has no GPUs" if hardware_info.gpus is None or len(hardware_info.gpus) == 0
            else f"Node has no GPU driver" if hardware_info.driver is None
            else f"Node has no CUDA version" if hardware_info.cuda is None
            else f"Node GPU has no compute capability" if hardware_info.gpus[0].compute is None
            else None
        )
        if no_gpu_reason is not None:
            return (
                f"{no_gpu_reason} and Docker image is not CPU-only" if not self.docker_image.cpu
                else None
            )

        # Make sure the node supports the CUDA version and driver version
        cuda = self.docker_image.cuda_version
        if cuda.version > hardware_info.cuda:
            return f"Node's CUDA version ({hardware_info.cuda}) is too low for Docker image (requires >= {cuda.version})"
        elif cuda.min_driver_version > hardware_info.driver:
            return f"Node's driver version ({hardware_info.driver}) is too low for Docker image (requires >= {cuda.min_driver_version})"

        # Make sure our hardware is up-to-date
        min_hardware_generation = self.docker_image.min_hardware_generation
        if min_hardware_generation is not None and min_hardware_generation.min_compute_capability > hardware_info.gpus[0].compute:
            return f"Node's GPU compute capability ({hardware_info.gpus[0].compute}) is too low " \
                   f"for Docker image (requires >= {self.docker_image.min_hardware_generation.min_compute_capability})"

        return None
