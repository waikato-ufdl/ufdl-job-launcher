import getpass
import os
import re
import shlex
import subprocess
from abc import abstractmethod
from typing import Optional, Tuple, Union

from ufdl.jobtypes.base import String, UFDLJSONType
from ufdl.jobtypes.standard import PK, Name
from ufdl.jobtypes.standard.container import Array
from ufdl.jobtypes.standard.server import DockerImage, DockerImageInstance

from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.dataset import clear as dataset_clear, download as dataset_download
from ufdl.pythonclient.functional.core.nodes.cuda import retrieve as cuda_retrieve
from ufdl.pythonclient.functional.core.nodes.hardware import retrieve as hardware_retrieve

from wai.json.raw import RawJSONObject

from .._logging import logger
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

    dataset_options: Union[str, Tuple[str, ...]] = Parameter(
        Array(String()),
        String()
    )

    # The configuration of the job's execution
    body: Union[str, Tuple[str, ...]] = Parameter(
        String(),
        Array(String())
    )

    def __init__(self, context, config, template, job):
        """
        Initializes the executor with the backend context and configuration.

        :param context: the server context
        :type context: UFDLServerContext
        :param config: the configuration to use
        :type config: configparser.ConfigParser
        """
        super(AbstractDockerJobExecutor, self).__init__(context, config, template, job)
        self._use_current_user = (config['docker']['use_current_user'] == "true")
        self._use_gpu = False
        self._gpu_id = int(config['general']['gpu_id'])
        self._additional_gpu_flags = []

        docker_image_type = self._extract_domain_type_from_contract(self._contract)

        # The docker image to execute the job
        self.docker_image: DockerImageInstance = Parameter(
            docker_image_type,
            PK((docker_image_type,)),
            Name((docker_image_type,))
        )

    @classmethod
    @abstractmethod
    def _extract_domain_type_from_contract(cls, contract: ContractType) -> DockerImage:
        """
        Extracts the type of docker image this executor will use from its contract.

        :param contract:
                    The contract-type being implemented by the executor.
        :return:
                    The docker-image type required by the executor.
        """
        raise NotImplementedError(cls._extract_domain_type_from_contract.__qualname__)

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

    def _download_dataset(self, pk: int, clear_dataset: bool = False) -> str:
        """
        Downloads the dataset.

        :param pk:
                    The primary key of the dataset to download.
        :param clear_dataset:
                    Whether to clear the dataset first.
        :return:
                    The archive filename.
        """
        # clear dataset
        if clear_dataset:
            dataset_clear(self.context, pk)

        # download dataset
        data = self.job_dir + "/data.zip"
        options = self.dataset_options
        self.log_msg("Downloading dataset:", pk, "-> options='" + str(options) + "'", "->", data)
        with open(data, "wb") as zip_file:
            for b in dataset_download(self.context, pk, annotations_args=shlex.split(options) if isinstance(options, str) else list(options)):
                zip_file.write(b)

        return data

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
                def replacer(string):
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
                def replacer(string):
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

    def _pre_run(self):
        """
        Hook method before the actual job is run.

        :return: whether successful
        :rtype: bool
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

    def _post_run(self, pre_run_success, do_run_success, error):
        """
        Hook method after the actual job has been run. Will always be executed.

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

        super()._post_run(pre_run_success, do_run_success, error)

    def can_run(self, hardware_info):
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
        super_reason = super().can_run(hardware_info)
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
            if not self.docker_image.cpu:
                return no_gpu_reason + " and Docker image is not CPU-only"
            else:
                return None

        # Get the information about the CUDA version in the Docker image
        cuda = cuda_retrieve(self.context, self.docker_image['cuda_version'])

        # Make sure the node supports the CUDA version and driver version
        if self.docker_image.cuda_version.version > hardware_info["cuda"]:
            return f"Node's CUDA version ({hardware_info['cuda']}) is too low for Docker image (requires >= {cuda['version']})"
        elif self.docker_image.cuda_version.min_driver_version > hardware_info["driver"]:
            return f"Node's driver version ({hardware_info['driver']}) is too low for Docker image (requires >= {cuda['min_driver_version']})"

        # Make sure our hardware is up-to-date
        min_hardware_generation = self.docker_image.min_hardware_generation
        if min_hardware_generation is not None and min_hardware_generation.min_compute_capability > hardware_info["gpus"][0]["compute"]:
            return f"Node's GPU compute capability ({hardware_info['gpus'][0]['compute']}) is too low " \
                   f"for Docker image (requires >= {self.docker_image.min_hardware_generation.min_compute_capability})"

        return None
