from ..base import ConfigSection, ConfigProperty
from .._util import str2bool


class DockerConfigSection(ConfigSection):
    """
    The job-launcher's configuration for managing Docker containers.

    TODO: Comment property-descriptions.
    """
    work_dir: str = ConfigProperty(str)
    cache_dir: str = ConfigProperty(str)
    use_sudo: bool = ConfigProperty(str2bool)
    ask_sudo_pw: bool = ConfigProperty(str2bool)
    use_current_user: bool = ConfigProperty(str2bool)
