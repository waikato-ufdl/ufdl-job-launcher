from typing import List

from ..base import ConfigSection, ConfigProperty
from .._util import str2bool, list_of, enum_of


class GeneralConfigSection(ConfigSection):
    """
    General configuration of the job-launcher's execution.

    TODO: Comment property-descriptions.
    """
    debug: bool = ConfigProperty(str2bool)
    pip_no_cache: bool = ConfigProperty(str2bool)
    compression: int = ConfigProperty(int)
    poll: str = ConfigProperty(enum_of(str, "simple"))
    gpu_id: int = ConfigProperty(int)
    cancel_check_wait: int = ConfigProperty(int)
    poll_backenderror_wait: List[int] = ConfigProperty(list_of(int, sep=','), default=[10])
