from typing import List

from ..base import ConfigSection, ConfigProperty
from .._util import list_of


class PollSimpleConfigSection(ConfigSection):
    """
    Configuration of the polling strategy when checking for new jobs to execute.
    """
    interval: List[int] = ConfigProperty(list_of(int, sep=','))
    """List of intervals (in seconds) to wait between polls to the backend."""
