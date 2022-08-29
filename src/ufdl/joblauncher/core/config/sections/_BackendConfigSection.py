from ..base import ConfigSection, ConfigProperty


class BackendConfigSection(ConfigSection):
    """
    Configuration of the job-launcher's connection to the UFDL backend.
    """
    url: str = ConfigProperty(str)
    """The URL to route connections to."""

    user: str = ConfigProperty(str)
    """The username to authenticate with the backend."""

    password: str = ConfigProperty(str)
    """The password to authenticate with the backend."""
