import configparser
import os
from typing import Optional

from .sections import *
from ._SYSTEMWIDE_CONFIG import SYSTEMWIDE_CONFIG


class UFDLJobLauncherConfig:
    """
    Represents a loaded configuration file for the job-launcher. Could be better defined
    declaratively (similar to how ConfigPropertys are for ConfigSections) but PyCharm
    can't seem to see through the double-layer of descriptors.
    """
    def __init__(
            self,
            config_file: Optional[str] = None
    ):
        # Use the system configuration if no other is provided
        if config_file is None:
            config_file = SYSTEMWIDE_CONFIG

        # Check the file exists
        if not os.path.exists(config_file):
            raise Exception(f"Config file '{config_file}' does not exist!")

        # Do the raw parse of the config file
        config = configparser.ConfigParser()
        config.read(config_file)

        # Parse the individual sections of the configuration file
        self._general = GeneralConfigSection("general", config, config_file)
        self._backend = BackendConfigSection("backend", config, config_file)
        self._docker = DockerConfigSection("docker", config, config_file)
        self._poll_simple = PollSimpleConfigSection("poll_simple", config, config_file)

    @property
    def general(self) -> GeneralConfigSection:
        return self._general

    @property
    def backend(self) -> BackendConfigSection:
        return self._backend

    @property
    def docker(self) -> DockerConfigSection:
        return self._docker

    @property
    def poll_simple(self) -> PollSimpleConfigSection:
        return self._poll_simple
