import configparser
import os

SYSTEMWIDE_CONFIG = "/etc/ufdl/job-launcher.conf"
""" the system-wide config file. """


def _check_section(config_file, config, section, keys=None):
    """
    Ensures that the required section is present in the config object.
    Raises an exception if not present.

    :param config: the config object to check
    :type config: configparser.ConfigParser
    :param section: the name of the section to look for
    :type section: str
    :param keys: the key names that must be present, use None to skip check
    :type keys: list
    """
    if section not in config.sections():
        raise Exception("Error in config file '%s': missing section '%s'" % (config_file, section))
    if keys is not None:
        for key in keys:
            if key not in config[section]:
                raise Exception("Error in config file '%s': missing key '%s' in section '%s'" % (config_file, key, section))


def load_config(config_file=None):
    """
    Loads the configuration from disk. If no filename is provided,
    the system-wide one will be loaded. Expected sections: backend, docker

    :param config_file: the config file to load, None for system-wide one
    :type config_file: str
    :return: the configuration object
    :rtype: configparser.ConfigParser
    """
    if config_file is None:
        config_file = SYSTEMWIDE_CONFIG

    if not os.path.exists(config_file):
        raise Exception("Config file '%s' does not exist!" % config_file)

    config = configparser.ConfigParser()
    config.read(config_file)
    _check_section(config_file, config, "general", ["debug", "pip_no_cache", "compression", "poll", "gpu_id", "cancel_check_wait"])
    _check_section(config_file, config, "backend", ["url", "user", "password"])
    _check_section(config_file, config, "docker", ["work_dir", "cache_dir", "use_sudo", "ask_sudo_pw", "use_current_user"])
    _check_section(config_file, config, "poll_simple", ["interval"])
    return config
