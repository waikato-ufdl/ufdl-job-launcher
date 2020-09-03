import logging

_logger = None
""" the global logger instance to use. """

def init_logger(debug):
    """
    Initializes the logging.

    :param debug: whether to use debug level or just info
    :type debug: bool
    """
    global _logger
    logging.basicConfig()
    _logger = logging.getLogger("ufdl.joblauncher")
    print("Initializing logging (debug=%s)" % str(debug))
    if debug:
        _logger.setLevel(logging.DEBUG)
    else:
        _logger.setLevel(logging.INFO)


def logger():
    """
    Returns the logger instance.

    :return: the logger
    :rtype: logging.Logger
    """
    if _logger is None:
        init_logger(False)
    return _logger


def backup_root_logger():
    """
    Creates a backup of the root logger.

    :return: the backup
    :rtype: dict
    """
    result = dict()
    result['level'] = logging.root.level
    result['disabled'] = logging.root.disabled
    result['handlers'] = logging.root.handlers[:]
    return result

def restore_root_logger(backup):
    """
    Restores the root logger from the backup.

    :param backup: the backup (level/disable/handlers)
    :type backup: dict
    """

    logging.root.level = backup['level']
    logging.root.disabled = backup['disabled']
    logging.root.handlers = backup['handlers']
