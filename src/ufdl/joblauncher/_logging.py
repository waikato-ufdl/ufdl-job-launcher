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
