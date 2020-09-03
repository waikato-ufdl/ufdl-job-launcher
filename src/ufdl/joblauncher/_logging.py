import logging

_logger = None
""" the global logger instance to use. """

_logger_counter = 0
""" calling pip.main disables logging somehow, we create a new logger name each time we initialize logging. """

def init_logger(debug):
    """
    Initializes the logging.

    :param debug: whether to use debug level or just info
    :type debug: bool
    """
    global _logger
    global _logger_counter
    _logger_counter += 1
    logging.basicConfig()
    _logger = logging.getLogger("ufdl.joblauncher-%s" % str(_logger_counter))
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
