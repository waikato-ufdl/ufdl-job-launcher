import logging
from typing import Optional

_logger: Optional[logging.Logger] = None
""" the global logger instance to use. """


def init_logger(debug: bool) -> None:
    """
    Initializes the logging.

    :param debug: whether to use debug level or just info
    """
    global _logger
    logging.basicConfig()
    _logger = logging.getLogger("ufdl.joblauncher")
    print("Initializing logging (debug=%s)" % str(debug))
    if debug:
        _logger.setLevel(logging.DEBUG)
    else:
        _logger.setLevel(logging.INFO)


def logger() -> logging.Logger:
    """
    Returns the logger instance.

    :return: the logger
    """
    if _logger is None:
        init_logger(False)
    return _logger
