import configparser
import pika
from ufdl.pythonclient import UFDLServerContext
from ufdl.joblauncher._logging import logger


def poll(context, config, hardware_info, debug=False):
    """
    Waits on the RabbitMQ broadcast queue for notifications of jobs.

    :param context: the UFDL server context
    :type context: UFDLServerContext
    :param hardware_info: the hardware info (see _node.)
    :type hardware_info: dict
    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param debug: whether to output debugging information
    :type debug: bool
    :return: the next job
    :rtype: dict
    """
    raise NotImplemented()
