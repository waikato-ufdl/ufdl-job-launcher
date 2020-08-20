import configparser
import time
from .._logging import logger
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.jobs.job import list as list_jobs
from ._core import generate_filter

def poll(context, config, hardware_info, debug=False):
    """
    Simply polls the backend every X seconds for new jobs.

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
    result = None
    seconds = int(config['simple_poll']['interval'])
    while result is None:
        jobs = list_jobs(context, filter_spec=generate_filter(hardware_info, debug=debug))
        if len(jobs) > 0:
            result = jobs[0]
            return result

        if result is None:
            if debug:
                logger().debug("waiting for " + str(seconds) + " seconds before next poll")
            time.sleep(seconds)

