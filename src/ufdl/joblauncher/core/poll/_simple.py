import configparser
import time
from .._logging import logger
from .._sleep import SleepSchedule
from ._core import generate_filter
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.jobs.job import list as list_jobs


def poll(context, config, prepare_job, debug=False):
    """
    Simply polls the backend every X seconds for new jobs.

    :param context: the UFDL server context
    :type context: UFDLServerContext
    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param prepare_job: function which creates an executor for a job
    :type prepare_job: (dict) -> ufdl.joblauncher.AbstractJobExecutor|None
    :param debug: whether to output debugging information
    :type debug: bool
    :return: the next job executor
    :rtype: ufdl.joblauncher.AbstractJobExecutor
    """
    result = None
    sleep = SleepSchedule(config['poll_simple']['interval'], debug=debug, debug_msg="Waiting for %s seconds before next poll")
    while result is None:
        jobs = list_jobs(context, filter_spec=generate_filter(debug=debug))
        for job in jobs:
            result = prepare_job(job)
            if result is not None:
                break

        if result is None:
            sleep.sleep()
            sleep.next()

    return result
