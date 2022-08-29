from .._sleep import SleepSchedule
from ._core import generate_filter
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.jobs.job import list as list_jobs

from ..config import UFDLJobLauncherConfig
from ..executors import AbstractJobExecutor
from ..types import JobPrepper, Poller


class Simple(Poller):
    """
    Simply polls the backend every X seconds for new jobs.
    """
    def poll(
            self,
            context: UFDLServerContext,
            config: UFDLJobLauncherConfig,
            job_prepper: JobPrepper,
            debug: bool = False
    ) -> AbstractJobExecutor:
        result = None
        sleep = SleepSchedule(config.poll_simple.interval, debug=debug, debug_msg="Waiting for %s seconds before next poll")
        while result is None:
            jobs = list_jobs(context, filter_spec=generate_filter(debug=debug))
            for job in jobs:
                result = job_prepper.prepare_job(job)
                if result is not None:
                    break

            if result is None:
                sleep.sleep()
                sleep.next()

        return result
