from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ufdl.pythonclient import UFDLServerContext

from ..config import UFDLJobLauncherConfig
from ._JobPrepper import JobPrepper

if TYPE_CHECKING:
    from ..executors import AbstractJobExecutor


class Poller(ABC):
    @abstractmethod
    def poll(
            self,
            context: UFDLServerContext,
            config: UFDLJobLauncherConfig,
            job_prepper: JobPrepper,
            debug: bool
    ) -> 'AbstractJobExecutor':
        """
        Polls for the next job.

        :param context: the UFDL server context
        :param config: the configuration to use
        :param job_prepper: function which creates an executor for a job
        :param debug: whether to output debugging information
        :return: the next job executor
        """
        raise NotImplementedError(self.poll.__qualname__)
