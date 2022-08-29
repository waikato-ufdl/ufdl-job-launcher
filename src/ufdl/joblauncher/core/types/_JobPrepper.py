from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

from ._Job import Job

if TYPE_CHECKING:
    from ..executors import AbstractJobExecutor


class JobPrepper(ABC):
    @abstractmethod
    def prepare_job(self, job: Job) -> Optional['AbstractJobExecutor']:
        """
        Prepares the job.

        :param job:
                    The job to prepare.
        :return:
                    The appropriate executor for the job, or None if there are none.
        """
        raise NotImplemented(self.prepare_job.__qualname__)
