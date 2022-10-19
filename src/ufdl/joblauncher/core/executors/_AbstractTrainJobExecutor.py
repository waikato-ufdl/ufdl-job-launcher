import shlex
from abc import ABC
from typing import Tuple, Union

from ufdl.jobcontracts.standard import Train

from ufdl.jobtypes.base import String
from ufdl.jobtypes.standard.container import Array
from ufdl.jobtypes.standard.server import DockerImage, DatasetInstance

from .descriptors import Parameter
from ._AbstractDockerJobExecutor import AbstractDockerJobExecutor
from ._util import download_dataset


class AbstractTrainJobExecutor(AbstractDockerJobExecutor[Train], ABC):

    dataset_options: Union[str, Tuple[str, ...]] = Parameter(
        Array(String()),
        String()
    )

    @property
    def dataset(self) -> DatasetInstance:
        return self[self.contract.dataset]

    def _download_dataset(
            self,
            pk: int,
            output_dir: str
    ):
        """
        Downloads the dataset.

        :param pk:
                    The primary key of the dataset to download.
        :param output_dir:
                    Where to download the dataset to.
        """
        download_dataset(
            self.context,
            pk,
            self.template['domain'],
            output_dir,
            shlex.split(self.dataset_options) if isinstance(self.dataset_options, str)
            else list(self.dataset_options)
        )

    @classmethod
    def _extract_docker_image_type_from_contract(cls, contract: Train) -> DockerImage:
        return DockerImage((contract.domain_type, contract.framework_type))
