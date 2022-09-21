from abc import ABC
import shlex
from typing import List, Tuple, Union

from typing.io import IO
from ufdl.jobcontracts.standard import Predict

from ufdl.jobtypes.base import Boolean, String
from ufdl.jobtypes.standard.container import Array
from ufdl.jobtypes.standard.server import DockerImage, DatasetInstance

from .descriptors import Parameter
from ._AbstractDockerJobExecutor import AbstractDockerJobExecutor
from ._util import download_dataset


class AbstractPredictJobExecutor(AbstractDockerJobExecutor[Predict], ABC):

    dataset_options: Union[str, Tuple[str, ...]] = Parameter(
        Array(String()),
        String()
    )

    clear_dataset: bool = Parameter(
        Boolean()
    )

    @property
    def model(self) -> Union[bytes, IO[bytes]]:
        return self[self.contract.model]

    @property
    def dataset(self) -> DatasetInstance:
        return self[self.contract.dataset]

    def _download_dataset(
            self,
            pk: int,
            output_dir: str,
            additional_source_options: Union[str, Tuple[str, ...]] = tuple()
    ):
        """
        Downloads a dataset.

        :param pk:
                    The primary key of the dataset to download.
        :param output_dir:
                    Where to download the dataset to.
        :param additional_source_options:
                    Any additional options to the ufdl-annotations-plugin source.
        """
        # Split the source options into a list
        options: List[str] = (
            shlex.split(additional_source_options) if isinstance(additional_source_options, str)
            else list(additional_source_options)
        )

        # Append the dataset options parameter
        options += (
            shlex.split(self.dataset_options) if isinstance(self.dataset_options, str)
            else list(self.dataset_options)
        )

        download_dataset(
            self.context,
            pk,
            self.template['domain'],
            output_dir,
            options,
            self.clear_dataset
        )

    @classmethod
    def _extract_domain_type_from_contract(cls, contract: Predict) -> DockerImage:
        return DockerImage((contract.domain_type, contract.framework_type))
