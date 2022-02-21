from typing import Union, Tuple

from ufdl.jobcontracts.standard import Predict

from ufdl.jobtypes.base import String, Boolean
from ufdl.jobtypes.standard.container import Array
from ufdl.jobtypes.standard.server import DockerImage

from .descriptors import Parameter
from ._AbstractDockerJobExecutor import AbstractDockerJobExecutor


class AbstractPredictJobExecutor(AbstractDockerJobExecutor[Predict]):

    clear_dataset: bool = Parameter(
        Boolean()
    )

    @classmethod
    def _extract_domain_type_from_contract(cls, contract: Predict) -> DockerImage:
        return DockerImage((contract.domain_type, contract.framework_type))
