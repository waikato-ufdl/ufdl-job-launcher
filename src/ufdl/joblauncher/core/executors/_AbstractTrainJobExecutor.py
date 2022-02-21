from typing import Union, Tuple

from ufdl.jobcontracts.standard import Train

from ufdl.jobtypes.base import String
from ufdl.jobtypes.standard.container import Array
from ufdl.jobtypes.standard.server import DockerImage

from .descriptors import Parameter
from ._AbstractDockerJobExecutor import AbstractDockerJobExecutor


class AbstractTrainJobExecutor(AbstractDockerJobExecutor[Train]):

    @classmethod
    def _extract_domain_type_from_contract(cls, contract: Train) -> DockerImage:
        return DockerImage((contract.domain_type, contract.framework_type))
