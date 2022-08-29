from abc import ABC

from ufdl.jobcontracts.standard import Train

from ufdl.jobtypes.standard.server import DockerImage

from ._AbstractDockerJobExecutor import AbstractDockerJobExecutor


class AbstractTrainJobExecutor(AbstractDockerJobExecutor[Train], ABC):
    @classmethod
    def _extract_domain_type_from_contract(cls, contract: Train) -> DockerImage:
        return DockerImage((contract.domain_type, contract.framework_type))
