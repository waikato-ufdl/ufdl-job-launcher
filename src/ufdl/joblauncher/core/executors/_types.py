from typing import TypeVar

from ufdl.jobcontracts.base import UFDLJobContract


ContractType = TypeVar('ContractType', bound=UFDLJobContract)
