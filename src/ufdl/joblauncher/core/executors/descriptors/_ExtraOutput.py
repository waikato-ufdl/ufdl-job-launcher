from typing import Any, Generic, Optional, Type, TYPE_CHECKING

from ufdl.jobtypes.base import UFDLType, OutputType

if TYPE_CHECKING:
    from .._AbstractJobExecutor import AbstractJobExecutor


class ExtraOutput(Generic[OutputType]):
    """
    A descriptor for AbstractJobExecutor which declares an output additional
    to the contractual outputs, of the given type.
    """
    def __init__(
            self,
            type: UFDLType[tuple, Any, OutputType]
    ):
        self._name: Optional[str] = None
        self._type = type

    @property
    def name(self) -> str:
        # Can't get name until we're bound
        if self._name is None:
            raise Exception(f"{ExtraOutput.__qualname__} not bound")
        return self._name

    @property
    def type(self) -> UFDLType[tuple, Any, OutputType]:
        return self._type

    def __set__(self, instance: 'AbstractJobExecutor', value: OutputType):
        from .._AbstractJobExecutor import AbstractJobExecutor
        assert isinstance(instance, AbstractJobExecutor)
        instance._add_output_to_job(self.name, self.type, value)

    def __set_name__(self, owner: Type['AbstractJobExecutor'], name: str):
        # Only designed to be used with AbstractJobExecutors
        from .._AbstractJobExecutor import AbstractJobExecutor
        if not issubclass(owner, AbstractJobExecutor):
            raise Exception(f"Owner is not an {AbstractJobExecutor.__qualname__}")

        # Can't reassign this descriptor
        if self._name is not None:
            raise Exception(f"{ExtraOutput.__qualname__} descriptor already assigned")

        self._name = name
