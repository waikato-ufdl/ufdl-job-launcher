"""
Descriptor classes for declarative-style handling of inputs/parameters.
"""
from typing import Dict, Any, Callable, Tuple, Iterator, Type, Union
from weakref import WeakKeyDictionary

from ufdl.pythonclient.functional.core.jobs import job, job_output

# The type of function that handles parsing of a type value-string
# (and options) for an input
InputTypeHandler = Callable[['AbstractJobExecutor', str, str], Any]


class JobOutput:
    """
    Input type handler which helps with job_output<?> types.
    """
    def __init__(self, type: str):
        # Make sure the type is a job_output<?> type
        if not type.startswith("job_output<") or not type.endswith(">"):
            raise Exception(f"{JobOutput.__qualname__} only usable with job_output<?> types")

        self._type: str = type[11:-1]
        self._serialised = None

    def __call__(self, owner: 'AbstractJobExecutor', pk_string: str, options: str):
        self._owner = owner
        self._pk: int = int(pk_string)
        self._options: str = options
        return self

    @property
    def name(self) -> str:
        """
        The name of the job-output that supplied this value.
        """

        return self.serialised['name']

    @property
    def type(self) -> str:
        """
        The type of the job output.
        """
        return self._type

    @property
    def pk(self) -> int:
        """
        The primary key of the job-output that supplied this value.
        """
        return self._pk

    @property
    def job_pk(self) -> int:
        """
        The primary key of the job owning this output.
        """
        return int(self.serialised['job'])

    @property
    def options(self) -> str:
        """
        The options of the input that sourced this value.
        """
        return self._options

    def download(self) -> Iterator[bytes]:
        """
        Downloads the contents of the job-output.

        :return:
                    An iterator of byte-chunks of the output as it downloads.
        """
        return job.get_output(self._owner.context, self.job_pk, self.name, self._type)

    @property
    def serialised(self):
        self._ensure_serialised()
        return self._serialised

    def _ensure_serialised(self):
        """
        Ensures the serialised description of the job output has been retrieved.
        """
        # Retrieve and cache the serialised job-output description
        if self._serialised is None:
            self._serialised = job_output.retrieve(self._owner.context, self._pk)


class Input:
    """
    A descriptor for AbstractJobExecutor which declares an input
    which can handle some given types.
    """
    def __init__(
            self,
            type_handlers: Dict[str, Union[InputTypeHandler, Type[JobOutput]]]
    ):
        # Must supply at least one type-handler
        if len(type_handlers) == 0:
            raise Exception("No type-handlers")

        self._name: str = None
        self._type_handlers: Dict[str, InputTypeHandler] = {
            type_string:
                handler(type_string)
                if handler is JobOutput else
                handler
            for type_string, handler in type_handlers.items()
        }
        self._cache = WeakKeyDictionary()

    def __get__(self, instance, owner: type):
        # If called from the class, return the descriptor itself
        if instance is None:
            return self

        # Can't get a value until we're bound
        if self._name is None:
            raise Exception(f"{Input.__qualname__} not bound")

        # Return the cached result if any
        if instance in self._cache:
            return self._cache[instance]

        # Get the input value description from the instance
        input = instance._input(self._name)

        # Get the handler for this type
        type_handler = self._type_handlers[input['type']]

        # Process and cache the value
        value = type_handler(instance, input['value'], input['options'])
        self._cache[instance] = value

        return value

    def __set_name__(self, owner: type, name: str):
        # Local import to avoid circular dependency
        from ._executors import AbstractJobExecutor

        # Only designed to be used with AbstractJobExecutors
        if not issubclass(owner, AbstractJobExecutor):
            raise Exception(f"Owner is not an {AbstractJobExecutor.__qualname__}")

        # Can't reassign this descriptor
        if self._name is not None:
            raise Exception(f"{Input.__qualname__} descriptor already assigned")

        self._name = name


# The type of function that handles parsing of a type value-string
# for a parameter
ParameterTypeHandler = Callable[['AbstractJobExecutor', str], Any]


class Parameter:
    """
    A descriptor for AbstractJobExecutor which declares a parameter
    of the given type.
    """
    def __init__(self, type_handlers: Dict[str, ParameterTypeHandler]):
        # Must supply at least one type-handler
        if len(type_handlers) == 0:
            raise Exception("No type-handlers")

        self._name: str = None
        self._type_handlers: Dict[str, ParameterTypeHandler] = type_handlers.copy()
        self._allowed_types: Tuple[str] = tuple(type_handlers.keys())
        self._cache = WeakKeyDictionary()

    def __get__(self, instance, owner: type):
        # If called from the class, return the descriptor itself
        if instance is None:
            return self

        # Can't get a value until we're bound
        if self._name is None:
            raise Exception(f"{Parameter.__qualname__} not bound")

        # Return the cached result if any
        if instance in self._cache:
            return self._cache[instance]

        # Get the input value description from the instance
        parameter = instance._parameter(self._name, allowed_types=self._allowed_types)

        # Get the handler for this type
        type_handler = self._type_handlers[parameter['type']]

        # Process and cache the value
        value = type_handler(instance, parameter['value'])
        self._cache[instance] = value

        return value

    def __set_name__(self, owner: type, name: str):
        # Local import to avoid circular dependency
        from ._executors import AbstractJobExecutor

        # Only designed to be used with AbstractJobExecutors
        if not issubclass(owner, AbstractJobExecutor):
            raise Exception(f"Owner is not an {AbstractJobExecutor.__qualname__}")

        # Can't reassign this descriptor
        if self._name is not None:
            raise Exception(f"{Parameter.__qualname__} descriptor already assigned")

        self._name = name
