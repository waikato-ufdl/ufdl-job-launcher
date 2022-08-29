from typing import Any, Generic, Optional, Tuple, Type, TypeVar, Union, overload, TYPE_CHECKING
from weakref import WeakKeyDictionary

from ufdl.jobtypes.base import UFDLJSONType
from ufdl.jobtypes.util import parse_type

from wai.json.raw import RawJSONElement

if TYPE_CHECKING:
    from .._AbstractJobExecutor import AbstractJobExecutor


ParameterType = TypeVar('ParameterType')


class RequiredParameter:
    pass


class Parameter(Generic[ParameterType]):
    """
    A descriptor for AbstractJobExecutor which declares a parameter
    of the given type.
    """
    def __init__(
            self,
            *types: UFDLJSONType[tuple, ParameterType, Any],
            default: Union[ParameterType, Type[RequiredParameter]] = RequiredParameter
    ):
        # Must supply at least one type-handler
        assert len(types) > 0, "No types"
        assert all(isinstance(param_type, UFDLJSONType) for param_type in types), "All types must be JSON-compatible"

        self._name: Optional[str] = None
        self._types = types
        self._default = default
        self._cache: WeakKeyDictionary = WeakKeyDictionary()

    @property
    def name(self) -> str:
        # Can't get a value until we're bound
        if self._name is None:
            raise Exception(f"{Parameter.__qualname__} not bound")
        return self._name

    @property
    def types(self) -> Tuple[UFDLJSONType[tuple, ParameterType, Any], ...]:
        return self._types

    @property
    def default(self) -> Union[ParameterType, Type[RequiredParameter]]:
        return self._default

    @overload
    def __get__(self, instance: None, owner: Type['AbstractJobExecutor']) -> 'Parameter[ParameterType]': ...
    @overload
    def __get__(self, instance: 'AbstractJobExecutor', owner: Type['AbstractJobExecutor']) -> ParameterType: ...

    def __get__(self, instance: Optional['AbstractJobExecutor'], owner: Type['AbstractJobExecutor']) -> Union[ParameterType, 'Parameter[ParameterType]']:
        # If called from the class, return the descriptor itself
        if instance is None:
            return self

        from .._AbstractJobExecutor import AbstractJobExecutor
        assert isinstance(instance, AbstractJobExecutor)

        return self.parse_parameter(
            self.name,
            self._types,
            instance,
            self._default,
            self._cache
        )

    @staticmethod
    def _parse_json_value(
            name: str,
            types: Tuple[UFDLJSONType[tuple, ParameterType, Any], ...],
            value: RawJSONElement,
            passed_type: str
    ) -> ParameterType:
        # Parse the type of the value
        parsed_type = parse_type(passed_type)

        # Ensure the passed type is a sub-type of an allowed type
        if not any(parsed_type.is_subtype_of(allowed_type) for allowed_type in types):
            raise Exception(
                f"{passed_type} is not a valid type for parameter '{name}'; "
                f"Must be a sub-type of one of the following: {', '.join(map(str, types))}"
            )

        # Assert the parsed type is a JSON type
        assert isinstance(parsed_type, UFDLJSONType)

        # Use the type to parse the JSON value
        return parsed_type.parse_json_value(value)

    @staticmethod
    def parse_parameter(
            name: str,
            types: Tuple[UFDLJSONType[tuple, ParameterType, Any], ...],
            instance: 'AbstractJobExecutor',
            default: Union[ParameterType, Type[RequiredParameter]] = RequiredParameter,
            cache: Optional[WeakKeyDictionary] = None
    ) -> ParameterType:
        # Return the cached result if any
        if cache is not None and instance in cache:
            return cache[instance]

        # Try get the value from the job
        if 'parameter_values' in instance.job and name in instance.job['parameter_values']:
            value_and_type = instance.job['parameter_values'][name]
            value = Parameter._parse_json_value(name, types, value_and_type['value'], value_and_type['type'])
        elif name in instance.template['parameters'] and 'default' in instance.template['parameters'][name]:
            parameter_spec = instance.template['parameters'][name]
            value = Parameter._parse_json_value(name, types, parameter_spec['default'], parameter_spec['default_type'])
        else:
            value = default

        # If the value is required but not given, except
        if value is RequiredParameter:
            raise Exception(f"No value passed for required parameter '{name}'")

        # Cache the value
        if cache is not None:
            cache[instance] = value

        return value

    def __set_name__(self, owner: Type['AbstractJobExecutor'], name: str):
        # Only designed to be used with AbstractJobExecutors
        from .._AbstractJobExecutor import AbstractJobExecutor
        if not issubclass(owner, AbstractJobExecutor):
            raise Exception(f"Owner is not an {AbstractJobExecutor.__qualname__}")

        # Can't reassign this descriptor
        if self._name is not None:
            raise Exception(f"{Parameter.__qualname__} descriptor already assigned")

        self._name = name
