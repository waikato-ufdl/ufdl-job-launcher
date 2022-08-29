from typing import Callable, Generic, Optional, TYPE_CHECKING, Type, TypeVar

if TYPE_CHECKING:
    from ._ConfigSection import ConfigSection


ValueType = TypeVar('ValueType')
"""
The type of value that the property converts its raw string input into.
"""


class ConfigProperty(Generic[ValueType]):
    """
    A single property of a section in a configuration file. Decorator-class which
    manages conversion from raw string values, property-optionality/defaults, and
    type-safe retrieval.
    """
    def __init__(
            self,
            convert: Callable[[str], ValueType],
            default: Optional[ValueType] = None
    ):
        """
        :param convert:
                    Converter from raw string values to the property's value-type.
        :param default:
                    A default value for optional properties.
        """
        self._name: Optional[str] = None
        self._owner: Optional[Type[ConfigSection]] = None
        self._convert = convert
        self._default = default

    def convert(self, value: Optional[str]) -> ValueType:
        """
        Converts the raw string value to the property's value-type, or provides
        the default if no value is given.

        :param value:
                    The raw string value to convert.
        :return:
                    The convert value.
        :raises Exception:
                    If no value is given and this is not an optional property.
        """
        # If a value is given, convert it
        if value is not None:
            return self._convert(value)

        # If this property is required, raise the fact that no value was given
        if self._default is None:
            raise Exception(f"No value specified for non-optional property '{self._name}'")

        return self._default

    def __get__(self, instance: 'ConfigSection', owner: Type['ConfigSection']) -> ValueType:
        """
        Gets the (converted) value of this property from the parsed section instance.

        :param instance:
                    The parsed configuration section.
        :param owner:
                    The configuration section class that owns this property.
        :return:
                    The converted or default value of this property for the instance.
        """
        # Ensure the owner hasn't been modified
        if owner is not self._owner:
            raise Exception(f"ConfigProperty owner changed, was {self._owner}, now {owner}")

        # Decorator access rules require that we return the decorator itself when instance is None
        if instance is None:
            return self

        # Make sure the instance is an instance of the property's owning class
        if not isinstance(instance, owner):
            raise Exception(f"Instance-type for ConfigProperty should be {owner} or None, got {type(instance)}")

        return instance[self._name]

    def __set_name__(self, owner: Type['ConfigSection'], name: str):
        """
        Saves the name/owning-class of this property, so that it can't be reused.

        :param owner:
                    The configuration section class that owns this property.
        :param name:
                    The name of this property.
        """
        # Make sure we haven't already been claimed
        if self._name is not None:
            raise Exception(f"ConfigProperty already registered under name '{self._name}'")

        self._name = name
        self._owner = owner
