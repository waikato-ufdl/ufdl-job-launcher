import configparser
from typing import Any, Dict, Optional

from ._ConfigProperty import ConfigProperty


class ConfigSection:
    """
    Base class for sections of the job-launcher's configuration file.
    """
    def __init__(
            self,
            name: str,
            raw: configparser.ConfigParser,
            source_filename: Optional[str]
    ):
        """
        :param name:
                    The section's name.
        :param raw:
                    The raw parsed config-file.
        :param source_filename:
                    The filename of the parsed config-file (for error messages). None to exclude.
        """
        self._name = name
        self._values = self._check(name, raw, source_filename)

    def __str__(self) -> str:
        return self._name

    def __getitem__(self, item: str) -> Any:
        return self._values[item]

    @classmethod
    def _check(
            cls,
            section_name: str,
            raw: configparser.ConfigParser,
            source_filename: Optional[str]
    ) -> Dict[str, Any]:
        """
        Checks a section of the raw parsed config-file for correctness, and parses the raw values
        into their converted data-types.

        :param section_name:
                    The name of the section to check.
        :param raw:
                    The raw parsed config-file.
        :param source_filename:
                    The filename of the parsed config-file (for error messages). None to exclude.
        :return:
                    A dictionary of property names to converted/defaulted property values.
        """
        # Format an error header based on whether the source filename was given
        header = (
            f"Error in config file '{source_filename}'" if source_filename is not None
            else "Error"
        )

        # Check the section is in the raw config-file
        if section_name not in raw.sections():
            raise Exception(f"{header}: missing section '{section_name}'")

        section_values = raw[section_name]
        converted_values: Dict[str, Any] = {}

        # Locate and convert each of our properties
        for property_name, property in cls._config_properties().items():
            # Get the raw value from the section, if it is defined
            raw_property_value: Optional[str] = section_values.get(property_name)

            # Attempt to convert it
            try:
                converted_values[property_name] = property.convert(raw_property_value)
            except Exception as e:
                raise ValueError(
                    f"Error converting value '{raw_property_value}' "
                    f"for property '{property_name}' "
                    f"of section '{section_name}:\n"
                    f"{e}"
                ) from e

        return converted_values

    @classmethod
    def _config_properties(cls) -> Dict[str, ConfigProperty]:
        """
        Gets all the properties of this section.
        """
        return {
            attr_name: attr
            for attr_name in dir(cls)
            for attr in (getattr(cls, attr_name),)
            if isinstance(attr, ConfigProperty)
        }
