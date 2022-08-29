"""
Utilities for converting raw string values into more useful types.
"""
from typing import Callable, Iterable, List, Optional, TypeVar

DEFAULT_TRUE_SET = frozenset((
    '1', 'yes', 'true', 'on'
))
"""Default set of string values which should treated as boolean True."""

DEFAULT_FALSE_SET = frozenset((
    '0', 'no', 'false', 'off'
))
"""Default set of string values which should treated as boolean False."""


def str2bool(
        string: str,
        true_values: Optional[Iterable[str]] = None,
        false_values: Optional[Iterable[str]] = None
) -> bool:
    """
    Converts a raw string value into a boolean value. Does so by comparing
    the value with two sets of allowed keywords, one for True values and one
    for False. Comparison is done normalised (lower-case and stripped whitespace).

    :param string:
                The raw string value to convert.
    :param true_values:
                A set of values to consider boolean True. None for the default set.
    :param false_values:
                A set of values to consider boolean False. None for the default set.
    :return:
                The converted value.
    :raises ValueError:
                If the normalised string is not in either the true- or false-values.
    """
    # Normalise the given true values into a set, or default
    true_set = (
        frozenset(map(normalise, true_values)) if true_values is not None
        else DEFAULT_TRUE_SET
    )

    # Normalise the given false values into a set, or default
    false_set = (
        frozenset(map(normalise, false_values)) if false_values is not None
        else DEFAULT_FALSE_SET
    )

    # Normalise the value to convert
    normalised_string = normalise(string)

    # Find which boolean set the value is in, or raise if in neither
    if normalised_string in true_set:
        return True
    elif normalised_string in false_set:
        return False
    else:
        raise ValueError(
            f"String '{normalised_string}' (normalised from '{string}') not found in either bool-set\n"
            f"True-set: {true_set}\n"
            f"False-set: {false_set}"
        )


def normalise(string: str) -> str:
    """
    Normalises a string for case-insensitive, stripped comparison.

    :param string:
                The string to normalise.
    :return:
                The normalised string.
    """
    return string.lower().strip()


ElementType = TypeVar('ElementType')
"""The type of elements in a list."""


def list_of(
        convert: Callable[[str], ElementType],
        sep: Optional[str] = None
) -> Callable[[str], List[ElementType]]:
    """
    Creates a convert function for lists of values.

    :param convert:
                The function to use to convert each element of the list.
    :param sep:
                The separator of sub-strings in the list.
    :return:
                A function which takes a [sep]-separated string of strings,
                and converts it into a list of converted elements.
    """
    def convert_list(
            string: str
    ) -> List[ElementType]:
        return [
            convert(element)
            for element in string.split(sep)
        ]

    return convert_list


ValueType = TypeVar('ValueType')
"""The type of allowed values in an enumerated set."""


def enum_of(
        convert: Callable[[str], ValueType],
        *allowed_values: ValueType
) -> Callable[[str], ValueType]:
    """
    Creates a convert function which limits the allowed values to an
    enumerated set.
    
    :param convert:
                Function to convert a raw string into the type of the enumeration.
    :param allowed_values:
                Set of allowed values for the converted value.
    :return:
                Function which converts the raw value and checks it is in the enumeration.
    """
    def convert_enum(
            string: str
    ) -> ValueType:
        # Perform the base-line conversion
        converted = convert(string)

        # Ensure the converted value is in the enumerated set
        if converted not in allowed_values:
            raise ValueError(
                f"'{converted}' (parsed from '{string}') is not one of:\n"
                f"{allowed_values}"
            )

        return converted

    return convert_enum
