from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import IsNull
from .._logging import logger


def generate_filter(debug: bool = False) -> FilterSpec:
    """
    Generates a filter for retrieving jobs.

    :param debug: whether to output debugging information
    :return: the filter
    :rtype: FilterSpec
    """
    result = FilterSpec(
        expressions=[
            IsNull(field="start_time") &
            IsNull(field="error_reason") &
            IsNull(field="node") &
            ~IsNull(field="template.workabletemplate")
        ]
    )

    if debug:
        logger().debug("Filter:\n%s" % result.to_json_string(indent=2))

    return result
