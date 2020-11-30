from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import IsNull
from ufdl.json.core.filter.logical import And
from .._logging import logger


def generate_filter(debug=False):
    """
    Generates a filter for retrieving jobs.

    :param debug: whether to output debugging information
    :type debug: bool
    :return: the filter
    :rtype: FilterSpec
    """
    result = FilterSpec(
        expressions=[
            And(
                sub_expressions=[
                    IsNull(field="start_time"),
                    IsNull(field="node"),
                    ~IsNull(field="template.workabletemplate")
            ])
        ],
    )

    if debug:
        logger().debug("Filter:\n%s" % result.to_json_string(indent=2))

    return result
