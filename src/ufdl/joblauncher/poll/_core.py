from ufdl.json.core.filter import FilterSpec, OrderBy
from ufdl.json.core.filter.field import Exact, Contains, IsNull, Compare
from ufdl.json.core.filter.logical import Or, And
from .._logging import logger


def generate_filter(hardware_info, debug=False):
    """
    Generates a filter for retrieving jobs.

    :param hardware_info: the hardware info to use
    :type hardware_info: dict
    :param debug: whether to output debugging information
    :type debug: bool
    :return: the filter
    :rtype: FilterSpec
    """

    if "gpus" in hardware_info:
        result = FilterSpec(
            expressions=[
                Or(
                    sub_expressions=[
                        And(
                            sub_expressions=[
                                IsNull(field="start_time"),
                                Compare(field="docker_image.cuda_version.version", operator="<=", value=hardware_info["cuda"]),
                                Compare(field="docker_image.cuda_version.min_driver_version", operator="<=", value=hardware_info["driver"]),
                                Compare(field="docker_image.min_hardware_generation.min_compute_capability", operator="<=", value=hardware_info["gpus"][0]["compute"]),
                                IsNull(field="node", invert=True),
                        ]),
                        And(
                            sub_expressions=[
                                IsNull(field="start_time"),
                                Exact(field="docker_image.cpu", value=True),
                                IsNull(field="node", invert=True),
                        ])
                    ])
            ])
    else:
        result = FilterSpec(
            expressions=[
                And(
                    sub_expressions=[
                        IsNull(field="start_time"),
                        Exact(field="docker_image.cpu", value=True),
                        IsNull(field="node", invert=True),
                ])
            ],
        )

    if debug:
        logger().debug("Filter:\n%s" % result.to_json_string(indent=2))

    return result
