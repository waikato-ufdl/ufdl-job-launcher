import importlib
from ._logging import logger


def load_class(class_name, debug=False):
    """
    Loads the class and returns it.
    Will fail with an exception if class cannot be loaded.

    :param class_name: the executor class to load
    :type class_name: str
    :param debug: whether to output debugging information
    :type debug: bool
    :return: the class object
    :rtype: class
    """
    if debug:
        logger().debug("Instantiating class: %s" % class_name)

    module_name = ".".join(class_name.split(".")[0:-1])
    cls_name = class_name.split(".")[-1]

    module = importlib.import_module(module_name)
    cls = getattr(module, cls_name)
    return cls
