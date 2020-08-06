import importlib
from wai.lazypip import require_class


def load_executor_class(class_name, required_packages):
    """
    Loads the executor class and returns it. Will install any required packages beforehand.
    Will fail with an exception if class cannot be loaded.

    :param class_name: the executor class to load
    :type class_name: str
    :param required_packages: the required packages to install, ignored if None or empty string
    :type required_packages: str
    :return: the class object
    :rtype: class
    """

    module_name = ".".join(class_name.split(".")[0:-1])
    cls_name = class_name.split(".")[-1]

    if required_packages is not None and (required_packages == ""):
        required_packages = None
    if required_packages is not None:
        require_class(module_name, class_name, packages=required_packages)

    module = importlib.import_module(module_name)
    cls = getattr(module, cls_name)
    return cls