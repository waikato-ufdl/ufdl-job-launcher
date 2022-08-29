import importlib
from typing import List, Optional, TypeVar, Type

from wai.lazypip import require_module, install_packages

from ._logging import logger

ClassType = TypeVar('ClassType')


def load_class(
        class_name: str,
        required_type: Type[ClassType] = object,
        debug: bool = False,
        required_packages: Optional[List[str]] = None,
        no_cache: bool = False,
        upgrade: bool = False
) -> Type[ClassType]:
    """
    Loads the class and returns it.
    Will fail with an exception if class cannot be loaded.

    :param class_name: the executor class to load
    :param required_type: the required type of the loaded class (raises if it is not this type)
    :param debug: whether to output debugging information
    :param required_packages: optional list of packages that must be installed to find the class
    :param no_cache: if pip's cache should be disabled
    :param upgrade: forces upgrading of required packages (no-op if no packages required)
    :return: the class object
    """
    if debug:
        logger().debug(f"Instantiating class: {class_name}")

    module_name, cls_name = class_name.rsplit(".", 1)

    pip_args = []
    if no_cache:
        pip_args.append("--no-cache-dir")

    if not upgrade or required_packages is None:
        require_module(module_name, pip_args=pip_args)
    else:
        install_packages(required_packages, pip_args + ["--upgrade"])

    module = importlib.import_module(module_name)
    importlib.reload(module)
    cls = getattr(module, cls_name)

    if not isinstance(cls, type) or not issubclass(cls, required_type):
        raise Exception(f"'{class_name}' is not a sub-class of {required_type.__qualname__}")

    return cls
