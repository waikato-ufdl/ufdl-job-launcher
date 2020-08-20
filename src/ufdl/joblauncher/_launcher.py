import configparser
import importlib
import traceback
from wai.lazypip import require_class
from ufdl.pythonclient import UFDLServerContext
from ._node import hardware_info
from ._logging import logger
from ufdl.joblauncher.poll import simple_poll, rabbitmq_poll
from ufdl.pythonclient.functional.core.jobs.job_template import retrieve as jobtemplate_retrieve


def create_server_context(config):
    """
    Creates the config from the configuration.

    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :return: the server context
    :rtype: UFDLServerContext
    """
    return UFDLServerContext(
        config['backend']['url'],
        config['backend']['user'],
        config['backend']['password'])


def load_executor_class(class_name, required_packages):
    """
    Loads the executor class and returns it. Will install any required packages beforehand.
    Will fail with an exception if class cannot be loaded.

    :param class_name: the executor class to load
    :type class_name: str
    :param required_packages: the required packages to install (in pip format, get split on space), ignored if None or empty string
    :type required_packages: str
    :return: the class object
    :rtype: class
    """
    module_name = ".".join(class_name.split(".")[0:-1])
    cls_name = class_name.split(".")[-1]

    if required_packages is not None and (required_packages == ""):
        required_packages = None
    if required_packages is not None:
        require_class(module_name, class_name, packages=required_packages.split(" "))

    module = importlib.import_module(module_name)
    cls = getattr(module, cls_name)
    return cls


def execute_job(context, config, job, debug=False):
    """
    Executes the given job.

    :param context: the UFDL server context
    :type context: UFDLServerContext
    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param job: the job to execute
    :type job: dict
    :param debug: whether to output debugging information
    :type debug: bool
    :return:
    """
    if debug:
        logger().debug("Job: %s" % str(job))
    template = jobtemplate_retrieve(context, job['template']['pk'])

    cls = load_executor_class(template["executor_class"], template["required_packages"])
    executor = cls(
        context,
        config['docker']['work_dir'],
        use_sudo=config['docker']['use_sudo'],
        ask_sudo_pw=config['docker']['ask_sudo_pw'],
        use_current_user=bool(config['docker']['use_current_user'])
    )
    executor.debug = bool(config['general']['debug'])
    executor.compression = int(config['general']['compression'])
    executor.run(template, job)


def launch_jobs(config, continuous, debug=False):
    """
    Launches the jobs.

    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param continuous: whether to poll continuously or only once
    :type continuous: bool
    :param debug: whether to output debugging information
    :type debug: bool
    """
    context = create_server_context(config)
    info = hardware_info(context)
    if debug:
        logger().debug("hardware info: %s" % str(info))
    poll = config['general']['poll']
    if debug:
        logger().debug("poll method: %s" % poll)

    # TODO register node with backend

    while True:
        try:
            job = None
            if poll == "simple":
                job = simple_poll(context, config, info, debug=debug)
            elif poll == "rabbitmq":
                job = rabbitmq_poll(context, config, info, debug=debug)
            else:
                logger().fatal("Unknown poll method: %s" % poll)
                exit(1)
            if job is not None:
                execute_job(context, config, job, debug=debug)
        except:
            logger().error(traceback.format_exc())

        # continue polling?
        if not continuous:
            break

    # TODO de-register node