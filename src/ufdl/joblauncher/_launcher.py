import configparser
import importlib
from wai.lazypip import require_class
from ufdl.pythonclient import UFDLServerContext
from ._node import hardware_info
from ._logging import logger
from ._node import get_ipv4
from ufdl.joblauncher.poll import simple_poll, rabbitmq_poll
from ufdl.pythonclient.functional.core.jobs.job_template import retrieve as jobtemplate_retrieve
import ufdl.pythonclient.functional.core.nodes.node as node
from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import Exact
from ufdl.json.core.filter.logical import And
from wai.json.object import Absent


def create_server_context(config, debug=False):
    """
    Creates the config from the configuration.

    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :return: the server context
    :rtype: UFDLServerContext
    """
    if debug:
        logger().debug("Connecting to backend: %s@%s" % (config['backend']['user'], config['backend']['url']))

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
        config['docker']['cache_dir'],
        use_sudo=config['docker']['use_sudo'],
        ask_sudo_pw=config['docker']['ask_sudo_pw'],
        use_current_user=bool(config['docker']['use_current_user'])
    )
    executor.debug = bool(config['general']['debug'])
    executor.compression = int(config['general']['compression'])
    executor.run(template, job)


def register_node(context, config, info):
    """
    Registers the node with the backend.

    :param context: the UFDL server context
    :type context: UFDLServerContext
    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param info: the hardware information, see hardware_info method
    :type info: dict
    """
    ip = get_ipv4()
    node_id = int(config['general']['node_id'])
    context.set_node_id(node_id)
    driver = Absent
    generation = Absent
    gpu_mem = Absent
    cpu_mem = Absent
    if 'memory' in info:
        cpu_mem = int(info['memory']['total'])
    if 'driver' in info:
        driver = info['driver']
    if ('gpus' in info) and (node_id in info['gpus']):
        if 'generation' in info['gpus'][node_id]:
            generation = int(info['gpus'][node_id]['generation']['pk'])
        if 'generation' in info['gpus'][node_id]:
            gpu_mem = int(info['gpus'][node_id]['memory']['total'])

    try:
        f = FilterSpec(
            expressions=[
                    And(
                        sub_expressions=[
                            Exact(field="ip", value=ip),
                            Exact(field="index", value=node_id)
                        ]
                    ),
            ],
            include_inactive=False
        )
        nodes = node.list(context, filter_spec=f)

        # already stored?
        if len(nodes) > 0:
            pk = int(nodes[0]['pk'])
            node.partial_update(context, pk, ip=ip, index=node_id, driver_version=driver, hardware_generation=generation, gpu_mem=gpu_mem, cpu_mem=cpu_mem)
        else:
            obj = node.create(context, ip=ip, index=node_id, driver_version=driver, hardware_generation=generation, gpu_mem=gpu_mem, cpu_mem=cpu_mem)
            pk = int(obj['pk'])

        # store pk of node for deregistering
        config['general']['node_pk'] = pk
    except:
        logger().error("Failed to register node!", exc_info=1)


def deregister_node(context, config):
    """
    Deregisters the node with the backend.

    :param context: the UFDL server context
    :type context: UFDLServerContext
    :param config: the configuration to use
    :type config: configparser.ConfigParser
    """
    if 'node_pk' in config['general']:
        try:
            node.destroy(context, pk=int(config['general']['node_pk']))
        except:
            logger().error("Failed to deregister node!", exc_info=1)
    else:
        logger().warning("No node pk stored in config, cannot deregister!")


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
    context = create_server_context(config, debug=debug)
    info = hardware_info(context)
    if debug:
        logger().debug("hardware info: %s" % str(info))
    poll = config['general']['poll']
    if debug:
        logger().debug("poll method: %s" % poll)

    # register node with backend
    register_node(context, config, info)

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
            logger().error("Failed to poll/execute job!", exc_info=1)

        # continue polling?
        if not continuous:
            break

    # de-register node
    deregister_node(context, config)
