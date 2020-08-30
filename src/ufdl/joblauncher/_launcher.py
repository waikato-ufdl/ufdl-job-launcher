import configparser
import importlib
import os
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
from requests.exceptions import HTTPError


def create_server_context(config, debug=False):
    """
    Creates the config from the configuration.

    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param debug: whether to output debugging information
    :type debug: bool
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
    """
    if debug:
        logger().debug("Job: %s" % str(job))
    template = jobtemplate_retrieve(context, job['template']['pk'])

    cls = load_executor_class(template["executor_class"], template["required_packages"])
    executor = cls(
        context,
        config['docker']['work_dir'],
        config['docker']['cache_dir'],
        use_sudo=(config['docker']['use_sudo'] == "true"),
        ask_sudo_pw=(config['docker']['ask_sudo_pw'] == "true"),
        use_current_user=(config['docker']['use_current_user'] == "true")
    )
    executor.debug = (config['general']['debug'] == "true")
    executor.compression = int(config['general']['compression'])
    executor.run(template, job)


def register_node(context, config, info, debug=False):
    """
    Registers the node with the backend.

    :param context: the UFDL server context
    :type context: UFDLServerContext
    :param config: the configuration to use
    :type config: configparser.ConfigParser
    :param info: the hardware information, see hardware_info method
    :type info: dict
    :param debug: whether to output debugging information
    :type debug: bool
    :return: whether succeeded
    :rtype: bool
    """
    ip = get_ipv4()
    gpu_id = int(config['general']['gpu_id'])
    driver = Absent
    generation = Absent
    gpu_mem = Absent
    cpu_mem = Absent
    if 'memory' in info:
        cpu_mem = int(info['memory']['total'])
    if 'driver' in info:
        driver = info['driver']
    if ('gpus' in info) and (gpu_id in info['gpus']):
        if 'generation' in info['gpus'][gpu_id]:
            generation = int(info['gpus'][gpu_id]['generation']['pk'])
        if 'generation' in info['gpus'][gpu_id]:
            gpu_mem = int(info['gpus'][gpu_id]['memory']['total'])

    try:
        f = FilterSpec(
            expressions=[
                    And(
                        sub_expressions=[
                            Exact(field="ip", value=ip),
                            Exact(field="index", value=gpu_id)
                        ]
                    ),
            ]
        )
        if debug:
            logger().debug("Node filter:\n%s" % str(f.to_json_string(indent=2)))
        logger().info("Listing nodes %s/%d" % (ip, gpu_id))
        nodes = node.list(context, filter_spec=f)

        # already stored?
        if len(nodes) > 0:
            logger().info("Partially updating node %s/%d" % (ip, gpu_id))
            pk = int(nodes[0]['pk'])
            context.set_node_id(pk)
            node.partial_update(context, pk, ip=ip, index=gpu_id, driver_version=driver, hardware_generation=generation, gpu_mem=gpu_mem, cpu_mem=cpu_mem)
        else:
            logger().info("Creating node %s/%d" % (ip, gpu_id))
            obj = node.create(context, ip=ip, index=gpu_id, driver_version=driver, hardware_generation=generation, gpu_mem=gpu_mem, cpu_mem=cpu_mem)
            pk = int(obj['pk'])
            context.set_node_id(pk)

        # store pk in context
        logger().info("Node PK %d" % pk)
        return True
    except HTTPError as e:
        logger().error("Failed to register node!\n%s" % str(e.response.text), exc_info=1)
        return False
    except:
        logger().error("Failed to register node!", exc_info=1)
        return False


def create_dir(path, desc):
    """
    Creates the directory, if not present. Performs an exit call if fails to create.

    :param path: the path to check/create
    :type path: str
    :param desc: the description of the path
    :type desc: str
    """
    if not os.path.exists(path):
        logger().warning("%s ('%s') does not exist, creating..." % (desc, path))
        if not os.path.exists(path):
            exit(1)


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
    create_dir(config['docker']['work_dir'], "Work directory")
    create_dir(config['docker']['cache_dir'], "Cache directory")

    context = create_server_context(config, debug=debug)
    info = hardware_info(context)
    if debug:
        logger().debug("hardware info: %s" % str(info))
    poll = config['general']['poll']
    if debug:
        logger().debug("poll method: %s" % poll)

    # register node with backend
    if not register_node(context, config, info, debug=debug):
        return

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
