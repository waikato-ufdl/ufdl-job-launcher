import configparser
import importlib
import os
from wai.lazypip import install_packages
from ufdl.pythonclient import UFDLServerContext
from ._node import hardware_info
from ._logging import logger
from ._utils import load_class
from ._node import get_ipv4
from ._sleep import SleepSchedule
from ufdl.joblauncher.poll import simple_poll
from ufdl.pythonclient.functional.core.jobs.job_template import retrieve as jobtemplate_retrieve
from ufdl.pythonclient.functional.core.jobs.job import list as job_list
from ufdl.pythonclient.functional.core.jobs.job import reset_job
import ufdl.pythonclient.functional.core.nodes.node as node
from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import Exact
from ufdl.json.core.filter.logical import And
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


def load_executor_class(class_name, required_packages, no_cache=True, debug=False):
    """
    Loads the executor class and returns it. Will install any required packages beforehand.
    Will fail with an exception if class cannot be loaded.

    :param class_name: the executor class to load
    :type class_name: str
    :param required_packages: the required packages to install (in pip format, get split on space), ignored if None or empty string
    :type required_packages: str
    :param no_cache: whether to turn of pip's cache
    :type no_cache: bool
    :param debug: whether to output debugging information
    :type debug: bool
    :return: the class object
    :rtype: class
    """
    if debug:
        logger().debug("Loading executor: %s - required packages: %s" % (class_name, required_packages))

    if required_packages is not None and (required_packages == ""):
        required_packages = None
    if required_packages is not None:
        pip_args = ["--upgrade"]
        if no_cache:
            pip_args.append("--no-cache-dir")
        install_packages(required_packages.split(" "), pip_args=pip_args)

    return load_class(class_name, debug=debug)


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

    cls = load_executor_class(
        template["executor_class"], template["required_packages"],
        no_cache=config['general']['pip_no_cache'] == 'true', debug=debug)
    executor = cls(context, config)
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
    driver = None
    generation = None
    gpu_mem = None
    cpu_mem = None
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

        # any jobs currently still open? -> reset them
        f = FilterSpec(
            expressions=[
                    Exact(field="node", value=pk),
            ]
        )
        jobs = job_list(context, filter_spec=f)
        if len(jobs) > 0:
            logger().info("Found #%d jobs still registered for node, will reset." % len(jobs))
            for j in jobs:
                try:
                    reset_job(context, j['pk'])
                except:
                    logger().error("Failed to reset job #%d!" % j['pk'], exc_info=1)

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
        try:
            os.makedirs(path, exist_ok=True)
        except:
            logger().fatal("Failed to create %s ('%s')!" % (desc, path))
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
    create_dir(config['docker']['work_dir'], "work directory")
    create_dir(config['docker']['cache_dir'], "cache directory")

    context = create_server_context(config, debug=debug)
    info = hardware_info(context)
    if debug:
        logger().debug("hardware info: %s" % str(info))
    poll = config['general']['poll']
    backenderror_wait = [10]
    if 'poll_backenderror_wait' in config['general']:
        backenderror_wait = config['general']['poll_backenderror_wait']
    sleep = SleepSchedule(backenderror_wait, debug=debug, debug_msg="Waiting %s before contacting backend again.")
    if debug:
        logger().debug("poll method: %s" % poll)

    # register node with backend
    while True:
        if not register_node(context, config, info, debug=debug):
            sleep.sleep()
            sleep.next()
        else:
            sleep.reset()
            break

    while True:
        try:
            job = None
            if poll == "simple":
                job = simple_poll(context, config, info, debug=debug)
            else:
                logger().fatal("Unknown poll method: %s" % poll)
                exit(1)
            if job is not None:
                execute_job(context, config, job, debug=debug)
                sleep.reset()
        except KeyboardInterrupt:
            logger().error("Polling/execution interrupted!", exc_info=1)
            break
        except:
            logger().error("Failed to poll/execute job!", exc_info=1)
            sleep.sleep()
            sleep.next()

        # continue polling?
        if not continuous:
            break
