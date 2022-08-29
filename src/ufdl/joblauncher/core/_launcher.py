import os
from typing import Optional, Type

from ufdl.pythonclient import UFDLServerContext
from ._logging import logger
from ._utils import load_class
from ._node import get_ipv4
from ._sleep import SleepSchedule
from .executors import AbstractJobExecutor
from .poll import Simple
from ufdl.pythonclient.functional.core.jobs.job_template import retrieve as jobtemplate_retrieve
from ufdl.pythonclient.functional.core.jobs.job import finish_job, reset_job, release_job
import ufdl.pythonclient.functional.core.nodes.node as node
from ufdl.json.core.filter import FilterSpec
from ufdl.json.core.filter.field import Exact
from ufdl.json.core.filter.logical import And
from requests.exceptions import HTTPError

from .config import UFDLJobLauncherConfig
from .types import Poller, Job, JobPrepper
from ._node import HardwareInfo, GPU


def create_server_context(
        config: UFDLJobLauncherConfig,
        debug: bool = False
) -> UFDLServerContext:
    """
    Creates the config from the configuration.

    :param config: the configuration to use
    :param debug: whether to output debugging information
    :return: the server context
    """
    if debug:
        logger().debug(f"Connecting to backend: {config.backend.user}@{config.backend.url}")

    return UFDLServerContext(
        config.backend.url,
        config.backend.user,
        config.backend.password
    )


def load_executor_class(
        class_name: str,
        required_packages: str,
        no_cache: bool = True,
        debug: bool = False
) -> Type[AbstractJobExecutor]:
    """
    Loads the executor class and returns it. Will install any required packages beforehand.
    Will fail with an exception if class cannot be loaded.

    :param class_name: the executor class to load
    :param required_packages: the required packages to install (in pip format, get split on space), ignored if None or empty string
    :param no_cache: whether to turn of pip's cache
    :param debug: whether to output debugging information
    :return: the class object
    """
    if debug:
        logger().debug("Loading executor: %s - required packages: %s" % (class_name, required_packages))

    if required_packages == "":
        required_packages = None

    return load_class(
        class_name,
        required_type=AbstractJobExecutor,
        debug=debug,
        no_cache=no_cache,
        required_packages=required_packages.split(" ") if required_packages is not None else None,
        upgrade=True
    )


def create_executor(
        context: UFDLServerContext,
        config: UFDLJobLauncherConfig,
        job: Job,
        debug: bool = False
) -> AbstractJobExecutor:
    """
    Executes the given job.

    :param context: the UFDL server context
    :param config: the configuration to use
    :param job: the job to execute
    :param debug: whether to output debugging information
    :return: the job executor
    """
    if debug:
        logger().debug(f"Job: {job}")
    template = jobtemplate_retrieve(context, job['template']['pk'])

    cls = load_executor_class(
        template["executor_class"],
        template["required_packages"],
        no_cache=config.general.pip_no_cache,
        debug=debug
    )
    return cls(context, config, template, job)


def register_node(
        context: UFDLServerContext,
        config: UFDLJobLauncherConfig,
        info: HardwareInfo,
        debug: bool = False
) -> bool:
    """
    Registers the node with the backend.

    :param context: the UFDL server context
    :param config: the configuration to use
    :param info: the hardware information, see hardware_info method
    :param debug: whether to output debugging information
    :return: whether succeeded
    """
    ip = get_ipv4()
    gpu_id = config.general.gpu_id
    driver: Optional[str] = info.driver if info.driver is not None else None
    gpu: Optional[GPU] = info.gpus.get(gpu_id, None) if info.gpus is not None else None
    generation: Optional[int] = gpu.generation.pk if gpu is not None and gpu.generation is not None else None
    gpu_mem: Optional[int] = gpu.memory.total if gpu is not None and gpu.memory is not None else None
    cpu_mem: Optional[int] = info.memory.total if info.memory is not None else None

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
            logger().debug(f"Node filter:\n{f.to_json_string(indent=2)}")
        logger().info(f"Listing nodes {ip}/{gpu_id}")
        nodes = node.list(context, filter_spec=f)

        # already stored?
        if len(nodes) > 0:
            logger().info("Partially updating node %s/%d" % (ip, gpu_id))
            pk = int(nodes[0]['pk'])
            context.set_node_id(pk)
            obj = node.partial_update(context, pk, ip=ip, index=gpu_id, driver_version=driver, hardware_generation=generation, gpu_mem=gpu_mem, cpu_mem=cpu_mem)
        else:
            logger().info("Creating node %s/%d" % (ip, gpu_id))
            obj = node.create(context, ip=ip, index=gpu_id, driver_version=driver, hardware_generation=generation, gpu_mem=gpu_mem, cpu_mem=cpu_mem)
            pk = int(obj['pk'])
            context.set_node_id(pk)

        # store pk in context
        logger().info("Node PK %d" % pk)

        # any jobs currently still open? -> finish/reset them
        current_job = obj['current_job']
        if current_job is not None:
            logger().info("Found job #%d still registered for node." % current_job)
            error = "Node restarted during job execution."
            result = None

            try:
                result = finish_job(context, current_job, False, "", error)
            except:
                logger().error("Failed to finalise job #%d!" % current_job, exc_info=1)
            else:
                logger().info("Job #%d successfully finalised." % current_job)

            if result is not None and result['error_reason'] == error:
                logger().info("Job #%d can be reset." % current_job)
                try:
                    reset_job(context, current_job)
                    release_job(context, current_job)
                except:
                    logger().error("Failed to reset job #%d!" % current_job, exc_info=1)
                else:
                    logger().info("Job #%d successfully reset." % current_job)

        return True
    except HTTPError as e:
        logger().error("Failed to register node!\n%s" % str(e.response.text), exc_info=1)
        return False
    except:
        logger().error("Failed to register node!", exc_info=1)
        return False


def create_dir(path: str, desc: str):
    """
    Creates the directory, if not present. Performs an exit call if fails to create.

    :param path: the path to check/create
    :param desc: the description of the path
    """
    if not os.path.exists(path):
        logger().warning(f"{desc} ('{path}') does not exist, creating...")
        try:
            os.makedirs(path, exist_ok=True)
        except Exception as e:
            logger().fatal(f"Failed to create {desc} ('{path}')!", exc_info=e)
            exit(1)


class DefaultJobPrepper(JobPrepper):
    def __init__(
            self,
            context: UFDLServerContext,
            config: UFDLJobLauncherConfig,
            info: HardwareInfo,
            debug: bool
    ):
        self._context = context
        self._config = config
        self._info = info
        self._debug = debug

    def prepare_job(self, job: Job) -> Optional[AbstractJobExecutor]:
        executor = create_executor(self._context, self._config, job, self._debug)
        cant_run_reason = executor.can_run(self._info)
        if cant_run_reason is not None:
            if self._debug:
                logger().debug(
                    f"Can't run job {executor.job['pk']} with template {executor.template['pk']}\n"
                    f"{cant_run_reason}"
                )
            return None
        return executor


def get_next_job(
        poller: Poller,
        context: UFDLServerContext,
        config: UFDLJobLauncherConfig,
        info: HardwareInfo,
        debug: bool = False
) -> AbstractJobExecutor:
    """
    Uses the provided polling strategy to get the next job that
    this node can run.

    :param poller: the polling mechanism to use
    :param context: the UFDL server context
    :param config: the configuration to use
    :param info: the hardware info to use
    :param debug: whether to output debugging information
    :return: the executor of the next job to run.
    """
    job_prepper = DefaultJobPrepper(
        context,
        config,
        info,
        debug
    )

    return poller.poll(
        context,
        config,
        job_prepper,
        debug
    )


def launch_jobs(
        config: UFDLJobLauncherConfig,
        continuous: bool,
        debug: bool = False
):
    """
    Launches the jobs.

    :param config: the configuration to use
    :param continuous: whether to poll continuously or only once
    :param debug: whether to output debugging information
    """
    create_dir(config.docker.work_dir, "work directory")
    create_dir(config.docker.cache_dir, "cache directory")

    context = create_server_context(config, debug=debug)
    info = HardwareInfo.collect(context)

    if debug:
        logger().debug("hardware info: %s" % str(info))

    poll = config.general.poll
    sleep = SleepSchedule(
        config.general.poll_backenderror_wait,
        debug=debug,
        debug_msg="Waiting %s before contacting backend again."
    )

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
            poller = None
            if poll == "simple":
                poller = Simple()
            else:
                logger().fatal(f"Unknown poll method: {poll}")
                exit(1)
            executor = get_next_job(poller, context, config, info, debug=debug)
            if executor is not None:
                executor.run()
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
