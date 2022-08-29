import argparse
import traceback
from typing import List, Optional

from ufdl.joblauncher.core import launch_jobs, init_logger
from ufdl.joblauncher.core.config import UFDLJobLauncherConfig, SYSTEMWIDE_CONFIG


def main(args: Optional[List[str]] = None):
    """
    Starts the job launcher.
    Use -h to see all options.

    :param args: the command-line arguments to use, uses sys.argv if None
    :type args: list
    """
    parser = argparse.ArgumentParser(
        description='Starts the UFDL job-launcher.',
        prog="ufdl-joblauncher",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-C", "--config",
        dest="config",
        metavar="FILE",
        required=False,
        help=f"The configuration to use if not the system wide one ({SYSTEMWIDE_CONFIG})."
    )
    parser.add_argument(
        "-c", "--continuous",
        action="store_true",
        required=False,
        help="For continuous polling for jobs rather than stopping after executing the first one."
    )

    parsed = parser.parse_args(args=args)
    config = UFDLJobLauncherConfig(parsed.config)
    dbg = config.general.debug
    init_logger(dbg)
    launch_jobs(config, parsed.continuous, debug=config.general.debug)


def sys_main() -> int:
    """
    Runs the main function using the system cli arguments, and
    returns a system error code.

    :return: 0 for success, 1 for failure.
    :rtype: int
    """
    try:
        main()
        return 0
    except Exception:
        print(traceback.format_exc())
        return 1


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
