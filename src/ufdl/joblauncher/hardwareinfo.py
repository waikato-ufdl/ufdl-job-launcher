import argparse
import json
import traceback
from typing import Callable, Dict, List, Optional

import yaml

from ufdl.pythonclient import UFDLServerContext

from ufdl.joblauncher.core import create_server_context, HardwareInfo
from ufdl.joblauncher.core.config import SYSTEMWIDE_CONFIG, UFDLJobLauncherConfig

SUPPORTED_OUT_FORMATS: Dict[str, Callable[[HardwareInfo], str]] = {
    "json": lambda info: json.dumps(info, indent=2),
    "yaml": yaml.dump
}


def output_info(
        context: UFDLServerContext,
        out_format: str,
        out_file: Optional[str] = None
):
    """
    Outputs the hardware information.

    :param context: the UFDL server context to use
    :param out_format: the output format (json, yaml)
    :param out_file: the file to write the information to instead of printing it to stdout
    """
    info = HardwareInfo.collect(context)

    if out_format not in SUPPORTED_OUT_FORMATS:
        raise ValueError(
            f"Unhandled output format: {out_format}\n"
            f"Must be one of: {SUPPORTED_OUT_FORMATS.keys()}"
        )

    info_str = SUPPORTED_OUT_FORMATS[out_format](info)

    if out_file is None:
        print(info_str)
    else:
        with open(out_file, "w") as of:
            of.write(info_str)


def main(args: Optional[List[str]] = None):
    """
    Outputs the hardware info.
    Use -h to see all options.

    :param args: the command-line arguments to use, uses sys.argv if None
    :type args: list
    """
    parser = argparse.ArgumentParser(
        description='Outputs UFDL hardware information.',
        prog="ufdl-hwinfo",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-C", "--config", dest="config", metavar="FILE", required=False, help="The configuration to use if not the system wide one (%s)." % SYSTEMWIDE_CONFIG)
    parser.add_argument("-F", "--format", dest="format", metavar="FORMAT", default="yaml", choices=["yaml", "json"], help="The format to use for the output of the information.")
    parser.add_argument("-O", "--output", dest="output", metavar="FILE", default=None, help="The file to store the information in, otherwise stdout is used.")
    parsed = parser.parse_args(args=args)
    config = UFDLJobLauncherConfig(parsed.config)
    context = create_server_context(config)
    output_info(context, parsed.format, out_file=parsed.output)


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
