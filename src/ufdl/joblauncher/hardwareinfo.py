import argparse
import json
import traceback
import yaml
from .core import SYSTEMWIDE_CONFIG, load_config, create_server_context, hardware_info


def output_info(context, out_format, out_file=None):
    """
    Outputs the hardware information.

    :param context: the UFDL server context to use
    :type context: UFDLServerContext
    :param out_format: the output format (json, yaml)
    :type out_format: str
    :param out_file: the file to write the information to instead of printing it to stdout
    :type out_file: str
    """
    info = hardware_info(context)
    if out_format == "json":
        info_str = json.dumps(info, indent=2)
    elif out_format == "yaml":
        info_str = yaml.dump(info)
    else:
        raise Exception("Unhandled output format: %s" % out_format)
    if out_file is None:
        print(info_str)
    else:
        with open(out_file, "w") as of:
            of.write(info_str)


def main(args=None):
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
    config = load_config(parsed.config)
    context = create_server_context(config)
    output_info(context, parsed.format, out_file=parsed.output)


def sys_main():
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
