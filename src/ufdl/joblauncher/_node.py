import psutil
import socket
import subprocess
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.nodes.hardware import list as list_hardware


def to_bytes(s):
    """
    Turns the string with suffix of KiB/MiB/GiB into bytes.

    :param s: the string (NUM SUFFIX)
    :type s: str
    :return: the number of bytes
    :rtype: int
    """
    factor = 1
    if s.endswith(" KiB"):
        factor = 1024
        s = s[:-4].strip()
    elif s.endswith(" MiB"):
        factor = 1024 * 1024
        s = s[:-4].strip()
    elif s.endswith(" GiB"):
        factor = 1024 * 1024 * 1024
        s = s[:-4].strip()
    elif s.endswith(" TiB"):
        factor = 1024 * 1024 * 1024 * 1024
        s = s[:-4].strip()
    return int(s) * factor


def to_hardware_generation(context, compute):
    """
    Turns the compute number into a hardware generation string

    :param context: the server context
    :type context: UFDLServerContext
    :param compute: the compute number
    :type compute: float
    :return: the hardware generation (pk, name)
    :rtype: dict
    """
    match = None
    for hw in list_hardware(context):
        if (compute >= hw['min_compute_capability']) and (compute < hw['max_compute_capability']):
            match = hw
            break

    if match is not None:
        return {'pk': match['pk'], 'name': match['generation']}
    else:
        raise Exception("Unhandled compute version: " + str(compute))


def hardware_info(context):
    """
    Collects hardware information with the following keys (memory is in bytes):
    - memory
      - total
      - used
      - free
    - driver (NVIDIA driver version, if available)
    - cuda (CUDA version, if available)
    - gpus (if available)
      - device ID (index)
        - model (GeForce RTX 2080 Ti)
        - brand (GeForce)
        - uuid (GPU-AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE)
        - bus (00000000:01:00.0)
        - compute (7.5)
        - generation:
          - pk
          - name
        - memory
          - total
          - used
          - free

    :param context: the server context
    :type context: UFDLServerContext
    :return: the hardware info
    :rtype: dict
    """
    hardware = dict()
    gpus = dict()
    has_gpu = False

    # ram
    try:
        mem = psutil.virtual_memory()
        hardware['memory'] = dict()
        hardware['memory']['total'] = mem.total
        hardware['memory']['used'] = mem.used
        hardware['memory']['free'] = mem.free
    except:
        pass

    # gpu
    try:
        res = subprocess.run(["nvidia-container-cli", "info"], stdout=subprocess.PIPE)
        has_gpu = True
        lines = res.stdout.decode().split("\n")
        index = ""
        minor = ""
        for line in lines:
            if ":" in line:
                parts = line.split(":")
                for i in range(len(parts)):
                    parts[i] = parts[i].strip()
                if "NVRM version" in line:
                    hardware['driver'] = parts[1]
                elif "CUDA version" in line:
                    hardware['cuda'] = parts[1]
                elif "Device Index" in line:
                    index = parts[1]
                elif "Device Minor" in line:
                    minor = parts[1]
                    if not index in gpus:
                        gpus[index] = dict()
                elif "Architecture" in line:
                    gpus[index]['compute'] = float(parts[1])
                    gpus[index]['generation'] = to_hardware_generation(context, float(parts[1]))
                elif "Model" in line:
                    gpus[index]['model'] = parts[1]
                elif "Brand" in line:
                    gpus[index]['brand'] = parts[1]
                elif "GPU UUID" in line:
                    gpus[index]['uuid'] = parts[1]
                elif "Bus Location" in line:
                    gpus[index]['bus'] = ":".join(parts[1:])
    except:
        pass

    # gpu memory
    try:
        res = subprocess.run(["nvidia-smi", "-q", "-d", "MEMORY"], stdout=subprocess.PIPE)
        has_gpu = True
        lines = res.stdout.decode().split("\n")
        bus = ""
        fb = False
        for line in lines:
            if line.startswith("GPU "):
                bus = line[4:].strip()
                continue
            elif "FB Memory Usage" in line:
                fb = True
                continue
            elif "BAR1 Memory Usage" in line:
                fb = False
                continue
            if not fb:
                continue
            if ":" in line:
                parts = line.split(":")
                for i in range(len(parts)):
                    parts[i] = parts[i].strip()
                key = ""
                val = ""
                if "Total" in line:
                    key = "total"
                    val = parts[1]
                elif "Used" in line:
                    key = "used"
                    val = parts[1]
                elif "Free" in line:
                    key = "free"
                    val = parts[1]
                if key != "":
                    for gpu in hardware['gpus'].values():
                        if gpu['bus'] == bus:
                            if not 'memory' in gpu:
                                gpu['memory'] = dict()
                            gpu['memory'][key] = to_bytes(val)
                            break
    except:
        pass

    if has_gpu:
        hardware['gpus'] = gpus

    return hardware


def get_ipv4():
    """
    Returns the primary IPv4 address.

    Source: https://stackoverflow.com/a/28950776/4698227
    Author: fatal_error https://stackoverflow.com/users/1301627/fatal-error
    License: CC-BY-SA 3.0 (https://creativecommons.org/licenses/by-sa/3.0/)

    :return: the IP address
    :rtype: str
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        result = s.getsockname()[0]
    except Exception:
        result = '127.0.0.1'
    finally:
        s.close()
    return result