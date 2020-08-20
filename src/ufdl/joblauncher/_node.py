import psutil
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
    :return: the hardware generation
    :rtype: str
    """
    match = None
    for hw in list_hardware(context):
        if (compute >= hw['min_compute_capability']) and (compute < hw['max_compute_capability']):
            match = hw
            break

    if match is not None:
        return match['generation']
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
      - device ID (major.minor)
        - model (GeForce RTX 2080 Ti)
        - brand (GeForce)
        - uuid (GPU-AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE)
        - bus (00000000:01:00.0)
        - compute (7.5)
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
        major = ""
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
                    major = parts[1]
                elif "Device Minor" in line:
                    minor = parts[1]
                    if not major + "." + minor in gpus:
                        gpus[major + "." + minor] = dict()
                elif "Architecture" in line:
                    gpus[major + "." + minor]['compute'] = float(parts[1])
                    gpus[major + "." + minor]['generation'] = to_hardware_generation(context, float(parts[1]))
                elif "Model" in line:
                    gpus[major + "." + minor]['model'] = parts[1]
                elif "Brand" in line:
                    gpus[major + "." + minor]['brand'] = parts[1]
                elif "GPU UUID" in line:
                    gpus[major + "." + minor]['uuid'] = parts[1]
                elif "Bus Location" in line:
                    gpus[major + "." + minor]['bus'] = ":".join(parts[1:])
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
