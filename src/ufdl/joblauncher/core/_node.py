from dataclasses import dataclass
from typing import Dict, Optional

import psutil
import socket
import subprocess
from ufdl.pythonclient import UFDLServerContext
from ufdl.pythonclient.functional.core.nodes.hardware import list as list_hardware


def to_bytes(s: str) -> int:
    """
    Turns the string with suffix of KiB/MiB/GiB into bytes.

    :param s: the string (NUM SUFFIX)
    :return: the number of bytes
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


@dataclass
class HardwareGeneration:
    pk: int
    name: str

    @staticmethod
    def from_compute(context: UFDLServerContext, compute: float) -> 'HardwareGeneration':
        """
        Turns the compute number into a hardware generation string

        :param context: the server context
        :param compute: the compute number
        :return: the hardware generation (pk, name)
        """
        match = None
        for hw in list_hardware(context):
            if (compute >= hw['min_compute_capability']) and (compute < hw['max_compute_capability']):
                match = hw
                break

        if match is not None:
            return HardwareGeneration(match['pk'], match['generation'])
        else:
            raise Exception("Unhandled compute version: " + str(compute))

    @staticmethod
    def from_architecture(context: UFDLServerContext, architecture: str) -> 'HardwareGeneration':
        """
        Turns the architecture name into a hardware generation string

        :param context: the server context
        :param architecture: the architecture
        :return: the hardware generation (pk, name)
        """
        match = None
        for hw in list_hardware(context):
            if architecture == hw['generation']:
                match = hw
                break

        if match is not None:
            return HardwareGeneration(match['pk'], match['generation'])
        else:
            raise Exception("Unhandled architecture: " + str(architecture))


@dataclass
class Memory:
    """
    In bytes.
    """
    total: Optional[int] = None
    used: Optional[int] = None
    free: Optional[int] = None

    @staticmethod
    def try_get_system_memory() -> Optional['Memory']:
        try:
            mem = psutil.virtual_memory()
            return Memory(
                mem.total,
                mem.used,
                mem.free
            )
        except:
            return None


@dataclass
class GPU:
    # E.g. GeForce RTX 2080 Ti
    model: Optional[str] = None

    # E.g. GeForce
    brand: Optional[str] = None

    # E.g. GPU-AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE
    uuid: Optional[str] = None

    # E.g. 00000000:01:00.0
    bus: Optional[str] = None

    # E.g. 7.5
    compute: Optional[float] = None

    # Hardware generation
    generation: Optional[HardwareGeneration] = None

    # Graphics memory
    memory: Optional[Memory] = None

    # FIXME: ???
    minor: Optional[int] = None


@dataclass
class HardwareInfo:
    """
    Hardware configuration relevant to executing jobs.
    """
    # System memory
    memory: Optional[Memory] = None

    # NVIDIA driver version, if available
    driver: Optional[str] = None

    # CUDA version, if available
    cuda: Optional[str] = None

    # If available, keyed by device ID (index)
    gpus: Optional[Dict[int, GPU]] = None

    @staticmethod
    def collect(context: UFDLServerContext) -> 'HardwareInfo':
        """
        Collects the available information about the running hardware.

        :param context: the server context, to resolve the GPU generation
        :return: the hardware info
        """
        hardware = HardwareInfo()
        gpus: Dict[int, GPU] = {}
        has_gpu = False

        # ram
        hardware.memory = Memory.try_get_system_memory()

        # gpu
        try:
            res = subprocess.run(["nvidia-container-cli", "info"], stdout=subprocess.PIPE)
            has_gpu = True
            lines = res.stdout.decode().split("\n")
            index: int = 0
            for line in lines:
                if ":" in line:
                    parts = line.split(":")
                    for i in range(len(parts)):
                        parts[i] = parts[i].strip()
                    if "NVRM version" in line:
                        hardware.driver = parts[1]
                    elif "CUDA version" in line:
                        hardware.cuda = parts[1]
                    elif "Device Index" in line:
                        index = int(parts[1])
                        if index not in gpus:
                            gpus[index] = GPU()
                    elif "Device Minor" in line:
                        gpus[index].minor = int(parts[1])
                    elif "Architecture" in line:
                        gpus[index].compute = float(parts[1])
                        gpus[index].generation = HardwareGeneration.from_compute(context, float(parts[1]))
                    elif "Model" in line:
                        gpus[index].model = parts[1]
                    elif "Brand" in line:
                        gpus[index].brand = parts[1]
                    elif "GPU UUID" in line:
                        gpus[index].uuid = parts[1]
                    elif "Bus Location" in line:
                        gpus[index].bus = ":".join(parts[1:])
        except:
            # if nvidia-container-cli is not available, fall back on nvidia-smi
            try:
                res = subprocess.run(["nvidia-smi", "-q"], stdout=subprocess.PIPE)
                has_gpu = True
                lines = res.stdout.decode().split("\n")
                gpu = None
                for line in lines:
                    if line.startswith("GPU "):
                        gpu = GPU()
                        print("new gpu!")
                    parts = line.split(":")
                    if "Driver Version" in line:
                        hardware.driver = parts[1].strip()
                    elif "CUDA Version" in line:
                        hardware.cuda = parts[1].strip()
                    elif "Minor Number" in line:
                        print("gpu minor", line, parts[1])
                        try:
                            index = int(parts[1])
                        except:
                            # could be N/A
                            index = 0
                        gpu.minor = index
                        if index not in gpus:
                            gpus[index] = gpu
                    elif "Product Architecture" in line:
                        gpu.generation = HardwareGeneration.from_architecture(context, parts[1].strip())
                        print(gpu.generation)
                        for hw in list_hardware(context):
                            if gpu.generation == hw['generation']:
                                gpu.compute = hw['min_compute_capability']
                                break
                    elif "Product Name" in line:
                        gpu.model = parts[1].strip()
                    elif "Product Brand" in line:
                        gpu.brand = parts[1].strip()
                    elif "GPU UUID" in line:
                        gpu.uuid = parts[1].strip()
                    elif "Bus Id" in line:
                        gpu.bus = ":".join(parts[1:]).strip()
            except Exception as e:
                print(e)
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
                        for gpu in gpus.values():
                            if gpu.bus == bus:
                                if gpu.memory is None:
                                    gpu.memory = Memory()
                                setattr(gpu.memory, key, to_bytes(val))
                                break
        except:
            pass

        if has_gpu:
            hardware.gpus = gpus

        return hardware


def get_ipv4() -> str:
    """
    Returns the primary IPv4 address.

    Source: https://stackoverflow.com/a/28950776/4698227
    Author: fatal_error https://stackoverflow.com/users/1301627/fatal-error
    License: CC-BY-SA 3.0 (https://creativecommons.org/licenses/by-sa/3.0/)

    :return: the IP address
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
