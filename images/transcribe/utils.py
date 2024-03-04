import logging
import subprocess as sp


logger = logging.getLogger(__name__)


def get_free_gpus(min_memory_mb=1024):
    result = sp.run([
        'nvidia-smi',
        '--query-gpu=memory.free',
        '--format=csv'
    ], check=True, stdout=sp.PIPE)
    # command line output looks like:
    # $ nvidia-smi --query-gpu=memory.free --format=csv
    # memory.free [MiB]
    # 11171 MiB
    # 11171 MiB
    # 11171 MiB
    # 11171 MiB

    result = result.stdout.decode('utf-8')
    result = result.strip().split('\n')[1:]  # drop header

    # Loop through each line and extract the GPU index and free memory
    free_memory = {}
    for i, line in enumerate(result):
        index, memory = i, int(line.strip().split()[0])
        if memory >= min_memory_mb:  # 1GB in megabytes
            free_memory[index] = memory

    return free_memory
