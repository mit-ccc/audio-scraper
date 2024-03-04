from typing import Any, Union, Optional

import os
import io
import json
import random
import logging
import subprocess as sp

import numpy as np
import ffmpeg
import torch


logger = logging.getLogger(__name__)


def log_setup():
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    try:
        log_level = getattr(logging, os.environ['LOG_LEVEL'])
    except KeyError:
        log_level = logging.INFO
    except AttributeError as exc:
        raise AttributeError('Bad log level') from exc

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def seed_everything(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


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


#
# Loading audio
#

def probe(data, timeout=None):
    args = ['ffprobe', '-show_format', '-show_streams',
            '-of', 'json',
            '-i', 'pipe:0']

    with sp.Popen(args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE) as proc:
        out, err = proc.communicate(input=data, timeout=timeout)

        if proc.returncode != 0:
            raise ffmpeg.Error('ffprobe', out, err)

    return json.loads(out.decode('utf-8'))


def discover_sample_rate(data):
    try:
        probe_result = probe(data)

        audio_stream = next((
            stream
            for stream in probe_result['streams']
            if stream['codec_type'] == 'audio'
        ), None)

        if not audio_stream or 'sample_rate' not in audio_stream:
            raise RuntimeError("Could not find sample rate in audio stream")

        return int(audio_stream['sample_rate'])
    except ffmpeg.Error as exc:
        msg = f"Error probing for sample rate: {exc.stderr.decode()}"
        raise RuntimeError(msg) from exc


def load_audio(data, sample_rate: Optional[int] = None):
    if sample_rate is None:
        sample_rate = discover_sample_rate(data)

    try:
        out, _ = (
            ffmpeg.input(
                'pipe:',
                threads=0,
                seek_timestamp=1,
                err_detect='ignore_err',
            )
            .output("-", format="s16le", acodec="pcm_s16le", ac=1,
                    ar=sample_rate)
            .run(
                input=data,
                cmd=["ffmpeg", "-nostdin"],
                capture_stdout=True,
                capture_stderr=True
            )
        )
    except ffmpeg.Error as exc:
        msg = f'Failed to load audio: {exc.stderr.decode()}'
        raise RuntimeError(msg) from exc

    return np.frombuffer(out, np.int16).flatten().astype(np.float32) / 32768.0


def slice_audio(data, sr: int = 16000,
                start_time: Union[float, int] = 0,
                end_time: Optional[Union[float, int]] = None):
    start = int(sr * start_time)
    end = int(sr * (end_time if end_time else data.shape[0]))

    return data[start:end]
