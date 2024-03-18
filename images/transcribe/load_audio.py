'''
ffmpeg-based utilities for loading audio and probing its properties.
'''

from typing import Optional

import json
import logging
import subprocess as sp

import numpy as np
import ffmpeg


logger = logging.getLogger(__name__)


def _probe(data, timeout=None):
    args = ['ffprobe', '-show_format', '-show_streams',
            '-of', 'json',
            '-i', 'pipe:0']

    with sp.Popen(args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE) as proc:
        out, err = proc.communicate(input=data, timeout=timeout)

        if proc.returncode != 0:
            raise ffmpeg.Error('ffprobe', out, err)

    return json.loads(out.decode('utf-8'))


def discover_sample_rate(data):
    '''
    Use ffprobe to discover and return the sample rate of the audio provided in
    the data argument, which should be a bytes object.
    '''

    try:
        probe_result = _probe(data)

        audio_stream = next((
            stream
            for stream in probe_result['streams']
            if stream['codec_type'] == 'audio'
        ), None)

        if not audio_stream or 'sample_rate' not in audio_stream:
            raise RuntimeError('Could not find sample rate in audio stream')

        return int(audio_stream['sample_rate'])
    except ffmpeg.Error as exc:
        msg = f'Error probing for sample rate: {exc.stderr.decode()}'
        raise RuntimeError(msg) from exc


def load_audio(data, sample_rate: Optional[int] = None):
    '''
    Load audio from a bytes object and return it as a numpy array (in pcm_s16le
    format) at the same sample rate.
    '''

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
            .output('-', format='s16le', acodec='pcm_s16le', ac=1,
                    ar=sample_rate)
            .run(
                input=data,
                cmd=['ffmpeg', '-nostdin'],
                capture_stdout=True,
                capture_stderr=True
            )
        )

        out = np.frombuffer(out, np.int16) \
            .flatten() \
            .astype(np.float32) \
            / 32768.0

    except ffmpeg.Error as exc:
        msg = f'Failed to load audio: {exc.stderr.decode()}'
        raise RuntimeError(msg) from exc

    return {'waveform': out, 'sample_rate': sample_rate}
