import json
import logging
import subprocess as sp

import ffmpeg
import requests as rq

import exceptions as ex

logger = logging.getLogger(__name__)


def probe(data, timeout=None):
    args = ['ffprobe', '-show_format', '-show_streams',
            '-of', 'json',
            '-i', 'pipe:0']

    with sp.Popen(args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE) as proc:
        out, err = proc.communicate(input=data, timeout=timeout)

        if proc.returncode != 0:
            msg = f'ffprobe exited abnormally\nstdout: {out}\nstderr: {err}'
            raise ex.IngestException(msg)

    return json.loads(out.decode('utf-8'))


def probe_format(data, timeout=None):
    try:
        res = probe(data, timeout=timeout)
        return res['format']['format_name']
    except (KeyError, ex.IngestException):
        return None


def discover_sample_rate(data):
    try:
        probe_result = probe(data)

        audio_stream = next((
            stream
            for stream in probe_result['streams']
            if stream['codec_type'] == 'audio'
        ), None)

        if not audio_stream or 'sample_rate' not in audio_stream:
            raise ex.IngestException('Could not find sample rate in stream')

        return int(audio_stream['sample_rate'])
    except ffmpeg.Error as exc:
        msg = f'Error probing for sample rate: {exc.stderr.decode()}'
        raise ex.IngestException(msg) from exc
