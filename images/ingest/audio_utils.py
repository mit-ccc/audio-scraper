import json
import logging
import mimetypes as mt
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


def autodetect_ext_ffprobe(url):
    kwargs = {'url': url, 'stream': True, 'timeout': 10}
    with rq.get(**kwargs) as resp:
        if not resp.ok:
            resp.raise_for_status()

        chunk = next(iter(resp.iter_content(chunk_size=2**17)))

    return probe_format(chunk)


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


def autodetect_ext_mime_type(url):
    try:
        # Open a stream to it and guess by MIME type
        args = {'url': url, 'stream': True, 'timeout': 10}

        with rq.get(**args) as resp:
            mimetype = resp.headers.get('Content-Type')

            if mimetype is None:
                autoext = ''
            else:
                if ';' in mimetype:
                    mimetype = mimetype.split(';')[0]

                autoext = mt.guess_extension(mimetype)
                if autoext is None:
                    autoext = ''
                else:
                    autoext = autoext[1:]

        ext = autoext
    except Exception:  # pylint: disable=broad-except
        msg = 'Encountered exception while guessing stream type'
        logger.warning(msg)

        ext = ''

    return ext

