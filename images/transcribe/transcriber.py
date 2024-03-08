from typing import Optional

import os
import io
import json
import logging
import subprocess as sp

import numpy as np
import torch

import ffmpeg

from faster_whisper import WhisperModel

from pyannote.audio import Pipeline


logger = logging.getLogger(__name__)



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


def segment_to_json(segment):
    segment_fields = ['start', 'end', 'text', 'avg_logprob', 'no_speech_prob']
    word_fields = ['start', 'end', 'word', 'probability']

    ret = {}
    for field in segment_fields:
        ret[field] = getattr(segment, field)

    ret['words'] = []
    for word in getattr(segment, 'words'):
        ret['words'] += [{f: getattr(word, f) for f in word_fields}]

    return ret


def whisper_to_json(segments, info):
    segments = [segment_to_json(seg) for seg in segments]
    return dict(info._asdict(), segments=segments)


class Transcriber:
    def __init__(self, whisper_version: str = 'base', device: str = 'cpu',
                 compute_type: str = 'default'):
        self.asr = WhisperModel(
            whisper_version,
            device=device,
            compute_type=compute_type,
        )

        self.diarizer = Pipeline.from_pretrained(
            'pyannote/speaker-diarization-3.0',
            use_auth_token=os.environ['HF_ACCESS_TOKEN'],
        )

    def process(self, data, lang=None):
        sample_rate = discover_sample_rate(data)
        data = load_audio(data, sample_rate=sample_rate)

        transcribed = self.transcribe(data, lang=lang)

        diarized = self.diarize({
            'waveform': torch.from_numpy(data[None, :]),
            'sample_rate': sample_rate,
        })

        return {
            'transcription': transcribed,
            'diarization': diarized,
        }

    def diarize(self, obj):
        res = self.diarizer(obj)

        segments = []
        for turn, _, speaker in res.itertracks(yield_label=True):
            segments.append({
                'start': turn.start,
                'end': turn.end,
                'speaker_id': speaker
            })

        return segments

    def transcribe(self, obj, lang=None):
        segments, info = self.asr.transcribe(obj, word_timestamps=True,
                                             language=lang)
        segments = list(segments)

        return whisper_to_json(segments, info)
