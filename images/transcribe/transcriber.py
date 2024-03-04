import os
import io
import logging

from faster_whisper import WhisperModel

from pyannote.audio import Pipeline
from pyannote.audio.pipelines import SpeakerDiarization

import utils as ut


logger = logging.getLogger(__name__)


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
    def __init__(self, whisper_version: str = 'base', device: str = 'cpu'):
        self.asr = WhisperModel(
            whisper_version,
            device=device,
            compute_type='int8',
        )

        self.diarizer = Pipeline.from_pretrained(
            'pyannote/speaker-diarization-3.0',
            use_auth_token=os.environ['HF_ACCESS_TOKEN'],
        )

    def diarize(self, data):
        with io.BytesIO(data) as infile:
            res = self.diarizer(infile)

        segments = []
        for turn, _, speaker in res.itertracks(yield_label=True):
            segments.append({
                'start': turn.start,
                'end': turn.end,
                'speaker_id': speaker
            })

        return segments

    def transcribe(self, data):
        with io.BytesIO(data) as infile:
            segments, info = self.asr.transcribe(infile, word_timestamps=True)
            segments = list(segments)

        return whisper_to_json(segments, info)

    def process(self, data):
        return {
            'diarization': self.diarize(data),
            'transcription': self.transcribe(data),
        }
