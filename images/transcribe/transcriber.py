import os
import io
import logging

import torch

import transformers as tf

from pyannote.audio import Pipeline
from pyannote.audio.pipelines import SpeakerDiarization

import utils as ut


logger = logging.getLogger(__name__)


class Transcriber:
    def __init__(self, whisper_version: str = 'base', device: str = 'cpu'):
        self.asr = tf.pipeline(
            'automatic-speech-recognition',
            model=f"openai/whisper-{whisper_version}",
            device=device,
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

    def process(self, data):
        sample_rate = ut.discover_sample_rate(data)
        waveform = ut.load_audio(data, sample_rate=sample_rate)

        ret = []
        for segment in self.diarize(data):
            audio_segment = ut.slice_audio(waveform, sample_rate, segment['start'], segment['end'])

            with torch.inference_mode():
                chunk_asr = self.asr(
                    audio_segment,
                    return_timestamps='word',
                    return_language=True,
                )

            chunk_asr['speaker_id'] = segment['speaker_id']
            chunk_asr['speaker_turn_start'] = segment['start']
            chunk_asr['speaker_turn_end'] = segment['end']

            ret.append(chunk_asr)

        return ret
