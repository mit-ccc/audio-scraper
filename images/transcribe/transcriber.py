import torch

from pyannote.audio.pipelines import SpeakerDiarization
# import pyannote.core
# import pyannote.audio
# import pyannote.audio.pipelines
import transformers as tf
from pydub import AudioSegment

import utils as ut


class Transcriber:
    def __init__(self, whisper_version: str = 'base', device: str = 'cpu'):
        self.asr = tf.pipeline(
            'automatic-speech-recognition',
            model=f"openai/whisper-{whisper_version}",
            device=device,
        )

        self.diarizer = SpeakerDiarization(segmentation='pyannote/segmentation')

    def process(self, chunk):
        data = chunk.fetch()

        sample_rate = ut.discover_sample_rate(data)
        data = ut.load_audio(data, sample_rate=sample_rate)

        ret = []
        for segment in self.diarize(data, sample_rate):
            audio_segment = ut.slice_audio(data, sample_rate, segment['start'], segment['end'])

            # Perform transcription
            chunk_asr = self.transcribe(audio_segment)
            chunk_asr['speaker_id'] = segment['speaker_id']
            chunk_asr['speaker_turn_start'] = segment['start']
            chunk_asr['speaker_turn_end'] = segment['end']

            ret.append(chunk_asr)

        return ret

    def diarize(self, data, sample_rate):
        # Perform speaker diarization
        diarization_result = self.diarizer({'audio': data})

        # Convert diarization result to segments
        segments = []
        for turn, _, speaker in diarization_result.itertracks(yield_label=True):
            segments.append({
                'start': turn.start,
                'end': turn.end,
                'speaker_id': speaker
            })

        return segments

    def transcribe(self, data):
        # Convert data to AudioSegment for compatibility
        audio_segment = AudioSegment(data).set_frame_rate(16000).set_channels(1).set_sample_width(2)

        # Transcribe audio using Whisper
        transcription = self.asr_pipeline(audio_segment)

        # Convert transcription to desired format
        return ut.whisper_to_json(transcription)
