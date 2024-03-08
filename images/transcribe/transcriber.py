import os
import logging

import torch
import whisperx

from load_audio import load_audio


logger = logging.getLogger(__name__)


class Transcriber:
    def __init__(self, whisper_version: str = 'base', device: str = 'cpu',
                 compute_type: str = 'default', batch_size: int = 1):
        self.whisper_version = whisper_version
        self.device = device
        self.compute_type = compute_type
        self.batch_size = batch_size

        self.asr = whisperx.load_model(whisper_version, device,
                                       compute_type=compute_type)

        self.diarizer = whisperx.DiarizationPipeline(
            use_auth_token=os.environ['HF_ACCESS_TOKEN'],
            device=device,
        )

        self.align = None

    def set_aligner(self, lang=None):
        aligner, metadata = whisperx.load_align_model(
            language_code=lang,
            device=self.device,
        )

        self.align = (lang, aligner, metadata)

    def get_aligner(self, lang=None):
        if self.align is None or self.align[0] != lang:
            self.set_aligner(lang)

        return self.align[1:]

    def process(self, data, lang=None):
        data = load_audio(data)

        result = self.asr.transcribe(
            data,
            language=lang,
            batch_size=self.batch_size
        )

        aligner, metadata = self.get_aligner(lang)

        result = whisperx.align(
            result['segments'], aligner, metadata, data, self.device,
            return_char_alignments=False,
        )

        diarize_segments = self.diarizer(data)
        result = whisperx.assign_word_speakers(diarize_segments, result)

        return result
