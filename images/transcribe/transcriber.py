'''
Transcription, diarization and alignment tooling, based on whisperX.
'''

from typing import Optional

import logging

import torch
import whisperx

from load_audio import load_audio


logger = logging.getLogger(__name__)


class Transcriber:
    '''
    A transcriber class to handle transcription, diarization and alignment of
    audio files ingested by the sister containers.
    '''

    def __init__(self,  # pylint: disable=too-many-arguments
                 whisper_version: str = 'base',
                 compute_type: str = 'default',
                 batch_size: int = 1,
                 device: Optional[str] = None,
                 hf_token: Optional[str] = None):
        self.whisper_version = whisper_version
        self.compute_type = compute_type
        self.batch_size = batch_size

        if device is None:
            device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.device = device

        if ':' in self.device:
            dev, ind = self.device.split(':')
        else:
            dev, ind = self.device, '0'

        self.asr = whisperx.load_model(
            whisper_arch=self.whisper_version,
            compute_type=self.compute_type,

            device=dev,
            device_index=int(ind),
        )

        self.diarizer = whisperx.DiarizationPipeline(
            model_name='pyannote/speaker-diarization-3.1',
            use_auth_token=hf_token,
            device=device,
        )

        self.align = None

    def _set_aligner(self, lang=None):
        aligner, metadata = whisperx.load_align_model(
            language_code=lang,
            device=self.device,
        )

        self.align = (lang, aligner, metadata)

    def _get_aligner(self, lang=None):
        if self.align is None or self.align[0] != lang:
            self._set_aligner(lang)

        return self.align[1:]

    def process(self, data, lang=None):
        '''
        Process an audio file, provided as a bytes object, and return a dict
        containing the transcribed, diarized, and aligned results.
        '''

        data = load_audio(data)['waveform']

        result = self.asr.transcribe(
            data,
            language=lang,
            batch_size=self.batch_size
        )

        aligner, metadata = self._get_aligner(lang)

        result = whisperx.align(
            result['segments'], aligner, metadata, data, self.device,
            return_char_alignments=False,
        )

        diarize_segments = self.diarizer(data)
        result = whisperx.assign_word_speakers(diarize_segments, result)

        return result
