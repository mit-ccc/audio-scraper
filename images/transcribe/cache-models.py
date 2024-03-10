#!/usr/bin/env python3

import os
import logging

import whisperx


logger = logging.getLogger(__name__)


if __name__ == '__main__':
        hf_token = os.getenv('HF_TOKEN', None)

        device = 'cpu'  # we just want to download and cache the models
        compute_type = 'int8'

        whisper_versions = [
            'tiny', 'tiny.en',
            'base', 'base.en',
            'small', 'small.en',
            'medium', 'medium.en',
            'large', 'large-v1', 'large-v2', 'large-v3',
        ]

        langs = (
            set(whisperx.alignment.DEFAULT_ALIGN_MODELS_TORCH.keys()) |
            set(whisperx.alignment.DEFAULT_ALIGN_MODELS_HF.keys())
        )

        whisperx.DiarizationPipeline(
            model_name='pyannote/speaker-diarization-3.1',
            use_auth_token=hf_token,
            device=device,
        )

        for lang in langs:
            whisperx.load_align_model(language_code=lang, device=device)

        for version in whisper_versions:
            whisperx.load_model(
                whisper_arch=version,
                compute_type=compute_type,
                device=device,
            )
