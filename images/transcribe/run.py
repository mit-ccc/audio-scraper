#!/usr/bin/env python3

'''
Transcription worker entrypoint script
'''

import os
import logging

import torch

import utils as ut
from worker import TranscribeWorker
from transcriber import Transcriber


logger = logging.getLogger(__name__)


if __name__ == '__main__':
    ut.log_setup()

    SEED = int(os.environ.get('SEED', '42'))
    ut.seed_everything(SEED)

    transcriber = Transcriber(
        whisper_version=os.environ.get('WHISPER_VERSION', 'base'),
        device=('cuda' if torch.cuda.is_available() else 'cpu'),
    )

    args = {
        'transcriber': transcriber,
        'dsn': os.environ.get('DSN', 'Database'),
        'chunk_error_behavior': os.environ.get('CHUNK_ERROR_BEHAVIOR', 'ignore'),
        'chunk_error_threshold': int(os.environ.get('CHUNK_ERROR_THRESHOLD', 10)),
        'poll_interval': int(os.environ.get('POLL_INTERVAL', 60)),
    }

    with TranscribeWorker(**args) as worker:
        worker.run()
