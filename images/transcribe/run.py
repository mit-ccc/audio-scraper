#!/usr/bin/env python3

'''
Transcription worker entrypoint script
'''

import os
import random
import logging

import numpy as np
import torch

import utils as ut
from worker import TranscribeWorker
from transcriber import Transcriber


logger = logging.getLogger(__name__)


def log_setup():
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    try:
        log_level = getattr(logging, os.environ['LOG_LEVEL'])
    except KeyError:
        log_level = logging.INFO
    except AttributeError as exc:
        raise AttributeError('Bad log level') from exc

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def seed_everything(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


if __name__ == '__main__':
    log_setup()

    SEED = int(os.environ.get('SEED', '42'))
    seed_everything(SEED)

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
