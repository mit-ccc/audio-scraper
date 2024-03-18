#!/usr/bin/env python3

'''
Transcription worker entrypoint script
'''

import os
import random
import logging

import numpy as np
import torch

from worker import TranscribeWorker


logger = logging.getLogger(__name__)


def _log_setup():
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)

    try:
        log_level = os.getenv('LOG_LEVEL', 'INFO')
        log_level = getattr(logging, log_level)
    except AttributeError as exc:
        raise AttributeError('Bad log level') from exc

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def _seed_everything(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


if __name__ == '__main__':
    # pylint: disable=invalid-name

    _log_setup()

    SEED = int(os.getenv('SEED', '42'))
    _seed_everything(SEED)

    thresh = os.getenv('TRANSCRIBE_CHUNK_ERROR_THRESHOLD', None)
    if thresh is None:
        pass
    elif thresh == 'None' or thresh == 'null' or int(thresh) < 0:
        thresh = None
    else:
        thresh = int(thresh)
    assert thresh is None or thresh > 0

    kwargs = {
        'whisper_version': os.getenv('WHISPER_VERSION', 'base'),
        'compute_type': os.getenv('COMPUTE_TYPE', 'default'),
        'device': os.getenv('DEVICE', None),
        'hf_token': os.getenv('HF_TOKEN', None),
        'dsn': os.getenv('DSN', 'Database'),
        'chunk_error_threshold': thresh,
        'poll_interval': int(os.getenv('POLL_INTERVAL', '60')),
        'remove_audio': os.getenv('REMOVE_AUDIO', 'false').lower() not in ('false', '0', '')
    }

    with TranscribeWorker(**kwargs) as worker:
        worker.run()
