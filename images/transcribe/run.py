#!/usr/bin/env python3

'''
Transcription worker entrypoint script
'''

import os
import logging

import numpy as np

import boto3
import botocore

import torch

import utils as ut
from worker import TranscribeWorker
from transcriber import CpuTranscriber, CudaTranscriber


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


def get_transcriber():
    has_cuda = torch.cuda.is_available()
    kls = CudaTranscriber if has_cuda else CpuTranscriber

    params = {
        'cuda_devices': 'auto' if has_cuda else [],
        'whisper_version': os.environ.get('WHISPER_VERSION', 'base'),
        'compute_type': os.environ.get('COMPUTE_TYPE', 'auto'),
    }

    return kls(**params)


if __name__ == '__main__':
    log_setup()

    SEED = int(os.environ.get('SEED', '42'))
    ut.seed_everything(SEED)

    transcriber = get_transcriber()

    args = {
        'transcriber': transcriber,
        'dsn': os.environ.get('DSN', 'Database'),
        'chunk_error_behavior': os.environ.get('CHUNK_ERROR_BEHAVIOR', 'ignore'),
        'chunk_error_threshold': int(os.environ.get('CHUNK_ERROR_THRESHOLD', 10)),
        'poll_interval': int(os.environ.get('POLL_INTERVAL', 60)),
    }

    with TranscribeWorker(**args) as worker:
        worker.run()
