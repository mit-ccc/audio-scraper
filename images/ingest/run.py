#!/usr/bin/env python3

'''
Audio ingest worker entrypoint script
'''

import os
import logging

from pool import Pool


logger = logging.getLogger(__name__)


def log_setup():
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


if __name__ == '__main__':
    log_setup()

    store_url = os.getenv('STORE_URL')
    if store_url is None:
        raise ValueError('Must provide STORE_URL environment variable')

    args = {
        'store_url': store_url,

        'dsn': os.getenv('DSN', 'Database'),
        'n_tasks': int(os.getenv('N_TASKS', 10)),
        'poll_interval': int(os.getenv('POLL_INTERVAL', 60)),

        'chunk_size': int(os.getenv('CHUNK_SIZE', 5 * 2**20)),
        'chunk_error_behavior': os.getenv('CHUNK_ERROR_BEHAVIOR', 'ignore'),
        'chunk_error_threshold': int(os.getenv('CHUNK_ERROR_THRESHOLD', 10)),
    }

    with Pool(**args) as pool:
        pool.run()
