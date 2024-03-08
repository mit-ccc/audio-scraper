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

    thresh = os.getenv('CHUNK_ERROR_THRESHOLD', None)
    if thresh == 'None' or thresh == 'null' or int(thresh) < 0:
        thresh = None
    else:
        thresh = int(thresh)
    assert thresh is None or thresh > 0

    args = {
        'store_url': store_url,

        'dsn': os.getenv('DSN', 'Database'),
        'n_tasks': int(os.getenv('N_TASKS', '10')),
        'poll_interval': int(os.getenv('POLL_INTERVAL', '60')),

        'chunk_size': int(os.getenv('CHUNK_SIZE', str(5 * 2**20))),
        'chunk_error_threshold': thresh,
    }

    with Pool(**args) as pool:
        pool.run()
