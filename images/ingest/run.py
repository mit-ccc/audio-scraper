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


if __name__ == '__main__':
    log_setup()

    args = {
        's3_bucket': os.environ.get('S3_BUCKET'),
        's3_prefix': os.environ.get('S3_PREFIX', ''),

        'dsn': os.environ.get('DSN', 'Database'),
        'n_tasks': int(os.environ.get('N_TASKS', 10)),
        'poll_interval': int(os.environ.get('POLL_INTERVAL', 60)),

        'chunk_size': int(os.environ.get('CHUNK_SIZE', 5 * 2**20)),
        'chunk_error_behavior': os.environ.get('CHUNK_ERROR_BEHAVIOR', 'ignore'),
        'chunk_error_threshold': int(os.environ.get('CHUNK_ERROR_THRESHOLD', 10)),
    }

    with Pool(**args) as pool:
        pool.run()
