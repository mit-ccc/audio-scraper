#!/bin/bash

# this is a python script, but the postgres image's entrypoint only runs sql
# and shell scripts here, so we have to wrap the python in a heredoc

set -Eeuo pipefail

python3 << 'EOF'
import os
import io
import csv
import logging

from pathlib import Path
from urllib.parse import urlparse
from collections import defaultdict
from functools import cached_property

import boto3
import psycopg2


logger = logging.getLogger(__name__)


class ChunkLoader:
    def __init__(self, store_url, dsn='Database'):
        super().__init__()

        self.store_url = store_url
        self.dsn = dsn

        self.db = psycopg2.connect(
            # host=os.environ.get('POSTGRES_HOST', 'postgres'),
            # port=os.environ.get('PGPORT', 5432),
            host='/var/run/postgresql',

            database=os.environ.get('POSTGRES_DB', 'postgres'),
            user=os.environ.get('POSTGRES_USER', 'postgres'),
            password=os.environ.get('POSTGRES_PASSWORD'),
        )
        self.db.set_session(isolation_level='SERIALIZABLE')

    @cached_property
    def _store_url_parsed(self):
        return urlparse(self.store_url)

    @cached_property
    def storage_mode(self):
        mode = self._store_url_parsed.scheme.lower()

        if mode not in ('s3', 'file'):
            raise ValueError('Bad storage URL format')

        if mode == 'file' and self._store_url_parsed.netloc != '':
            raise ValueError('Bad file URL - has netloc')

        return mode

    def all_files_s3(self):
        s3_bucket = self._store_url_parsed.netloc

        s3_prefix = self._store_url_parsed.path
        if s3_prefix.startswith('/'):
            s3_prefix = s3_prefix[1:]

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket)
        objects = bucket.objects.filter(Prefix=s3_prefix)

        files = set()
        for obj in objects:
            f = obj.key.removeprefix(s3_prefix)
            if f.startswith('/'):
                f = f[1:]

            try:
                prefix, key = f.split('/')
            except ValueError:
                url = f's3://{s3_bucket}' + os.path.join(s3_prefix, f)
                logger.warning(f'Bad path format in s3 bucket: {url}')

            files.add((prefix, key))

        return files

    def all_files_local(self):
        root = self._store_url_parsed.path

        objects = Path(root).rglob('*')

        files = set()
        for obj in objects:
            if not obj.is_file():
                continue

            f = str(obj).removeprefix(root)
            if f.startswith('/'):
                f = f[1:]

            try:
                prefix, key = f.split('/')
            except ValueError:
                logger.warning(f'Bad path format: file://{root}/{obj}')

            files.add((prefix, key))

        return files

    def all_files(self):
        if self.storage_mode == 's3':
            files = self.all_files_s3()
        else:  # self.storage_mode == 'file'
            files = self.all_files_local()

        ret = defaultdict(set)
        for prefix, val in files:
            ret[prefix].add(val)
        ret = dict(ret)

        return ret

    def unprocessed_files(self):
        files = self.all_files()

        queue = set()
        for prefix in files.keys():
            for file in files[prefix]:
                if file.endswith('.json'):
                    continue

                if file + '.json' not in files:
                    queue.add((prefix, file))

        return queue

    def get_station_ids(self):
        with self.db.cursor() as cur:
            cur.execute('select name, station_id from data.station')
            ret = dict(cur.fetchall())

        return ret

    def load_chunks(self, data, delim='|'):
        if not data:
            logger.info('No unprocessed transcribe jobs to load')
            return

        columns = list(data[0].keys())

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, delimiter=delim)
        for row in data:
            writer.writerow(row)
        output.seek(0)

        with self.db.cursor() as cur:
            cur.copy_expert(f'''
            copy app.chunks
                ({', '.join(columns)})
            from stdin
            with
                csv
                delimiter '{delim}'
                null as '';
            ''', file=output)

            self.db.commit()

    def run(self):
        queue = self.unprocessed_files()
        station_ids = self.get_station_ids()

        rows = []
        for prefix, file in queue:
            if prefix not in station_ids.keys():
                logger.warning(f'station {prefix} not in configured stations')
                continue

            station_id = station_ids[prefix]
            url = os.path.join(self.store_url, prefix, file)

            rows += [{
                'station_id': station_id,
                'url': url,
            }]

        self.load_chunks(rows)


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

    ChunkLoader(store_url=os.environ.get('STORE_URL')).run()
EOF
