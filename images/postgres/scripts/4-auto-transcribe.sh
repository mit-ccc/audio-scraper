#!/bin/bash

# this is a python script, but the postgres image's entrypoint only runs sql
# and shell scripts here, so we have to wrap the python in a heredoc

set -Eeuo pipefail

python3 <<EOF
import os
import io
import csv
import logging

from collections import defaultdict

import boto3
import psycopg2


logger = logging.getLogger(__name__)

class ChunkLoader:
    def __init__(self, s3_bucket, s3_prefix='', dsn='Database'):
        super().__init__()

        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix[1:] if s3_prefix.startswith('/') else s3_prefix
        self.dsn = dsn

        self._s3 = boto3.resource('s3')
        self.bucket = self._s3.Bucket(self.s3_bucket)

        self.db = psycopg2.connect(
            # host=os.environ.get('POSTGRES_HOST', 'postgres'),
            # port=os.environ.get('PGPORT', 5432),
            host='/var/run/postgresql',

            database=os.environ.get('POSTGRES_DB', 'postgres'),
            user=os.environ.get('POSTGRES_USER', 'postgres'),
            password=os.environ.get('POSTGRES_PASSWORD'),
        )
        self.db.set_session(isolation_level='SERIALIZABLE')

    def s3_files(self):
        objects = self.bucket.objects.filter(Prefix=self.s3_prefix)

        files = set()
        for obj in objects:
            f = obj.key.removeprefix(self.s3_prefix)
            if f.startswith('/'):
                f = f[1:]

            try:
                prefix, key = f.split('/')
            except ValueError:
                url = f's3://{self.s3_bucket}' + os.path.join(self.s3_prefix, f)
                logger.warning(f'Bad path format in s3 bucket: {url}')

            files.add((prefix, key))

        ret = defaultdict(set)
        for prefix, val in files:
            ret[prefix].add(val)
        ret = dict(ret)

        return ret

    def unprocessed_s3_files(self):
        files = self.s3_files()

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
            logger.warning('No rows loaded')
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
        queue = self.unprocessed_s3_files()
        station_ids = self.get_station_ids()

        rows = []
        for prefix, file in queue:
            if prefix not in station_ids.keys():
                logger.warning(f'station {prefix} not in configured stations')
                continue

            station_id = station_ids[prefix]
            s3_url = f's3://{self.s3_bucket}/' + os.path.join(self.s3_prefix, prefix, file)

            rows += [{
                'station_id': station_id,
                's3_url': s3_url,
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

    ChunkLoader(
        s3_bucket=os.getenv('S3_BUCKET'),
        s3_prefix=os.getenv('S3_PREFIX', ''),
    ).run()
EOF
