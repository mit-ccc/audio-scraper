'''
This file is the main file for the audio worker. It downloads the audio stream
from the remote (e.g., radio station site) and uploads it to S3.
'''

from typing import Optional

import gc
import io
import os
import sys
import time
import random
import logging

from functools import cached_property
from urllib.parse import urlparse

import boto3
import pyodbc

import exceptions as ex
from audio_stream import AudioStream


logger = logging.getLogger(__name__)


def payload(args):
    '''
    Payload wrapper for Pool to run workers.
    '''

    try:
        with Worker(**args) as worker:
            worker.run()
    except Exception:
        logger.exception("Error in station ingest")

        raise


class Worker:  # pylint: disable=too-many-instance-attributes
    '''
    Worker class for audio ingest. The worker acquires a station to ingest from
    the database, downloads the audio stream, and uploads it to S3. The
    database is used for synchronization, with each worker acquiring a lock on
    the station is is working on. The worker will exit if the station is no
    longer in the database, or if the station has failed too many times.
    '''

    # pylint: disable-next=too-many-arguments
    def __init__(self, store_url: str, dsn: str = 'Database',
                 chunk_error_threshold: Optional[int] = None,
                 chunk_size: int = 5*2**20, poll_interval: float = 10.0):
        super().__init__()

        # No AWS creds - we assume they're in the environment
        self.dsn = dsn
        self.store_url = store_url
        self.chunk_error_threshold = chunk_error_threshold
        self.chunk_size = chunk_size
        self.poll_interval = poll_interval

        self.db = pyodbc.connect(dsn=self.dsn)
        self.db.autocommit = True

        self.station = None
        self.station_id = None
        self.stream_url = None

        if self.storage_mode == 's3':
            self._client = boto3.client('s3')
        else:
            self._client = None

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.close()

    def write_chunk(self, chunk, key=None):
        assert self.station is not None

        if key is None:
            key = str(int(time.time() * 1000000))
        key = os.path.join(self.station, key)

        if self.storage_mode == 's3':
            s3_bucket = self._store_url_parsed.netloc

            s3_prefix = self._store_url_parsed.path
            if s3_prefix.startswith('/'):
                s3_prefix = s3_prefix[1:]

            s3_key = os.path.join(s3_prefix, key)

            with io.BytesIO(chunk) as fobj:
                self._client.upload_fileobj(fobj, s3_bucket, s3_key)
        else:  # self.storage_mode == 'file'
            path = self._store_url_parsed.path
            path = os.path.join(path, key)

            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as fobj:
                fobj.write(chunk)

        out_url = os.path.join(self.store_url, key)
        logger.info('Successfully fetched and saved %s', out_url)

        return out_url

    def lock_task(self):
        '''
        Lock a task in the database via an advisory lock on the station ID, in
        order to prevent multiple workers from working on the same station at
        the same time.
        '''

        params = (
            self.chunk_error_threshold is None,
            self.chunk_error_threshold,
            self.chunk_error_threshold is None,
            self.chunk_error_threshold
        )

        with self.db.cursor() as cur:
            cur.execute('''
            -- SQL ported from https://github.com/chanks/que
            with recursive job_locks as
            (
                select
                    (j).*,
                    pg_try_advisory_lock((j).station_id) as locked
                from
                (
                    select
                        j
                    from app.ingest_jobs j
                    where
                        ? or
                        j.error_count < ?
                    order by station_id
                    limit 1
                ) as t1

                union all

                (
                    select
                        (j).*,
                        pg_try_advisory_lock((j).station_id) as locked
                    from
                    (
                        select
                        (
                            select
                                j
                            from app.ingest_jobs j
                            where
                                j.station_id > job_locks.station_id and
                                (
                                    ? or
                                    j.error_count < ?
                                )
                            order by station_id
                            limit 1
                        ) as j
                        from job_locks
                        where
                            job_locks.station_id is not null
                        limit 1
                    ) as t1
                )
            )
            select
                station_id
            from job_locks
            where
                locked
            limit 1;
            ''', params)

            res = cur.fetchall()
            return res[0][0] if len(res) > 0 else None

    def release_lock(self):
        '''
        Release the lock the worker holds on its station. This should be called
        when the worker exits to avoid orphaning the station.
        '''

        with self.db.cursor() as cur:
            cur.execute('''
            select
                pg_advisory_unlock(?);
            ''', (self.station_id,))

            return cur.fetchone()[0]

    def close(self):
        '''
        Tear down the worker's persistent resources: the database connection and
        the advisory lock on the station ID.
        '''

        try:
            self.release_lock()
        except Exception:  # pylint: disable=broad-except
            pass

        try:
            self.db.close()
        except Exception:  # pylint: disable=broad-except
            pass

        self.station_id = None
        self.stream_url = None

    def get_stop_conditions(self):
        '''
        Check whether the worker has hit its stop conditions and should exit.
        The worker will exit if the station is no longer in the database, or if
        the station has failed too many times.
        '''

        with self.db.cursor() as cur:
            params = (
                self.station_id,
                self.station_id,
                self.chunk_error_threshold
            )

            cur.execute('''
            select
                not exists(
                    select
                        1
                    from app.ingest_jobs
                    where
                        station_id = ?
                ) as deleted,

                exists(
                    select
                        1
                    from app.ingest_jobs
                    where
                        station_id = ? and

                        -- the exists() will return false if ? is null
                        -- which happens if self.chunk_error_threshold is None
                        error_count >= ?
                ) as failed;
            ''', params)

            ret = cur.fetchone()
            cols = [col[0] for col in cur.description]

            return dict(zip(cols, ret))

    def acquire_task(self):
        '''
        Acquire a task (i.e., station to work on) from the database, blocking
        until one is available. Configure the station ID and stream URL on the
        worker from the acquired task.
        '''

        # in a high-concurrency situation,
        # spread out the load on the DB
        time.sleep(random.uniform(0, 2*self.poll_interval))

        # Use a spinlock; if there's nothing to work on, let's
        # wait around and keep checking if there is
        while True:
            res = self.lock_task()
            if res is not None:
                break

            # nothing to lock
            logger.debug('Nothing to work on; spinning')
            time.sleep(self.poll_interval)
        self.station_id = res

        with self.db.cursor() as cur:
            cur.execute('''
            select
                name as station,
                stream_url
            from data.station
            where
                station_id = ?;
            ''', (self.station_id,))

            res = cur.fetchone()
            self.station = res[0]
            self.stream_url = res[1]

        return self

    def run(self):
        '''
        Main worker entrypoint. Acquire a task, then ingest its stream.
        '''

        self.acquire_task()

        logger.info(
            "Began ingesting station_id %s from %s",
            self.station_id, self.stream_url
        )

        try:
            stream, itr = None, None

            while True:
                conds = self.get_stop_conditions()

                if conds['deleted']:
                    msg = "Job %s cancelled"
                    vals = (self.station_id,)
                    raise ex.JobCancelledException(msg % vals)

                if conds['failed']:
                    msg = "Job %s had too many failures"
                    vals = (self.station_id,)
                    raise ex.TooManyFailuresException(msg % vals)

                try:
                    # do this rather than "for chunk in stream" so that
                    # we can get everything inside the try block
                    if stream is None:
                        stream = AudioStream(
                            url=self.stream_url,
                            chunk_size=self.chunk_size,
                        )

                        itr = iter(stream)

                    chunk = next(itr)
                    out_url = self.write_chunk(chunk)
                except Exception as exc:  # pylint: disable=broad-except
                    with self.db.cursor() as cur:
                        # log the failure; this is concurency-safe because
                        # we have the lock on this station_id
                        cur.execute('''
                        update app.ingest_jobs
                        set
                            error_count = error_count + 1,
                            last_error = ?
                        where
                            station_id = ?;
                        ''', (str(sys.exc_info()), self.station_id))

                    if isinstance(exc, StopIteration):
                        raise  # no point continuing after we hit this

                    logger.exception('Chunk failed')
                else:
                    # log the success
                    with self.db.cursor() as cur:
                        cur.execute('''
                        insert into app.chunks
                            (station_id, url)
                        values
                            (?, ?);
                        ''', (self.station_id, out_url))
                finally:
                    gc.collect()
        finally:
            try:
                stream.close()
            except Exception:  # pylint: disable=broad-except
                pass

        return self
