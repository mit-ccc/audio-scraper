'''
This file is the main file for the audio worker. It downloads the audio stream
from the remote (e.g., radio source site) and uploads it to S3.
'''

from typing import Optional

import io
import os
import sys
import time
import random
import logging
import traceback

from functools import cached_property
from urllib.parse import urlparse

import boto3
import pyodbc
import backoff

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
        logger.exception("Error in source ingest")

        raise


class Worker:  # pylint: disable=too-many-instance-attributes
    '''
    Worker class for audio ingest. The worker acquires a source to ingest from
    the database, downloads the audio stream, and uploads it to S3. The
    database is used for synchronization, with each worker acquiring a lock on
    the source is is working on. The worker will exit if the source is no
    longer in the database, or if the source has failed too many times.
    '''

    # pylint: disable-next=too-many-arguments
    def __init__(self, store_url: str, dsn: str = 'Database',
                 chunk_error_threshold: Optional[int] = None,
                 chunk_size_seconds: int = 30,
                 poll_interval: float = 10.0,
                 save_format: str = 'wav'):
        super().__init__()

        # No AWS creds - we assume they're in the environment
        self.dsn = dsn
        self.store_url = store_url
        self.chunk_error_threshold = chunk_error_threshold
        self.chunk_size_seconds = chunk_size_seconds
        self.poll_interval = poll_interval
        self.save_format = save_format

        # properties of the ingested source set in the acquire_task method
        self.source_id = None
        self.source = None
        self.stream_url = None
        self.retry_on_close = None

        # the AudioStream instance and iterator set up in run()
        self.stream = None
        self.iterator = None

        self.db = pyodbc.connect(dsn=self.dsn, autocommit=False)

        if self.storage_mode == 's3':
            self._client = boto3.client('s3')
        else:
            self._client = None

    #
    # Worker properties and management
    #

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

    def close(self):
        '''
        Tear down the worker's persistent resources: the audio stream's network
        connection, the database connection and the advisory lock on the
        source ID.
        '''

        try:
            self.stream.close()
        except Exception:  # pylint: disable=broad-except
            pass

        try:
            self.unlock_task()
        except Exception:  # pylint: disable=broad-except
            pass

        try:
            self.db.close()
        except Exception:  # pylint: disable=broad-except
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.close()

    #
    # Handle a successful chunk
    #

    def _write_chunk(self, chunk, start_time, end_time):
        assert self.source is not None

        start_time = str(int(start_time * 1000000))
        end_time = str(int(end_time * 1000000))

        key = start_time + '-' + end_time + '.' + self.save_format
        key = os.path.join(self.source, key)

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

    #
    # Acquire a task
    #

    def lock_task(self):
        with self.db.cursor() as cur:
            try:
                cur.execute('lock table ingest.jobs in exclusive mode;')

                cur.execute('''
                select
                    source_id
                from ingest.jobs
                where
                    not is_locked and
                    (? or error_count < ?)
                order by random()
                limit 1;
                ''', (self.chunk_error_threshold is None, self.chunk_error_threshold,))

                row = cur.fetchone()
                if row is None:
                    return None
                source_id = row[0]

                cur.execute('''
                update ingest.jobs
                set is_locked = true
                where
                    source_id = ?
                ''', (source_id,))

                return source_id
            except Exception:  # pylint: disable=broad-except
                cur.rollback()
                raise
            else:
                cur.commit()

    def unlock_task(self):
        '''
        Release the lock the worker holds on its source. This should be called
        when the worker exits to avoid orphaning the source.
        '''

        with self.db.cursor() as cur:
            try:
                cur.execute('lock table ingest.jobs in exclusive mode;')

                cur.execute('''
                update ingest.jobs
                set is_locked = false
                where
                    source_id = ?
                ''', (self.source_id,))
            except Exception:  # pylint: disable=broad-except
                cur.rollback()
                raise
            else:
                cur.commit()

    def delete_task(self):
        '''
        Delete a successfully completed source. Do NOT release the lock first,
        because then another worker may pick up the source to work on before we
        delete it.
        '''

        with self.db.cursor() as cur:
            try:
                cur.execute('lock table ingest.jobs in exclusive mode;')

                cur.execute('''
                delete
                from ingest.jobs
                where
                    source_id = ?
                ''', (self.source_id,))
            except Exception:  # pylint: disable=broad-except
                cur.rollback()
                raise
            else:
                cur.commit()

    def acquire_task(self):
        '''
        Acquire a task (i.e., source to work on) from the database, blocking
        until one is available. Configure the source ID and stream URL on the
        worker from the acquired task.
        '''

        # in a high-concurrency situation, spread out the load on the DB
        time.sleep(random.uniform(0, 2*self.poll_interval))

        # Use a spinlock; if there's nothing to work on, let's
        # wait around and keep checking if there is
        while True:
            res = self.lock_task()
            if res is not None:
                break

            logger.debug('Nothing to work on; spinning')
            time.sleep(self.poll_interval)
        self.source_id = res

        with self.db.cursor() as cur:
            cur.execute('''
            select
                name as source,
                stream_url,
                retry_on_close
            from data.source
            where
                source_id = ?;
            ''', (self.source_id,))

            res = cur.fetchone()
            self.source = res[0]
            self.stream_url = res[1]
            self.retry_on_close = res[2]

        return self

    #
    # Error handling
    #

    def get_stop_conditions(self):
        '''
        Check whether the worker has hit its stop conditions and should exit.
        The worker will exit if the source is no longer in the database, or if
        the source has failed too many times.
        '''

        with self.db.cursor() as cur:
            params = (
                self.source_id,
                self.source_id,
                self.chunk_error_threshold
            )

            cur.execute('''
            select
                not exists(
                    select
                        1
                    from ingest.jobs
                    where
                        source_id = ?
                ) as deleted,

                exists(
                    select
                        1
                    from ingest.jobs
                    where
                        source_id = ? and

                        -- the exists() will return false if ? is null
                        -- which happens if self.chunk_error_threshold is None
                        error_count >= ?
                ) as failed;
            ''', params)

            ret = cur.fetchone()
            cols = [col[0] for col in cur.description]

            return dict(zip(cols, ret))

    def stop_if_error(self):
        conds = self.get_stop_conditions()

        if conds['deleted']:
            msg = 'Job %s cancelled'
            vals = (self.source_id,)
            raise ex.JobCancelledException(msg % vals)

        if conds['failed']:
            msg = 'Job %s (%s) had too many failures'
            vals = (self.source_id, self.source)
            raise ex.TooManyFailuresException(msg % vals)

        return self

    def mark_failure(self):
        info = sys.exc_info()
        if info == (None, None, None):
            info = ''
        else:
            info = ''.join(traceback.format_exception(*info))

        with self.db.cursor() as cur:
            # concurency-safe because we have the lock on this source_id
            cur.execute('''
            update ingest.jobs
            set
                error_count = error_count + 1,
                last_error = ?
            where
                source_id = ?;
            ''', (info, self.source_id))

    def mark_success(self, url):
        logger.debug('Chunk queued')

        with self.db.cursor() as cur:
            cur.execute('''
            insert into transcribe.jobs
                (source_id, url)
            values
                (?, ?);
            ''', (self.source_id, url))

    #
    # Main loop
    #

    def stream_setup(self):
        if self.stream is not None:
            self.stream.close()

        while True:
            self.stop_if_error()

            try:
                self.stream = AudioStream(
                    url=self.stream_url,
                    save_format=self.save_format,
                    retry_on_close=self.retry_on_close,
                )

                self.iterator = self.stream.iter_time_chunks(self.chunk_size_seconds)
            except Exception:  # pylint: disable=broad-except
                logger.exception('Ingest failure')
                self.mark_failure()

                continue

            if self.iterator is not None:
                break

        return self

    # FIXME this implementation will - on error - start ingesting a
    # non-permanent stream again from the beginning by reopening the connection
    def run(self):
        '''
        Main worker entrypoint. Acquire a task, then ingest its stream.
        '''

        self.acquire_task()
        logger.info('Began ingest: %s from %s', self.source, self.stream_url)

        self.stream_setup()

        while True:
            self.stop_if_error()

            try:
                start_time = time.time()
                chunk = next(self.iterator)
                end_time = time.time()

                out_url = self._write_chunk(chunk, start_time, end_time)
            except StopIteration:
                logger.info('Source %s (%s) ran out of audio',
                            self.source_id, self.source)
                self.delete_task()  # successful completion

                break
            except Exception:  # pylint: disable=broad-except
                logger.exception('Ingest failure')
                self.mark_failure()

                # the exception exhausts our iterator (which is actually a
                # generator), so if we don't refresh the underlying stream
                # it'll raise StopIteration on the subsequent call to next()
                self.stream_setup()
            else:
                self.mark_success(out_url)

        return self
