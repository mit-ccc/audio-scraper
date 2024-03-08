from typing import Optional

import gc
import sys
import time
import random
import logging

import pyodbc

from chunk import Chunk
from transcriber import Transcriber


logger = logging.getLogger(__name__)


class TranscribeWorker:  # pylint: disable=too-many-instance-attributes
    # pylint: disable-next=too-many-arguments
    def __init__(self, whisper_version: str = 'base',
                 compute_type: str = 'float32',
                 device: Optional[str] = None,
                 hf_token: Optional[str] = None,
                 dsn: str = 'Database',
                 chunk_error_behavior: str = 'ignore',
                 chunk_error_threshold: int = 10,
                 poll_interval: int = 10):
        super().__init__()

        self.transcriber = Transcriber(
            whisper_version=whisper_version,
            device=device,
            compute_type=compute_type,
            hf_token=hf_token,
        )

        # No AWS creds - we assume they're in the environment
        self.dsn = dsn
        self.chunk_error_behavior = chunk_error_behavior
        self.chunk_error_threshold = chunk_error_threshold
        self.poll_interval = poll_interval

        self.db = pyodbc.connect(dsn=self.dsn)
        self.db.autocommit = True

        self.chunk_id = None
        self.url = None
        self.lang = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.close()

    def close(self):
        '''
        Tear down the worker's persistent resources: the database connection and
        the advisory lock on the chunk ID.
        '''

        try:
            self.release_lock()
        except Exception:  # pylint: disable=broad-except
            pass

        try:
            self.db.close()
        except Exception:  # pylint: disable=broad-except
            pass

        self.chunk_id = None
        self.url = None
        self.lang = None

    def lock_task(self):
        '''
        Lock a task in the database via an advisory lock on the chunk ID, in
        order to prevent multiple workers from working on the same chunk at
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
                    pg_try_advisory_lock((j).chunk_id) as locked
                from
                (
                    select
                        j
                    from app.chunks j
                    where
                        ? or
                        j.error_count < ?
                    order by chunk_id
                    limit 1
                ) as t1

                union all

                (
                    select
                        (j).*,
                        pg_try_advisory_lock((j).chunk_id) as locked
                    from
                    (
                        select
                        (
                            select
                                j
                            from app.chunks j
                            where
                                j.chunk_id > job_locks.chunk_id and
                                (
                                    ? or
                                    j.error_count < ?
                                )
                            order by chunk_id
                            limit 1
                        ) as j
                        from job_locks
                        where
                            job_locks.chunk_id is not null
                        limit 1
                    ) as t1
                )
            )
            select
                chunk_id
            from job_locks
            where
                locked
            limit 1;
            ''', params)

            res = cur.fetchall()
            return res[0][0] if len(res) > 0 else None

    def release_lock(self):
        '''
        Release the lock the worker holds on its chunk. This should be called
        when the worker exits to avoid orphaning the chunk.
        '''

        with self.db.cursor() as cur:
            cur.execute('''
            select
                pg_advisory_unlock(?);
            ''', (self.chunk_id,))

            return cur.fetchone()[0]

    def acquire_task(self):
        '''
        Acquire a task (i.e., chunk to work on) from the database, blocking
        until one is available. Configure the chunk ID and URL on the
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
        self.chunk_id = res

        with self.db.cursor() as cur:
            cur.execute('''
            select
                c.url,
                s.lang
            from app.chunks c
                inner join data.station s using(station_id)
            where
                c.chunk_id = ?;
            ''', (self.chunk_id,))

            res = cur.fetchone()
            self.url = res[0]
            self.lang = res[1]

        return self

    def run(self):
        '''
        Main worker entrypoint. Acquire a task, then ingest its stream.
        '''

        while True:
            self.acquire_task()

            msg = "Began processing chunk_id %s from %s"
            logger.info(msg, self.chunk_id, self.url)

            try:
                chunk = Chunk(url=self.url, lang=self.lang)

                data = chunk.fetch()
                results = self.transcriber.process(data, lang=chunk.lang)
                chunk.write_results(results)

                logger.info('Successfully transcribed %s', self.url)
            except Exception:  # pylint: disable=broad-except
                with self.db.cursor() as cur:
                    # log the failure; this is concurency-safe because
                    # we have the lock on this chunk_id
                    cur.execute('''
                    update app.chunks
                    set
                        error_count = error_count + 1,
                        last_error = ?
                    where
                        chunk_id = ?;
                    ''', (str(sys.exc_info()), self.chunk_id))

                if self.chunk_error_behavior == 'exit':
                    raise

                logger.exception('Chunk failed; ignoring')
            else:
                # we no longer need this DB entry
                with self.db.cursor() as cur:
                    cur.execute('''
                    delete
                    from app.chunks
                    where
                        chunk_id = ?
                    ''', (self.chunk_id,))
            finally:
                gc.collect()

        return self
