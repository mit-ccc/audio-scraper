'''
Main worker class, running one per container.
'''

from typing import Optional

import sys
import time
import random
import logging

import pyodbc

from audio_chunk import AudioChunk
from transcriber import Transcriber


logger = logging.getLogger(__name__)


class TranscribeWorker:  # pylint: disable=too-many-instance-attributes
    '''
    The worker class that loops forever over: acquiring a task, transcribing
    it, writing the results to backing store, marking it complete, and then
    acquiring a new task.
    '''

    # pylint: disable-next=too-many-arguments
    def __init__(self, whisper_version: str = 'base',
                 compute_type: str = 'float32',
                 device: Optional[str] = None,
                 hf_token: Optional[str] = None,
                 dsn: str = 'Database',
                 chunk_error_threshold: Optional[int] = None,
                 poll_interval: int = 10,
                 remove_audio: bool = False):
        super().__init__()

        self.transcriber = Transcriber(
            whisper_version=whisper_version,
            device=device,
            compute_type=compute_type,
            hf_token=hf_token,
        )

        # No AWS creds - we assume they're in the environment
        self.dsn = dsn
        self.chunk_error_threshold = chunk_error_threshold
        self.poll_interval = poll_interval
        self.remove_audio = remove_audio

        self.db = pyodbc.connect(dsn=self.dsn, autocommit=False)

        self.chunk_id = None
        self.url = None
        self.lang = None
        self.source = None

    #
    # Worker properties and management
    #

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.close()

    def close(self):
        '''
        Tear down the worker's database connection.
        '''

        try:
            self.db.close()
        except Exception:  # pylint: disable=broad-except
            pass

    #
    # Acquire a task
    #

    def acquire_task(self, cur):
        '''
        Acquire a task (i.e., chunk to work on) from the database, blocking
        until one is available. Configure the chunk ID and URL on the
        worker from the acquired task.
        '''

        logger.debug('Began acquiring chunk')

        # Use a spinlock; if there's nothing to work on, let's wait around and
        # keep checking if there is
        while True:
            cur.execute('''
            select
                chunk_id
            from transcribe.jobs
            where
                ? or error_count < ?
            order by random()
            limit 1
            for update skip locked;
            ''', (self.chunk_error_threshold is None, self.chunk_error_threshold))

            row = cur.fetchone()
            chunk_id = row[0] if row is not None else None

            if chunk_id is not None:
                break

            # nothing to lock
            logger.debug('Nothing to work on; spinning')
            time.sleep(self.poll_interval)

        cur.execute('''
        select
            c.url,
            s.lang,
            s.name
        from transcribe.jobs c
            inner join data.source s using(source_id)
        where
            c.chunk_id = ?;
        ''', (chunk_id,))

        res = cur.fetchone()

        self.chunk_id = chunk_id
        self.url = res[0]
        self.lang = res[1]
        self.source = res[2]

        logger.info('Acquired chunk_id %s from %s', self.chunk_id, self.url)

        return self

    #
    # Error handling
    #

    def _error_count(self, cur):
        cur.execute('''
        select
            error_count
        from transcribe.jobs
        where
            chunk_id = ?;
        ''', (self.chunk_id,))

        return cur.fetchone()[0]

    def _mark_failure(self, cur):
        info = sys.exc_info()
        info = '' if info == (None, None, None) else str(info)

        cur.execute('''
        update transcribe.jobs
        set
            error_count = error_count + 1,
            last_error = ?
        where
            chunk_id = ?;
        ''', (info, self.chunk_id))

        return self

    def _mark_success(self, cur):
        logger.debug('Updating DB to remove chunk %s', self.url)
        cur.execute('''
        delete
        from transcribe.jobs
        where
            chunk_id = ?
        ''', (self.chunk_id,))
        logger.debug('Removed chunk %s from DB', self.url)

        return self

    #
    # Main loop
    #

    def run(self):
        '''
        Main worker entrypoint. Acquire a task, then ingest its stream.
        '''

        # in a high-concurrency situation, spread out the load on the DB
        time.sleep(random.uniform(0, 2*self.poll_interval))

        while True:
            with self.db.cursor() as cur:
                self.acquire_task(cur)

                try:
                    chunk = AudioChunk(
                        url=self.url,
                        source=self.source,
                        lang=self.lang
                    )

                    data = chunk.fetch()
                    results = self.transcriber.process(data, lang=chunk.lang)
                    chunk.write_results(results)

                    logger.info('Successfully transcribed %s', self.url)
                except Exception:  # pylint: disable=broad-except
                    if self._error_count(cur) < self.chunk_error_threshold:
                        msg = 'Chunk %s with url %s failed'
                    else:
                        msg = 'Chunk %s with url %s failed too many times ' + \
                              'and will no longer be retried'
                    logger.exception(msg, self.chunk_id, self.url)

                    self._mark_failure(cur)
                else:
                    self._mark_success(cur)

                    if self.remove_audio:
                        logger.debug('Removing chunk %s', self.url)
                        chunk.remove()
                        logger.debug('Removed chunk %s', self.url)

        return self
