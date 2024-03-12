'''
This module contains the Pool class, which is responsible for spawning
and managing the ingest workers.
'''

import time
import logging
import multiprocessing as mp

import exceptions as ex
from worker import payload


logger = logging.getLogger(__name__)


class Pool:
    '''
    This class is responsible for spawning and managing the ingest workers.
    It's intended to be used as a context manager. If workers exit, they are
    respawned.
    '''

    def __init__(self, n_tasks=10, poll_interval=10, **kwargs):
        super().__init__()

        self.n_tasks = n_tasks
        self.poll_interval = poll_interval
        self.worker_args = dict(kwargs, poll_interval=self.poll_interval)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        pass

    def run(self):
        '''
        Run the pool, respawning workers if they exit.
        '''

        results = []
        with mp.Pool(self.n_tasks, maxtasksperchild=1) as pool:
            logger.debug('Spawning initial tasks')
            for _ in range(0, self.n_tasks):
                results += [pool.apply_async(payload, (self.worker_args,))]
            logger.debug('Spawned initial tasks')

            while True:
                time.sleep(self.poll_interval)

                for (ind, res) in enumerate(results):
                    if res.ready():
                        try:
                            res.get()
                        except Exception:  # pylint: disable=broad-except
                            logger.exception("Worker exited")

                        # Respawn the task after exit
                        results[ind] = pool.apply_async(payload, (self.worker_args,))
