from typing import Optional

import io
import os
import json
import hashlib
import logging

from urllib.parse import urlparse
from functools import cached_property

import boto3


logger = logging.getLogger(__name__)


class Chunk:
    '''
    Encapsulates an audio file for processing
    '''

    def __init__(self, url: str, station: str,
                 lang: Optional[str] = None,
                 cache_dir: Optional[str] = None):
        super().__init__()

        self.url = url
        self.station = station
        self.lang = lang

        if self.storage_mode == 's3':
            self._client = boto3.client('s3')
            self.cache_dir = cache_dir
        else:
            self._client = None
            self.cache_dir = None

            if cache_dir is not None:
                logger.warning('Ignoring cache_dir with local files')

    @cached_property
    def _url_parsed(self):
        return urlparse(self.url)

    @cached_property
    def storage_mode(self):
        mode = self._url_parsed.scheme.lower()

        if mode not in ('s3', 'file'):
            raise ValueError('Bad storage URL format')

        if mode == 'file' and self._url_parsed.netloc != '':
            raise ValueError('Bad file URL - has netloc')

        return mode

    def _read_data_s3(self):
        bucket = self._url_parsed.netloc

        key = self._url_parsed.path
        if key.startswith('/'):
            key = key[1:]

        resp = self._client.get_object(Bucket=bucket, Key=key)

        return resp['Body'].read()

    def _read_data_local(self):
        with open(self._url_parsed.path, 'rb') as fobj:
            return fobj.read()

    def _read_data(self):
        if self.storage_mode == 's3':
            return self._read_data_s3()

        return self._read_data_local()

    @property
    def _cache_path(self):
        assert self.storage_mode == 's3'

        bucket = self._url_parsed.netloc

        key = self._url_parsed.path
        if key.startswith('/'):
            key = key[1:]

        val = (bucket, key)
        fname = hashlib.sha1(str(val).encode('utf-8')).hexdigest()

        return os.path.join(self.cache_dir, fname)

    @property
    def _is_cached(self):
        if not self.cache_dir:
            return False

        return os.path.exists(self._cache_path)

    def _write_to_cache(self, data):
        assert not self._is_cached
        assert self.cache_dir is not None

        with open(self._cache_path, 'wb') as fobj:
            fobj.write(data)

    def _read_from_cache(self):
        assert self._is_cached

        with open(self._cache_path, 'rb') as fobj:
            return fobj.read()

    def _write_results_s3(self, results):
        bucket = self._url_parsed.netloc

        key = self._url_parsed.path
        if key.startswith('/'):
            key = key[1:]

        with io.BytesIO(results.encode('utf-8')) as fobj:
            self._client.upload_fileobj(fobj, bucket, key + '.json')

    def _write_results_local(self, results):
        with open(self._url_parsed.path + '.json', 'wt', encoding='utf-8') as fobj:
            fobj.write(results)

    @cached_property
    def times(self):
        name = os.path.basename(self._url_parsed.path).split('.')[0]
        start, end = name.split('-')

        return {
            'start': float(int(start)) / 1000000,
            'end': float(int(end)) / 1000000,
        }

    def write_results(self, results):
        ret = json.dumps({
            'url': self.url,
            'station': self.station,
            'lang': self.lang,
            'ingest_start_time': self.times['start'],
            'ingest_end_time': self.times['end'],
            'results': results,
        })

        if self.storage_mode == 's3':
            self._write_results_s3(ret)
        else:
            self._write_results_local(ret)

    def fetch(self):
        if self._is_cached:
            return self._read_from_cache()

        data = self._read_data()

        if self.cache_dir is not None:
            self._write_to_cache(data)

        return data

    def __len__(self):
        return 1

    def __iter__(self):
        yield {
            'url': self.url,
            'data': self.fetch(),
        }
