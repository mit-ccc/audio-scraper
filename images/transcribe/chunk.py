from typing import Dict, Any, Union, Optional

import io
import os
import json
import hashlib
import logging
import datetime
import itertools as it

import boto3

import utils as ut


logger = logging.getLogger(__name__)


class Chunk:
    '''
    Encapsulates an S3 audio file for processing
    '''

    def __init__(self, bucket: str, key: str, cache_dir: Optional[str] = None):
        super().__init__()

        self.bucket = bucket
        self.key = key
        self.cache_dir = cache_dir

        self._session = boto3.Session()
        self._client = self._session.client('s3')

    @classmethod
    def from_s3_url(cls, url: str, cache_dir: Optional[str] = None):
        bucket, key = ut.parse_s3_url(url)

        return cls(bucket=bucket, key=key, cache_dir=cache_dir)

    @property
    def s3_url(self):
        return 's3://' + self.bucket + '/' + self.key

    def _s3_fetch(self):
        resp = self.client.get_object(
            Bucket=self.bucket,
            Key=self.key,
        )

        return resp['Body'].read()

    def fetch(self):
        if self._is_cached:
            return self._read_from_cache()

        data = self._s3_fetch()

        if self.cache_dir is not None:
            self._write_to_cache(data)

        return data

    @property
    def _cache_path(self):
        val = (self.bucket, self.key)
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

    def process(self, transcriber):
        data = self.fetch()
        ret = transcriber.process(data)

        ret['bucket'] = self.bucket
        ret['key'] = self.key

        out = json.dumps(ret)
        with io.BytesIO(out) as fobj:
            self._client.upload_fileobj(fobj, self.bucket, self.key + '.json')

    def __len__(self):
        return 1

    def __iter__(self):
        yield {
            'bucket': self.bucket,
            'key': self.key,
            'data': self.fetch(),
        }


# class ChunkSet:
#     '''
#     Assemble all chunks posted in the last 24 hours not yet processed.
#     '''
#
#     def __init__(self, bucket: str, prefix: Optional[str] = None,
#                  cache_dir: Optional[str] = None,
#                  since: Optional[datetime.datetime] = None):
#         super().__init__()
#
#         self.bucket = bucket
#         self.prefix = prefix
#         self.cache_dir = cache_dir
#
#         if since is not None:
#             self.since = since
#         else:
#             # default to 24 hours ago
#             now = datetime.datetime.now(datetime.timezone.utc)
#             self.since = now - datetime.timedelta(days=1)
#
#         if self.cache_dir:
#             os.makedirs(self.cache_dir, exist_ok=True)
#
#         self._session = boto3.Session()
#         self._client = self._session.client('s3')
#
#         self._chunks = self._get_unprocessed_chunks()
#
#     def _get_recent_files(self):
#         objects = self._client.list_objects_v2(
#             Bucket=self.bucket,
#             Prefix=self.prefix
#         ).get('Contents', [])
#
#         recent_objects = [
#             obj
#             for obj in objects
#             if obj['LastModified'] > self.since
#         ]
#
#         return [obj['Key'] for obj in recent_objects]
#
#     def _get_unprocessed_chunks(self):
#         files = self._get_recent_files()
#
#         chunks = [f for f in files if f.endswith('.raw')]
#         unprocessed = [c for c in chunks if c + '.json' not in files]
#
#         return [
#             Chunk(bucket=self.bucket, key=c, client=self._client,
#                   cache_dir=self.cache_dir)
#
#             for c in unprocessed
#         ]
#
#     def __len__(self):
#         return len(self._chunks)
#
#     def __iter__(self):
#         yield from it.chain(*self._chunks)
