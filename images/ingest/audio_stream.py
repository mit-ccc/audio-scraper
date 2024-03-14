'''
This module defines the AudioStream class, which represents a single
audio stream. It also contains the MediaIterator class, which is used
to iterate over the stream's contents. Both classes cover a variety of stream
types, including direct audio streams, playlists, and scraped web pages.
'''

import os
import io
import re
import json
import logging
import mimetypes as mt
import itertools as it
import subprocess as sp
import configparser as cp
import urllib.parse as urlparse

from enum import Enum
from abc import ABC, abstractmethod

import bs4
import m3u8
import ffmpeg
import requests as rq

from pydub import AudioSegment
from fake_useragent import UserAgent

import audio_utils as au
import exceptions as ex


logger = logging.getLogger(__name__)


class DirectMediaType(Enum):
    '''
    This class enumerates the media types that can be directly streamed.
    '''

    WAV = 'wav'
    MP3 = 'mp3'
    FLV = 'flv'
    OGG = 'ogg'
    AAC = 'aac'
    WMA = 'wma'
    RAW = 'raw'
    PCM = 'pcm'
    FLAC = 'flac'
    WEBM = 'webm'


class PlaylistMediaType(Enum):
    '''
    This class enumerates the media types that require parsing a playlist file
    to download.
    '''

    PLS = 'pls'
    M3U = 'm3u'
    M3U8 = 'm3u8'
    ASX = 'asx'


class WebscrapeMediaType(Enum):
    '''
    This class enumerates the media types that require scraping a web page to
    download.
    '''

    IHEART = 'iheart'


# AudioStream represents 'what to do' and this class
# represents 'how to do it'. Fetching and parsing logic
# is here; chunk sizes, retry configuration, the actual
# URL to fetch from, etc, are in AudioStream.
class MediaIterator(ABC):
    '''
    This class is used to iterate over the contents of a stream. It's an
    abstract base class which children need to complete with logic in the
    _refresh method for fetching and parsing the stream's contents.
    '''

    def __init__(self, **kwargs):
        try:
            self.stream = kwargs.pop('stream')
        except KeyError as exc:
            raise ValueError("Must pass stream object") from exc

        self.timeout = kwargs.pop('timeout', 10)

        super().__init__(**kwargs)

        # Must be defined in subclasses by the _refresh method
        self.content = NotImplemented

        self._refresh()

        self.retry_error_cnt = 0

    @abstractmethod
    def _refresh(self):
        raise NotImplementedError("Subclasses must define _refresh")

    def __iter__(self):
        return self

    def __next__(self):
        while self.retry_error_cnt <= self.stream.retry_error_max:
            try:
                return next(self.content)
            except rq.exceptions.RequestException:
                logger.exception("Failed to get next chunk")
                self.retry_error_cnt += 1

                if self.retry_error_cnt <= self.stream.retry_error_max:
                    self._refresh()
                else:
                    raise
            except StopIteration:
                if not self.stream.retry_on_close:
                    raise

                self._refresh()
                continue


class DirectStreamIterator(MediaIterator):
    '''
    This class is used to iterate over the contents of a direct audio stream,
    rather than a playlist or website that requires scraping.
    '''

    def _refresh(self):
        self.conn = self.stream._get(stream=True)

        chunk_size = self.stream.raw_chunk_size_bytes
        content = self.conn.iter_content(chunk_size=chunk_size)

        self.content = (
            {'media_type': self.stream.media_type, 'data': chunk}
            for chunk in content
        )


class PlaylistIterator(MediaIterator):
    '''
    This class is used to iterate over the contents of a playlist. It's an
    abstract class that children need to complete with logic for identifying
    the component URLs in the playlist. This is because of the several playlist
    formats we want to handle -- pls, asx, m3u, and possibly others in the
    future.
    '''

    @abstractmethod
    def _get_component_urls(self, txt):
        msg = "Subclasses must implement _get_component_urls"
        raise NotImplementedError(msg)

    def _refresh(self):
        # get the URLs
        txt = self.stream._fetch_url_stream_safe(max_size=2**16)
        comps = self._get_component_urls(txt.decode())

        # make streams out of them
        args = dict(self.stream.args, unknown_formats='direct')

        # Don't propagate this setting down to children, for this class
        # only. If we do propagate it, playlists with multiple segments
        # won't read correctly: we'll be stuck on the first segment forever
        # after it closes, reopening and repeatedly reading it.
        args['retry_on_close'] = False

        self.content = it.chain(*[
            AudioStream(**dict(args, url=x))
            for x in comps
        ])


class AsxIterator(PlaylistIterator):
    '''
    This class is used to iterate over the contents of an ASX playlist.
    '''

    def _get_component_urls(self, txt):
        soup = bs4.BeautifulSoup(txt, features='lxml')
        hrefs = [x['href'] for x in soup.find_all('ref')]

        return hrefs


class PlsIterator(PlaylistIterator):
    '''
    This class is used to iterate over the contents of a Pls playlist.
    '''

    def _get_component_urls(self, txt):
        prs = cp.ConfigParser(interpolation=None)
        prs.read_string(txt)

        # Section names are case sensitive by default, so we
        # need to find the case that 'playlist' actually has
        sections = prs.sections()
        matches = [re.search('playlist', x, re.I) for x in sections]
        matched = [x is not None for x in matches]
        ind = matched.index(True)
        key = sections[ind]

        keys = [x for x in prs[key].keys() if x[0:4] == 'file']
        urls = [prs['playlist'][x] for x in keys]

        return urls


class M3uIterator(PlaylistIterator):
    '''
    This class is used to iterate over the contents of an M3U playlist.
    '''

    def _get_component_urls(self, txt, i=0):
        if i >= 10:
            raise ex.IngestException("m3u playlists nested too deeply")

        pls = m3u8.loads(txt)

        if not pls.is_variant:
            if len(pls.segments) == 0:
                # seems to be an m3u8 package bug we'll try to work around
                segs = [line.strip() for line in txt.split('\n') if line.strip() != '']
                for seg in segs:
                    pls.add_segment(m3u8.Segment(uri=seg, base_uri=pls.base_uri))

            urls = [x.uri for x in pls.segments]
        else:
            urls = []
            for subpls in pls.playlists:
                subtxt = self.stream._fetch_url_stream_safe(subpls.uri)
                urls += self._get_component_urls(subtxt.decode(), i=i+1)

        return urls


class WebscrapeIterator(MediaIterator):
    '''
    This class is used to iterate over the contents of a web page that contains
    audio media. We identify the media URL(s) by scraping the page. It's an
    abstract class that child classes need to complete with page- or
    site-specific logic for identifying the media URL(s). The identified URLs
    cannot be other web pages for scraping but can be playlists or direct audio
    streams.
    '''

    @abstractmethod
    def _webscrape_extract_media_url(self, txt):
        msg = 'Subclasses must implement _webscrape_extract_media_url'
        raise NotImplementedError(msg)

    def _refresh(self):
        txt = self.stream._fetch_url_stream_safe(max_size=2**20)
        url = self._webscrape_extract_media_url(txt)

        # we'll just proxy for an iterator on the real stream
        args = dict(url=url, unknown_formats='direct', **self.stream.args)
        stream = AudioStream(**args)
        if stream.media_type in WebscrapeMediaType:
            raise ex.IngestException('WebscrapeIterators may not be nested')
        self.content = stream._iterator

    def _url_filter_extension(self, urls, direct=True, playlist=True):
        assert len(urls) > 0
        assert direct or playlist

        fmts = []
        fmts += [x.value for x in DirectMediaType] if direct else []
        fmts += [x.value for x in PlaylistMediaType] if playlist else []

        ret = None
        for fmt in fmts:
            matches = [x for x in urls if re.search(fmt, x)]
            if len(matches) > 0:
                ret = matches[0]

        return ret

    def _url_filter_ffprobe(self, urls):
        fmts = [x.value for x in DirectMediaType]

        for url in urls:
            chunk = self.stream._fetch_probe_chunk()
            ext = au.probe_format(chunk)

            if ext in fmts:
                return url

        return None

    def _url_filter(self, urls, direct=True, playlist=True):
        try:
            ret = self._url_filter_extension(urls, direct=direct,
                                             playlist=playlist)

            if ret is None:
                ret = self._url_filter_ffprobe(urls)

            assert ret is not None

            return ret
        except AssertionError as exc:
            msg = 'No usable streams in {0}'
            vals = (self.stream.url,)
            raise ex.IngestException(msg % vals) from exc


class IHeartIterator(WebscrapeIterator):
    '''
    This class is used to iterate over the contents of an iHeartRadio page.
    '''

    def _webscrape_extract_media_url(self, txt):
        # There's a chunk of json in the page with our URLs in it
        soup = bs4.BeautifulSoup(txt, features='lxml')
        script = soup.find_all('script', id='initialState')[0].text

        # Get the specific piece of json with the urls of interest
        stations = json.loads(script)['live']['stations']
        key = list(stations.keys())[0]
        streams = stations[key]['streams']

        if 'secure_shoutcast_stream' in streams.keys():
            return streams['secure_shoutcast_stream']
        elif 'shoutcast_stream' in streams.keys():
            return streams['shoutcast_stream']
        else:
            # playlists don't seem to work right and just loop over the same
            # short piece of audio. possibly they have to be refreshed by
            # in-page JS, which is way way more trouble than it's worth when a
            # direct stream is available instead.
            return self._url_filter(streams.values(), playlist=False)


class MediaUrl:
    '''
    This class represents a URL to a media file. It's used to encapsulate
    information about the URL, such as the file extension, and to provide
    methods for downloading the file.
    '''

    def __init__(self, **kwargs):
        try:
            url = kwargs.pop('url')
        except KeyError as exc:
            raise ValueError("Must provide url") from exc

        autodetect = kwargs.pop('autodetect', True)

        super().__init__(**kwargs)

        self.url = url
        self.autodetect = autodetect

        self.session = rq.Session()
        self.session.headers.update({'User-Agent': UserAgent().random})

        self.timeout = kwargs.pop('timeout', 10)

        try:
            self._ext = self._detect_ext()
        except Exception:  # pylint: disable=broad-except
            msg = 'Encountered exception while guessing stream type'
            logger.warning(msg)

            self._ext = ''

    def _detect_ext(self):
        pa_ext = self._parse_ext()

        if pa_ext != '' or not self.autodetect:
            return pa_ext

        au_ext = self._autodetect_ext_ffprobe()
        if au_ext is not None:
            return au_ext

        mt_ext = self._autodetect_ext_mime_type()
        if mt_ext is not None:
            return mt_ext

        return ''

    def _parse_ext(self):
        pth = urlparse.urlparse(self.url).path
        ext = os.path.splitext(os.path.basename(pth))[1][1:]

        return ext

    def _autodetect_ext_ffprobe(self):
        chunk = self._fetch_probe_chunk()

        return au.probe_format(chunk)

    def _autodetect_ext_mime_type(self):
        with self._get(stream=True) as resp:
            mimetype = resp.headers.get('Content-Type')

        if mimetype is None:
            return ''

        if ';' in mimetype:
            mimetype = mimetype.split(';')[0]

        ext = mt.guess_extension(mimetype)
        return '' if ext is None else ext[1:]

    @property
    def _is_iheart(self):
        '''
        This property returns whether the URL is an iHeartRadio URL.
        '''

        parsed = urlparse.urlparse(self.url)
        netloc, pth = parsed.netloc, parsed.path

        return (netloc == 'www.iheart.com' and pth[0:5] == '/live')

    @property
    def media_type(self):
        '''
        This property returns the media type of the URL.
        '''

        ext = 'iheart' if self._is_iheart else self._ext
        ext = ext.lower()

        values_direct = {x.value for x in DirectMediaType}
        values_playlist = {x.value for x in PlaylistMediaType}
        values_webscrape = {x.value for x in WebscrapeMediaType}

        if ext in values_direct:
            return DirectMediaType(ext)

        if ext in values_playlist:
            return PlaylistMediaType(ext)

        if ext in values_webscrape:
            return WebscrapeMediaType(ext)

        return None

    @backoff.on_exception(backoff.expo, rq.exceptions.RequestException,
                          max_tries=5, max_time=600)
    def _get(self, url=None, **kwargs):
        if url is None:
            url = self.url

        resp = self.session.get(url, timeout=self.timeout, **kwargs)

        if not resp.ok:
            resp.raise_for_status()

        return resp

    def _fetch_probe_chunk(self, url=None, chunk_size=2**17):
        with self._get(url, stream=True) as resp:
            content = resp.iter_content(chunk_size=chunk_size)
            chunk = next(iter(content))

        return chunk

    def _fetch_url_stream_safe(self, url=None, max_size=2**20):
        # This method is called by subclasses which expect self.stream.url
        # to be a short text file (playlist or web page), but need to be
        # robust to the possibility of server misconfiguration actually
        # returning an audio stream. Trying to fetch the whole thing would
        # cause the process to hang and eventually lead to out-of-memory
        # errors. Instead we'll fetch it as a stream and return only the
        # first max_size decoded bytes.
        with self._get(url, stream=True) as resp:
            txt = resp.raw.read(max_size + 1, decode_content=True)

        if len(txt) > max_size:
            msg = 'Too large a response - is it actually an audio file?'
            raise ValueError(msg)

        return txt


class AudioStream(MediaUrl):
    '''
    This class represents a stream of audio data. It's used to encapsulate
    information about the stream as defined in the parent MediaUrl class, and
    to provide methods for iterating over the streamed audio in chunks.
    '''

    def __init__(self, **kwargs):
        # for use in webscrape iterators' descendant streams
        self.args = dict(kwargs)

        self.retry_error_max = kwargs.pop('retry_error_max', 0)
        self.unknown_formats = kwargs.pop('unknown_formats', 'error')
        self.retry_on_close = kwargs.pop('retry_on_close', False)
        self.save_format = kwargs.pop('save_format', 'wav')

        # How large a block should we read from the underlying audio file
        # stream? This is a low-level detail separate from the bytes or seconds
        # chunk sizes used in the iter_*_chunks methods.
        self.raw_chunk_size_bytes = kwargs.pop('raw_chunk_size_bytes', 2**16)

        super().__init__(**kwargs)

        try:
            assert self.unknown_formats in ('direct', 'error')
        except AssertionError as exc:
            msg = "unknown_formats must be 'direct' or 'error'"
            raise ValueError(msg) from exc

        cls = self._get_iterator()
        if cls is not None:
            self._iterator = cls(stream=self)
        else:
            msg = f'No iterator found for {self.url} with _ext = {self._ext}'
            raise NotImplementedError(msg)

        logger.debug('AudioStream up for %s; _ext = %s; iterator class %s',
                     self.url, self._ext, type(self._iterator).__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_traceback):
        self.close()

    def __iter__(self):
        return self._iterator

    def iter_time_chunks(self, chunk_size_seconds=30):
        chunk_size = chunk_size_seconds * 1000  # indexing is in milliseconds
        buf = AudioSegment.empty()

        for chunk in self:
            assert (
                chunk['media_type'] is None or
                chunk['media_type'] in DirectMediaType
            )

            logger.debug('Chunk with format %s of size %s',
                         chunk['media_type'], len(chunk['data']))

            ffmpeg_params = ['-analyzeduration', '2147483647',
                             '-probesize', '2147483647']

            try:
                ar = au.discover_sample_rate(chunk['data'])
                ffmpeg_params += ['-ar', str(ar)]
            except ex.IngestException:
                logger.warning('Could not discover sample rate')

            with io.BytesIO(chunk['data']) as obj:
                mtype = chunk['media_type']
                mtype = mtype.value if mtype is not None else None

                buf += AudioSegment.from_file(
                    obj, format=mtype,
                    parameters=ffmpeg_params
                )

            while len(buf) >= chunk_size:
                out, buf = buf[:chunk_size], buf[chunk_size:]

                with io.BytesIO() as obj:
                    out.export(obj, format=self.save_format)
                    yield obj.getvalue()

        if len(buf) > 0:
            with io.BytesIO() as obj:
                buf.export(obj, format=self.save_format)
                yield obj.getvalue()

    def iter_byte_chunks(self, chunk_size=2**20):
        # Just ignore the format information in c['media_type']
        content = it.chain.from_iterable(c['data'] for c in self._iterator)

        while True:
            chunk = bytes(it.islice(content, chunk_size))
            if not chunk:
                break
            yield chunk

    def close(self):
        '''
        Close the underlying network connection.
        '''

        try:
            self.session.close()
        except Exception:  # pylint: disable=broad-except
            pass

    def _get_iterator(self):
        if self.media_type is None:
            ret = None
        elif self.media_type == PlaylistMediaType.ASX:
            ret = AsxIterator
        elif self.media_type == PlaylistMediaType.PLS:
            ret = PlsIterator
        elif self.media_type == PlaylistMediaType.M3U:
            ret = M3uIterator
        elif self.media_type == PlaylistMediaType.M3U8:
            ret = M3uIterator
        elif self.media_type == WebscrapeMediaType.IHEART:
            ret = IHeartIterator
        elif self.media_type in DirectMediaType:
            ret = DirectStreamIterator
        elif self.unknown_formats == 'direct':
            # Fall back to trying to stream it
            ret = DirectStreamIterator
        else:
            ret = None

        return ret
