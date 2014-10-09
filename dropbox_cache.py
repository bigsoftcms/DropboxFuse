#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import dropbox
from dropbox_exceptions import FileNotFoundError


class Cache(object):
    def __init__(self, client):
        self.cache = dict()
        self.client = client

    def get_cache(self):
        return self.cache

    def get_entry(self, path, create=True):
        cache_entry = self.cache.get(path)
        if cache_entry is None:
            print 'cache miss', path
            if create is False:
                return None

            cache_entry = CacheEntry(path, self.client).fetch()
            self.set_entry(path, cache_entry)

            if cache_entry.metadata['is_dir'] is True:
                print 'cache: its a dir, recursing'
                for content in cache_entry.metadata['contents']:
                    path = content['path']
                    print 'cache: adding', path
                    self.set_entry(path, CacheEntry(path, self.client, metadata=content))
        else:
            print 'cache hit', path
        return cache_entry

    def set_entry(self, path, entry):
        assert isinstance(entry, CacheEntry)
        assert isinstance(path, (str, unicode))
        self.cache[path] = entry

    def remove_entry(self, path):
        assert path in self.cache, 'path is not in cache'
        del self.cache[path]

    def set_parent_dirty(self, cache_entry):
        path = cache_entry.path
        path = os.path.dirname(path)

        try:
            cache_entry = self.get_entry(path)
            cache_entry.dirty = True
        except FileNotFoundError as e:
            print 'parent not found, not setting dirty flag', e


class CacheEntry(object):
    def __init__(self, path, client, metadata=None, data=None, uploader=None, downloader=None):
        self.path = path
        self.client = client
        self._metadata = dict() if metadata is None else metadata
        self._data = data
        self._uploader = uploader
        self._downloader = downloader
        self._dirty = False

    def fetch(self):
        # will work only if is_dir is True, else None
        prev_hash = self._metadata.get('hash')
        try:
            metadata = self.client.metadata(self.path, hash=prev_hash)
            if 'is_deleted' in metadata and metadata['is_deleted'] is True:
                raise FileNotFoundError('%s is deleted' % self.path)
        except dropbox.rest.ErrorResponse as e:
            print e.status, e.reason
            if 404 == e.status:
                # not found
                raise FileNotFoundError(str(e))
            elif 304 == e.status:
                # not modified
                self._dirty = False
                return self
            else:
                raise

        self._metadata = metadata
        self._dirty = False
        return self

    @property
    def dirty(self):
        return self._dirty

    @dirty.setter
    def dirty(self, value):
        assert isinstance(value, bool)
        self._dirty = value

    @property
    def uploader(self):
        return self._uploader

    @uploader.setter
    def uploader(self, value):
        self._uploader = value

    @property
    def metadata(self):
        if self.dirty is True:
            self.fetch()
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        assert isinstance(value, dict)
        self._metadata = value

    @property
    def data(self):
        if self.dirty is True:
            self._data = None
        return self._data

    @data.setter
    def data(self, value):
        assert isinstance(value, str)
        self._data = value

    @property
    def downloader(self):
        return self._downloader

    @downloader.setter
    def downloader(self, value):
        self._downloader = value

    def __str__(self):
        return '<cache entry for path %s: METADATA=%s DATA=%s' % (self.path,
                                                                  self.metadata is not None,
                                                                  self.data is not None)

