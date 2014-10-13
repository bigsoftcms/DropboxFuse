#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import dropbox
from dropbox_exceptions import FileNotFoundError
from dropbox_logger import DropboxLogDummy


class CacheManager(object):
    cache_list = dict()

    @staticmethod
    def set_cache(name, cache):
        CacheManager.cache_list[name] = cache

    @staticmethod
    def get_cache(name):
        return CacheManager.cache_list[name]


class CacheBase(object):
    def __init__(self, client, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.cache = dict()
        self.client = client

    def get_cache(self):
        return self.cache

    def get_entry(self, path):
        pass

    def set_entry(self, path, entry):
        self.logger.debug('setting entry for %s', path)
        assert isinstance(path, (str, unicode))
        self.cache[path] = entry

    def remove_entry(self, path):
        self.logger.info('removing entry for %s', path)
        assert path in self.cache, 'path is not in cache'
        del self.cache[path]


class MetadataCache(CacheBase):
    def get_entry(self, path, create=True):
        super(MetadataCache, self).get_entry(path)
        cache_entry = self.cache.get(path)
        if cache_entry is None:
            self.logger.info('cache miss %s', path)
            if create is False:
                return None

            cache_entry = MetadataCacheEntry(path, self.client).fetch()
            self.set_entry(path, cache_entry)

            if cache_entry.metadata['is_dir'] is True:
                self.logger.info('its a dir --> precaching sub-entries')
                for content in cache_entry.metadata['contents']:
                    path = content['path']
                    self.logger.info('adding %s', path)
                    self.set_entry(path, MetadataCacheEntry(path, self.client, metadata=content))
        else:
            self.logger.info('cache hit %s', path)
        return cache_entry

    def set_entry(self, path, entry):
        assert isinstance(entry, MetadataCacheEntry)
        super(MetadataCache, self).set_entry(path, entry)

    def set_parent_dirty(self, cache_entry):
        self.logger.info('%s: setting parent entry as dirty', cache_entry.path)
        path = cache_entry.path
        path = os.path.dirname(path)

        try:
            cache_entry = self.get_entry(path)
            cache_entry.dirty = True
        except FileNotFoundError as e:
            self.logger.error('parent not found, not setting dirty flag %s', e)


class CacheEntryBase(object):
    def __init__(self, path, client, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.path = path
        self.client = client
        self._dirty = False
        self._is_cached = False

    def fetch(self):
        self._dirty = False
        return self

    @property
    def dirty(self):
        return self._dirty

    @dirty.setter
    def dirty(self, value):
        assert isinstance(value, bool)
        self.logger.info('%s->dirty = %s', self.path, str(value))
        self._dirty = value

    @property
    def is_cached(self):
        return self._is_cached

    @is_cached.setter
    def is_cached(self, value):
        assert isinstance(value, bool)
        self._is_cached = value

    def __str__(self):
        return '<cache entry for path %s' % (self.path, )


class MetadataCacheEntry(CacheEntryBase):
    def __init__(self, path, client, metadata=None, uploader=None):
        self._metadata = dict() if metadata is None else metadata
        self._uploader = uploader
        super(MetadataCacheEntry, self).__init__(path, client)

    def fetch(self):
        # will work only if is_dir is True, else None
        prev_hash = self._metadata.get('hash')
        try:
            metadata = self.client.metadata(self.path, hash=prev_hash)
            if 'is_deleted' in metadata and metadata['is_deleted'] is True:
                self.logger.error('404: %s already marked as deleted', self.path)
                raise FileNotFoundError('%s is deleted' % self.path)
        except dropbox.rest.ErrorResponse as e:
            if 404 == e.status:
                # not found
                self.logger.error('404: %s', self.path)
                raise FileNotFoundError(str(e))
            elif 304 == e.status:
                # not modified
                self.logger.error('304: not modified --> %s', self.path)
                self._dirty = False
                return self
            else:
                raise

        self._metadata = metadata
        return super(MetadataCacheEntry, self).fetch()

    @property
    def uploader(self):
        return self._uploader

    @uploader.setter
    def uploader(self, value):
        self._uploader = value

    @property
    def metadata(self):
        if self.dirty is True:
            self.logger.info('%s found as dirty, re-fetching', self.path)
            self.fetch()
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        assert isinstance(value, dict)
        self._metadata = value


class DataCache(CacheBase):
    def get_entry(self, path, size, create=True):
        super(DataCache, self).get_entry(path)
        cache_entry = self.cache.get(path)
        if cache_entry is not None:
            if cache_entry.size != size:
                self.logger.info('%s: inconsistent size, setting as dirty', path)
                cache_entry.dirty = True
            else:
                # just print, returns cache_entry later
                self.logger.info('data cache: hit %s', path)
        else:
            self.logger.info('data cache miss %s', path)
            if create is False:
                return None

            cache_entry = DataCacheEntry(path, size, self.client).fetch()
            self.set_entry(path, cache_entry)

        return cache_entry

    def set_entry(self, path, entry):
        assert isinstance(entry, DataCacheEntry), 'expected DataCacheEntry, got %s' % (type(entry), )
        super(DataCache, self).set_entry(path, entry)


class DataCacheEntry(CacheEntryBase):
    def __init__(self, path, size, client):
        self.size = size
        self._fp = None
        self._fd = None
        self._buffer = bytearray()
        super(DataCacheEntry, self).__init__(path, client)

    def fetch(self):
        try:
            self._fp = self.client.get_file(self.path)
            self._fd = self._fp.fileno()
            self._buffer = bytearray()
        except dropbox.rest.ErrorResponse as e:
            if 404 == e.status:
                # not found
                self.logger.error('404: %s', self.path)
                raise FileNotFoundError(str(e))
            else:
                self.logger.error('exception: %s', str(e))
                raise
        except AttributeError as e:
            self.logger.error('exception: %s', str(e))
            raise

        return super(DataCacheEntry, self).fetch()

    @property
    def fp(self):
        if self.dirty is True:
            self.logger.info('%s found as dirty, re-fetching', self.path)
            self.fetch()
        return self._fp

    @fp.setter
    def fp(self, value):
        self._fp = value

    @property
    def buffer(self):
        if self.dirty is True:
            self.logger.info('%s found as dirty, re-fetching', self.path)
            self.fetch()
        return self._buffer

    @buffer.setter
    def buffer(self, value):
        self._buffer = value

    @property
    def fd(self):
        if self.dirty is True:
            self.logger.info('%s found as dirty, re-fetching', self.path)
            self.fetch()
            self.logger.error('change me!')
            raise Exception('chenge me!')
        return self._fd

    @fd.setter
    def fd(self, value):
        self._fd = value
