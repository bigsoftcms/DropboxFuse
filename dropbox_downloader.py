#!/usr/bin/python2
# -*- coding: utf-8 -*-

import dropbox
import cStringIO

from dropbox_exceptions import DownloadError


class DropboxDownloader(object):
    def __init__(self, path, client, cache_entry):
        self.path = path
        self.client = client
        self.cache_entry = cache_entry
        self.expected_offset = 0

        if self.cache_entry.data is not None:
            if len(self.cache_entry.data) != int(self.cache_entry.metadata['bytes']):
                print 'data cache: inconsistent size, refetching data'
                self.cache_entry.data = None
            else:
                print 'data cache hit', self.path
                self.fp = cStringIO.StringIO(cache_entry.data)
        if self.cache_entry.data is None:
            print 'data cache miss', self.path
            try:
                self.fp = self.client.get_file(self.path)
            except dropbox.rest.ErrorResponse as e:
                raise DownloadError(str(e))
            self.cache_entry.data = ''

    def __del__(self):
        self.fp.close()

    def download_chunk(self, size, offset):
        assert offset == self.expected_offset, 'out of order chunk download'
        assert hasattr(self, 'fp'), 'self.fp not found'

        print 'Downloaded %d KB' % (offset / 1024, )
        chunk = self.fp.read(size)

        self.expected_offset += len(chunk)
        self.cache_entry.data += chunk
        return chunk
