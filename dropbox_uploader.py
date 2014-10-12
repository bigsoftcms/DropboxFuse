#!/usr/bin/python2
# -*- coding: utf-8 -*-

import cStringIO
import os

import dropbox
from dropbox_exceptions import UploadError
from dropbox_cache import MetadataCacheEntry


class DropboxUploader(object):
    def __init__(self, path, client, overwrite=False):
        # logger
        self.logger = DropboxLogger(self)

        self.path = path
        self.client = client
        self.upload_id = None
        self.expected_offset = 0
        self.overwrite = overwrite
        self.expires = None

    def upload_chunk(self, chunk, offset):
        self.logger.info('%s: upload chunk len %d offset %d', self.path, len(chunk), offset)
        assert offset == self.expected_offset, 'out of order chunk upload'

        self.logger.info('%s: uploaded %d KB' % (
            os.path.basename(self.path),
            offset / 1024)
        )

        fp = cStringIO.StringIO(chunk)
        try:
            res = self.client.upload_chunk(fp, offset=offset, upload_id=self.upload_id)
        except dropbox.rest.ErrorResponse as e:
            self.logger.error('upload_chunk error: %s', str(e))
            raise UploadError(e)

        self.expected_offset = res[0]
        self.upload_id = res[1]

    def commit(self):
        #TODO: maybe add the data to the cache too??
        self.logger.info('%s: commit upload (id %s)',
                         os.path.basename(self.path),
                         self.upload_id)
        assert self.upload_id is not None, 'upload_id should not be None when commiting'

        try:
            full_path = os.path.normpath('/dropbox/%s' % self.path)
            res = self.client.commit_chunked_upload(full_path, self.upload_id, overwrite=self.overwrite)
        except dropbox.rest.ErrorResponse as e:
            self.logger.error('commit_chunked_upload error: %s', str(e))
            raise UploadError(str(e))
        return MetadataCacheEntry(self.path, self.client, metadata=res)
