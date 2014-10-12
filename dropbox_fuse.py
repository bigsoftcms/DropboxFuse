#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import fuse
import errno
import sys
import argparse
import time
import stat

import dropbox
from dropbox_exceptions import UploadError, FileNotFoundError
from dropbox_client import DropboxClient
from dropbox_cache import CacheManager, MetadataCache, MetadataCacheEntry, DataCache
from dropbox_downloader import DropboxDownloadManager
from dropbox_uploader import DropboxUploader
from dropbox_logger import DropboxLogger


class DropboxFuse(fuse.Operations):
    def __init__(self, dropbox_client):
        # logger
        self.logger = DropboxLogger(self)
        # dropbox api client
        self.client = dropbox_client
        # cache init
        self.cache = CacheManager.get_cache('MetadataCache')
        self.cache.get_entry('/')
        # downloader
        self.download_manager = DropboxDownloadManager(dropbox_client)

    def getattr(self, path, fh=None):
        self.logger.info('getattr %s', path)
        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            raise fuse.FuseOSError(os.errno.ENOENT)

        now = time.time()
        ret = dict(
            st_size=int(cache_entry.metadata['bytes']),
            st_ctime=now,
            st_mtime=now,
            st_atime=now
        )
        if cache_entry.metadata['is_dir'] is True:
            ret['st_mode'] = stat.S_IFDIR | 0755
            ret['st_nlink'] = 3
        else:
            ret['st_mode'] = stat.S_IFREG | 0444
            ret['st_nlink'] = 1

        return ret

    def mkdir(self, path, fh=None):
        self.logger.info('mkdir %s', path)
        cache_entry = self.cache.get_entry(path, create=False)
        if cache_entry is not None:
            return -os.errno.EEXIST

        try:
            metadata = self.client.file_create_folder(path)
        except dropbox.rest.ErrorResponse as e:
            print e
            return -os.errno.EBADR

        cache_entry = MetadataCacheEntry(path, self.client, metadata=metadata)
        self.cache.set_entry(path, cache_entry)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def mknod(self, path, mode, dev):
        self.logger.info('mknod %s', path)
        raise fuse.FuseOSError(os.errno.EACCES)

    def create(self, path, mode, fh=None):
        self.logger.info('create %s', path)
        cache_entry = self.cache.get_entry(path, create=False)
        should_overwrite = True if cache_entry is not None else False
        if cache_entry is None:
            self.logger.info('creating MetadataCacheEntry')
            cache_entry = MetadataCacheEntry(path, self.client)

        if cache_entry.uploader is not None:
            self.logger.info('uploader already exists, setting fuse as EBUSY')
            raise fuse.FuseOSError(errno.EBUSY)

        cache_entry.uploader = DropboxUploader(path, self.client, overwrite=should_overwrite)

        #fakeing a cache entry metadata, will be overwritten later
        #by the real metadata from dropbox
        cache_entry.dirty = False
        cache_entry.metadata['bytes'] = '0'
        cache_entry.metadata['is_dir'] = False
        cache_entry.metadata['path'] = path
        self.cache.set_entry(path, cache_entry)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def open(self, path, flags):
        flags = 32768 - flags
        self.logger.info('open %s %d', path, flags)

        if flags == os.O_RDWR:
            self.logger.info('invalid flags: O_RDWR')
            raise fuse.FuseOSError(errno.EINVAL)

        if flags == os.O_WRONLY:
            # if it O_WRONLY, let self.create do its thing
            self.logger.info('valid flags: O_WRONLY')
            return self.create(path, 0444)
        elif flags != os.O_RDONLY:
            # if there is no O_WRONLY / O_RD_ONLY
            self.logger.error('invalid flags: not O_RDONLY or O_WRONLY')
            raise fuse.FuseOSError(errno.EINVAL)

        # in case of O_RDONLY
        self.logger.info('valid flags: O_RDONLY')
        try:
            cache_entry = self.cache.get_entry(path, self.client)
        except FileNotFoundError as e:
            self.logger.error('file not found error: %s', path)
            raise fuse.FuseOSError(os.errno.ENOENT)

        fd = self.download_manager.open_file(path)
        self.cache.set_entry(path, cache_entry)
        return fd

    def write(self, path, buf, offset, fh=None):
        fd = int(fh)
        self.logger.info('write %s len %d offset %d fd %d', path, len(buf), offset, fd)
        cache_entry = self.cache.get_entry(path, create=False)
        if cache_entry is None:
            self.logger.error('cache inconsistency')
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.uploader is None:
            self.logger.error('uploader not found')
            raise fuse.FuseOSError(os.errno.EBADR)

        cache_entry.uploader.upload_chunk(buf, offset)
        self.cache.set_entry(path, cache_entry)
        return len(buf)

    def read(self, path, size, offset, fh):
        fd = int(fh)
        self.logger.info('read %s size %d offset %d fd %d', path, size, offset, fd)

        cache_entry = self.cache.get_entry(path, create=False)
        if cache_entry is None:
            self.logger.error('cache inconsistency')
            raise fuse.FuseOSError(os.errno.ENOENT)

        proxy = self.download_manager.download_by_fd(fd)
        if proxy is None:
            self.logger.error('downloader proxy not found')
            raise fuse.FuseOSError(os.errno.EBADR)

        buf = proxy.read(size, offset=offset)
        return buf

    def release(self, path, fh=None):
        fd = int(fh)
        self.logger.info('release %s fd %d', path, fd)

        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            self.logger.error('file not found error: %s', path)
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.uploader is not None:
            try:
                cache_entry = cache_entry.uploader.commit()
            except UploadError as e:
                self.logger.error('upload error error: %s', str(e))
                raise fuse.FuseOSError(errno.EIO)
            # replace the old fake entry with the new cache entry
            self.cache.set_entry(path, cache_entry)

        fd = int(fh)
        self.download_manager.close_file(fd)

    def unlink(self, path):
        self.logger.info('unlink %s', path)

        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            self.logger.error('file not found error: %s', path)
            raise fuse.FuseOSError(os.errno.ENOENT)

        self.client.file_delete(path)
        self.cache.remove_entry(path)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def rmdir(self, path):
        self.logger.info('rmdir %s', path)

        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            self.logger.error('file not found error: %s', path)
            raise fuse.FuseOSError(os.errno.ENOENT)

        self.client.file_delete(path)
        self.cache.remove_entry(path)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def readdir(self, path, fh):
        self.logger.info('rmdir %s', path)

        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            self.logger.error('file not found error: %s', path)
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.metadata['is_dir'] is False:
            self.logger.error('%s not a directory', path)
            raise fuse.FuseOSError(errno.ENOTDIR)

        files = ['.', '..']
        if not 'contents' in cache_entry.metadata:
            # cache entry might not be full, fetching info
            self.logger.info('setting metadata cache dirty: %s', cache_entry.path)
            cache_entry.dirty = True

        for content in cache_entry.metadata['contents']:
            basename = os.path.basename(content['path'])
            files.append(basename)

        return files


def main():
    default_path = os.path.join(os.getenv('HOME'), '.dropboxfuse')
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mount-point', type=str, required=True)
    parser.add_argument('-c', '--config', type=str, default=default_path, required=False)
    parser.add_argument('-k', '--app-key', type=str, default=None, required=False)
    parser.add_argument('-s', '--app-secret', type=str, default=None, required=False)
    parser.add_argument('-a', '--access-token', type=str, default=None, required=False)

    options = parser.parse_args()
    try:
        dropbox_client = DropboxClient(options.config, options.app_key, options.app_secret, options.access_token)
        CacheManager.set_cache('MetadataCache', MetadataCache(dropbox_client))
        CacheManager.set_cache('DataCache', DataCache(dropbox_client))
        dropbox_fuse = DropboxFuse(dropbox_client)
        dbfuse = fuse.FUSE(dropbox_fuse,
                           options.mount_point,
                           foreground=True, nothreads=True)
    except Exception as e:
        print str(e)
        raise
    return 0

if __name__ == '__main__':
    sys.exit(main())