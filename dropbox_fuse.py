#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import fuse
import sys
import argparse
import dropbox
import json
import time
import pwd
import stat
import cStringIO


class FileNotFoundError(Exception):
    pass


class UploadError(Exception):
    pass


class DownloadError(Exception):
    pass


class DropboxConfiguration(dict):
    def __init__(self, path):
        self.path = path
        if os.path.isfile(path):
            try:
                fp = open(self.path, 'rb')
                config = json.load(fp)
            except Exception as e:
                print 'config file %s could not be read, using defaults'
                config = dict()
        else:
            config = dict()
        super(DropboxConfiguration, self).__init__(config)

    def commit(self):
        fp = open(self.path, 'wb')
        json.dump(self, fp)


class DropboxClient(dropbox.client.DropboxClient):
    def __init__(self, config, app_key=None, app_secret=None, access_token=None):
        self.config = DropboxConfiguration(config)
        if not ('app_key' in self.config and 'app_secret' in self.config):
            if app_key is None or app_secret is None:
                raise Exception('app_secret AND app_secret needs to be valid')
            self.config['app_key'] = app_key
            self.config['app_secret'] = app_secret

        if not ('access_token' in self.config and 'user_id' in self.config):
            ret = DropboxClient.get_access_token(self.config['app_key'], self.config['app_secret'])
            access_token, user_id = ret
            self.config['access_token'] = access_token
            self.config['user_id'] = user_id

        self.config.commit()
        super(DropboxClient, self).__init__(self.config['access_token'])

    @staticmethod
    def get_access_token(app_key, app_secret):
        flow = dropbox.client.DropboxOAuth2FlowNoRedirect(app_key, app_secret)

        # Have the user sign in and authorize this token
        authorize_url = flow.start()
        print '1. Go to: ' + authorize_url
        print '2. Click "Allow" (you might have to log in first)'
        print '3. Copy the authorization code.'
        code = raw_input("Enter the authorization code here: ").strip()

        # This will fail if the user enters an invalid authorization code
        access_token, user_id = flow.finish(code)
        return access_token, user_id


class DropboxUploader(object):
    def __init__(self, path, client, overwrite=False):
        self.path = path
        self.client = client
        self.upload_id = None
        self.expected_offset = 0
        self.overwrite = overwrite
        self.expires = None

    def upload_chunk(self, chunk, offset):
        assert offset == self.expected_offset, 'out of order chunk upload'

        print 'uploaded %d KB' % (offset / 1024, )

        fp = cStringIO.StringIO(chunk)
        try:
            res = self.client.upload_chunk(fp, offset=offset, upload_id=self.upload_id)
        except dropbox.rest.ErrorResponse as e:
            raise UploadError(e)

        self.expected_offset = res[0]
        self.upload_id = res[1]

    def commit(self):
        #TODO: maybe add the data to the cache too??
        print 'commit', self.path, self.upload_id, self.overwrite, self.path, self.expected_offset, id(self)
        assert self.upload_id is not None, 'upload_id should not be None when commiting'

        try:
            full_path = os.path.normpath('/dropbox/%s' % self.path)
            print full_path
            res = self.client.commit_chunked_upload(full_path, self.upload_id, overwrite=self.overwrite)
        except dropbox.rest.ErrorResponse as e:
            raise UploadError(str(e))
        return CacheEntry(self.path, self.client, metadata=res)


class DropboxDownloader(object):
    def __init__(self, path, client, cache_entry):
        self.path = path
        self.client = client
        self.cache_entry = cache_entry
        self.expected_offset = 0

        try:
            if self.cache_entry.data is None:
                self.fp = self.client.get_file(self.path)
                self.cache_entry.data = ''
            else:
                self.fp = cStringIO.StringIO(cache_entry.data)
        except dropbox.rest.ErrorResponse as e:
            raise DownloadError(str(e))

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


class DropboxFuse(fuse.Operations):
    def __init__(self, dropbox_client):
        self.client = dropbox_client
        self.now = time.time()

        # cache init
        self.cache = Cache(self.client)
        self.cache.get_entry('/')

    def getattr(self, path, fh=None):
        print 'getattr', path
        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            print e
            raise fuse.FuseOSError(os.errno.ENOENT)

        uid = pwd.getpwuid(os.getuid()).pw_uid
        gid = pwd.getpwuid(os.getuid()).pw_gid
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
        print 'mkdir', path
        cache_entry = self.cache.get_entry(path, create=False)
        if cache_entry is not None:
            return -os.errno.EEXIST

        try:
            metadata = self.client.file_create_folder(path)
        except dropbox.rest.ErrorResponse as e:
            print e
            return -os.errno.EBADR

        cache_entry = CacheEntry(path, self.client, metadata=metadata)
        self.cache.set_entry(path, cache_entry)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def mknod(self, path, mode, dev):
        print 'mknod', path
        raise fuse.FuseOSError(os.errno.EACCES)

    def create(self, path, mode, fh=None):
        print 'create', path
        cache_entry = self.cache.get_entry(path, create=False)
        should_overwrite = True if cache_entry is not None else False
        if cache_entry is None:
            cache_entry = CacheEntry(path, self.client)

        if cache_entry.uploader is not None:
            raise fuse.FuseOSError(fuse.EBUSY)

        cache_entry.uploader = DropboxUploader(path, self.client, overwrite=should_overwrite)

        #fakeing a cache entry metadata, will be overwritten later
        #by the real metadata from dropbox
        cache_entry.metadata['bytes'] = '0'
        cache_entry.metadata['is_dir'] = False
        cache_entry.metadata['path'] = path
        self.cache.set_entry(path, cache_entry)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def open(self, path, flags):
        flags = 32768 - flags
        print 'open', path, flags

        if flags == os.O_RDWR:
            print 'invalid flags: O_RDWR'
            raise fuse.FuseOSError(fuse.EINVAL)

        if flags == os.O_WRONLY:
            # if it O_WRONLY, let self.create do its thing
            print 'valid flags: O_WRONLY'
            return self.create(path, 0444)
        elif flags != os.O_RDONLY:
            # if there is no O_WRONLY / O_RD_ONLY
            print 'invalid flags: not O_RDONLY or O_WRONLY'
            raise fuse.FuseOSError(fuse.EINVAL)

        # in case of O_RDONLY
        print 'valid flags: O_RDONLY'
        try:
            cache_entry = self.cache.get_entry(path, self.client)
        except FileNotFoundError as e:
            print e
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.downloader is not None:
            raise fuse.FuseOSError(fuse.EBUSY)

        cache_entry.downloader = DropboxDownloader(path, self.client, cache_entry)
        self.cache.set_entry(path, cache_entry)
        return 0

    def write(self, path, buf, offset, fh=None):
        print 'write', path
        cache_entry = self.cache.get_entry(path, create=False)
        if cache_entry is None:
            print 'cache inconsistency'
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.uploader is None:
            print 'uploader not found'
            raise fuse.FuseOSError(os.errno.EBADR)

        cache_entry.uploader.upload_chunk(buf, offset)
        self.cache.set_entry(path, cache_entry)
        return len(buf)

    def read(self, path, size, offset, fh):
        print 'read', path
        cache_entry = self.cache.get_entry(path, create=False)
        if cache_entry is None:
            print 'cache inconsistency'
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.downloader is None:
            print 'downloader not found'
            raise fuse.FuseOSError(os.errno.EBADR)

        buf = cache_entry.downloader.download_chunk(size, offset)
        return buf

    def release(self, path, fh=None):
        print 'release', path
        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            print e
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.uploader is not None:
            try:
                cache_entry = cache_entry.uploader.commit()
            except UploadError as e:
                print e
                raise fuse.FuseOSError(fuse.EIO)
            # replace the old fake entry with the new cache entry
            self.cache.set_entry(path, cache_entry)

        if cache_entry.downloader is not None:
            cache_entry.downloader = None

    def unlink(self, path):
        print 'unlink', path
        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            print e
            raise fuse.FuseOSError(os.errno.ENOENT)

        self.client.file_delete(path)
        self.cache.remove_entry(path)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def rmdir(self, path):
        print 'rmdir', path
        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            print e
            raise fuse.FuseOSError(os.errno.ENOENT)

        self.client.file_delete(path)
        self.cache.remove_entry(path)
        self.cache.set_parent_dirty(cache_entry)
        return 0

    def readdir(self, path, fh):
        print 'readdir', path
        try:
            cache_entry = self.cache.get_entry(path)
        except FileNotFoundError as e:
            print e
            raise fuse.FuseOSError(os.errno.ENOENT)

        if cache_entry.metadata['is_dir'] is False:
            raise fuse.ArgumentError('provided path is not a dir')

        files = ['.', '..']
        if not 'contents' in cache_entry.metadata:
            # cache entry might not be full, fetching info
            cache_entry.fetch()

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
        print 'client init done'
        dropbox_fuse = DropboxFuse(dropbox_client)
        dbfuse = fuse.FUSE(dropbox_fuse,
                           options.mount_point,
                           foreground=True, nothreads=True)
    except Exception as e:
        print str(e)
        return -1
    return 0

if __name__ == '__main__':
    sys.exit(main())