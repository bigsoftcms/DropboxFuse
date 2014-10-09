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

from multiprocessing.managers import BaseManager, BaseProxy


class FileNotFoundError(Exception):
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


class Cache(object):
    def __init__(self):
        self.cache = dict()

    def get_cache(self):
        return self.cache

    def get_entry(self, path, client, create=True):
        cache_entry = self.cache.get(path)
        if cache_entry is None:
            print 'cache miss', path
            if create is False:
                return None

            try:
                cache_entry = CacheEntry(path).fetch(client)
            except FileNotFoundError as e:
                print e
                raise fuse.FuseOSError(os.errno.ENOENT)

            self.set_entry(path, cache_entry)

            if cache_entry.metadata['is_dir'] is True:
                print 'cache: its a dir, recursing'
                for content in cache_entry.metadata['contents']:
                    path = content['path']
                    print 'cache: adding', path
                    self.set_entry(path, CacheEntry(path, metadata=content))
        else:
            print 'cache hit', path
        return cache_entry

    def set_entry(self, path, entry):
        assert isinstance(entry, CacheEntry)
        assert isinstance(path, (str, unicode))
        self.cache[path] = entry


class CacheEntry(object):
    def __init__(self, path, metadata=None, data=None):
        self.path = path
        self.metadata = metadata
        self.data = data

    def fetch(self, dropbox_client):
        #TODO: use hash arg?
        try:
            metadata = dropbox_client.metadata(self.path)
        except dropbox.rest.ErrorResponse as e:
            print e
            raise FileNotFoundError('path not found on remote server')

        self.metadata = metadata
        return self

    def __str__(self):
        return '<cache entry for path %s: METADATA=%s DATA=%s' % (self.path,
                                                                  self.metadata is not None,
                                                                  self.data is not None)


class CacheManager(BaseManager):
    pass

CacheManager.register('Cache', Cache)


class DropboxFuse(fuse.Operations):
    def __init__(self, dropbox_client):
        print 'dropboxfuse init start'
        self.client = dropbox_client
        #self.root = path
        self.now = time.time()
        print 'dropboxfuse init end'

        # cache init
        print 'cache init start'
        self.cache_manager = CacheManager()
        self.cache_manager.start()
        self.cache = self.cache_manager.Cache()
        self.cache.get_entry('/', self.client)
        print 'cache init done'

    def getattr(self, path, fh=None):
        print 'getattr', path
        cache_entry = self.cache.get_entry(path, self.client)

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
        cache_entry = self.cache.get_entry(path, self.client, create=False)
        if cache_entry is not None:
            return -os.errno.EEXIST

        try:
            metadata = self.client.file_create_folder(path)
        except dropbox.rest.ErrorResponse as e:
            print e
            return -os.errno.EBADR

        cache_entry = CacheEntry(path, metadata=metadata)
        self.cache.set_entry(path, cache_entry)
        return 0

    def mknod(self, path, mode, dev):
        print 'mknod', path
        raise fuse.FuseOSError(os.errno.EACCES)

    def create(self, path, mode, fh=None):
        print 'create', path
        return 0

    def open(self, path, flags):
        print 'open', path
        cache_entry = self.cache.get_entry(path, self.client)
        return 0

    def readdir(self, path, fh):
        print 'readdir', path
        cache_entry = self.cache.get_entry(path, self.client)

        if cache_entry.metadata['is_dir'] is False:
            raise fuse.ArgumentError('provided path is not a dir')

        files = ['.', '..']
        if not 'contents' in cache_entry.metadata:
            # cache entry might not be full, fetching info
            cache_entry.fetch(self.client)

        for content in cache_entry.metadata['contents']:
            basename = os.path.basename(content['path'])
            files.append(basename)

        return files

    def read(self, path, size, offset, fh):
        print 'read', path
        data = self.get_file(path)
        if data == None:
            return 0
        if offset + size > len(data):
            size = len(data) - offset

        return data[offset:offset + size]

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
                           options.mount_point, options.mount_point,
                           foreground=True, nothreads=True)
    except Exception as e:
        print str(e)
        return -1
    return 0

if __name__ == '__main__':
    sys.exit(main())