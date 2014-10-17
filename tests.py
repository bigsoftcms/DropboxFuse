#!/usr/bin/python2
# -*- coding: utf-8 -*-
"""
	Used to test simple flows without fuse
"""

import sys
import os
from dropbox_logger import DropboxLogManager
from dropbox_client import DropboxClient
from dropbox_cache import CacheManager, MetadataCache
from dropbox_download_manager import DropboxDownloadManager


def download_file(download_manager, filename):
    fd = download_manager.open_file(filename)
    proxy = download_manager.download_by_fd(fd)
    buf = bytearray()
    while True:
        tmpbuf = proxy.read(131072)
        if not tmpbuf:
            break
        buf += tmpbuf
        download_manager.logger.debug('downloaded %d bytes', len(buf))
    download_manager.close_file(fd)
    return buf


def main():
    config_file = os.path.join(os.getenv('HOME'), '.dropboxfuse')
    log = DropboxLogManager()
    client = DropboxClient(config_file)
    CacheManager.set_cache('MetadataCache', MetadataCache(client))
    download_manager = DropboxDownloadManager(client)
    print download_file(download_manager, '/hello.txt')

    del download_manager
    del client
    del log


if __name__ == "__main__":
    sys.exit(main())
