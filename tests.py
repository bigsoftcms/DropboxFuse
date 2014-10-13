#!/usr/bin/python2
# -*- coding: utf-8 -*-

import sys
import os
from dropbox_logger import DropboxLogManager
from dropbox_client import DropboxClient
from dropbox_cache import CacheManager, MetadataCache
from dropbox_download_manager import DropboxDownloadManager


def download_file(download_manager, filename):
    fd = download_manager.open_file(filename)
    proxy = download_manager.download_by_fd(fd)
    buf = proxy.read(1024, offset=0)
    download_manager.close_file(fd)
    return buf


def main():
    config_file = os.path.join(os.getenv('HOME'), '.dropboxfuse')
    log = DropboxLogManager()
    client = DropboxClient(config_file, log_manager=log)
    CacheManager.set_cache('MetadataCache', MetadataCache(client, log_manager=log))
    download_manager = DropboxDownloadManager(client, log_manager=log)
    print download_file(download_manager, '/hello.txt')
    print download_file(download_manager, '/hello.txt')

    del download_manager
    del client
    del log


if __name__ == "__main__":
    sys.exit(main())