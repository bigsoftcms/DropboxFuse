#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import socket

from dropbox_logger import DropboxLogDummy
from dropbox_exceptions import DownloadError, StreamReadBlock
from dropbox_download_ipc import DownloadResponse


class DownloadStream(object):
    def __init__(self, path, dcache_entry, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.path = path
        self.dcache_entry = dcache_entry
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.bind(('127.0.0.1', 0))
        # NOTE: backlog is set to 1
        # in order to allow the following `hack`
        # the socket is created, binded and listen(1)
        # the the addr, port is passed to the client process
        # the client will try to connect() and will be catched by the backlog
        # then we (our process) will hit accept() and finish the handshake
        self.server_sock.listen(1)
        self.client_sock = None
        self.client_addr = None
        self.client_read = 0
        self.logger.info('created stream (%d) for %s', id(self), os.path.basename(self.path))

    def __del__(self):
        if self.server_sock is not None:
            self.server_sock.close()
        if self.client_sock is not None:
            self.client_sock.close()
        self.logger.info('closed stream (%d) for %s', id(self), os.path.basename(self.path))

    def accept_connection(self):
        self.logger.info('Accepting connection for %s', os.path.basename(self.path))
        client_sock, addr = self.server_sock.accept()
        self.client_sock = client_sock
        self.client_addr = addr
        self.logger.info('Accepted connection')

    def prepare_response(self):
        addr, port = self.server_sock.getsockname()
        return DownloadResponse(addr, port)

    def read(self, size):
        # non blocking read from dcache buffer
        # reads from dcache.buffer only, not from dcache.fp
        if self.dcache_entry.dirty is True:
            msg = '%s: dcache indicates its dirty, cancelling stream' % (os.path.basename(self.path), )
            self.logger.warn(msg)
            raise DownloadError(msg)

        offset = self.client_read
        dcache = self.dcache_entry
        dcache_buf = dcache.buffer
        if offset == (dcache.size - 1):
            # return zero len buffer to indicate eof
            self.logger.debug('%s: end of buffer', os.path.basename(self.path))
            return ''

        if offset >= len(dcache_buf):
            self.logger.debug('%s: read would block', os.path.basename(self.path))
            raise StreamReadBlock()

        buf = dcache_buf[offset:offset+size]
        self.client_read += len(buf)
        return buf
