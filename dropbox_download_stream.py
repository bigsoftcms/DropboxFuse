#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import socket

from dropbox_logger import DropboxLogManager
from dropbox_exceptions import DownloadError, StreamReadBlock
from dropbox_download_ipc import DownloadResponse
from dropbox_utils import FakeFileObject


class DownloadStream(FakeFileObject):
    def __init__(self, path, dcache_entry):
        self.logger = DropboxLogManager.get_logger(self)
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
        self.client_pos = 0
        self.logger.info('created stream (%d) for %s', id(self), os.path.basename(self.path))
        super(DownloadStream, self).__init__()

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
        self.logger.debug('%s: read size %d', self.path, size)
        # first, set stream as EWOULDBLOCK
        self.is_readable = False
        # non blocking read from dcache buffer
        # reads from dcache.buffer only, not from dcache.fp
        if self.dcache_entry.dirty is True:
            msg = '%s: dcache indicates its dirty, cancelling stream' % (os.path.basename(self.path), )
            self.logger.warn(msg)
            raise DownloadError(msg)

        pos = self.client_pos
        dcache = self.dcache_entry
        dcache_buf = dcache.buffer
        if pos == dcache.size:
            # return zero len buffer to indicate eof
            self.logger.debug('%s: end of buffer', os.path.basename(self.path))
            return ''

        if pos >= len(dcache_buf):
            self.logger.debug('%s: read would block. pos %d, buf len %d',
                              os.path.basename(self.path),
                              pos,
                              len(dcache_buf))
            raise StreamReadBlock()

        buf = dcache_buf[pos:pos+size]
        self.client_pos += len(buf)

        if 0 == len(buf):
            self.logger.warn('read buffer is len(0)')

        # set the stream readable if the current buffer contain more data
        if ((len(dcache_buf) - 1) - self.client_pos) > 0:
            self.is_readable = True

        return buf
