#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import socket

from dropbox_logger import DropboxLogDummy
from dropbox_exceptions import DownloadError


class DropboxDownloadProxy(object):
    def __init__(self, path, addr, port, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.path = path
        self.addr = addr
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.offset = 0

    def __del__(self):
        self.sock.close()

    def connect(self):
        self.logger.debug('connecting to %s:%d', self.addr, self.port)
        self.sock.connect((self.addr, self.port))
        self.logger.debug('connected to %s:%d', self.addr, self.port)

    def read(self, size, offset=None):
        # offset is here just to verify that we are in order
        current_offset = self.offset if offset is None else offset
        if current_offset != self.offset:
            msg = 'out of order offset: %d != %d' % (current_offset, self.offset)
            self.logger.error(msg)
            raise DownloadError(msg)

        self.logger.debug('%s: recving..', os.path.basename(self.path))
        buf = self.sock.recv(size)
        self.logger.debug('%s: recvd %d', os.path.basename(self.path), len(buf))
        self.offset += len(buf)
        return buf

