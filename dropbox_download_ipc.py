#!/usr/bin/python2
# -*- coding: utf-8 -*-

import cPickle


class DownloadRequest(object):
    def __init__(self, path, size):
        self.path = path
        self.size = size


class DownloadResponse(object):
    def __init__(self, addr, port):
        self.stream_addr = addr
        self.stream_port = port


class DownloadShutdownRequest(object):
    pass


class DownloadCloseRequest(object):
    def __init__(self, addr, port):
        self.addr = addr
        self.port = port


class DownloadCloseResponse(object):
    pass


class ControlSocket(object):
    BUFSIZE = 1024

    def __init__(self, sock):
        self.sock = sock

    def __del__(self):
        self.sock.close()

    def send(self, msg):
        msg = cPickle.dumps(msg)
        self.sock.sendall(msg)

    def recv(self):
        msg = self.sock.recv(ControlSocket.BUFSIZE)
        msg = cPickle.loads(msg)
        return msg

    def fileno(self):
        return self.sock.fileno()

    def setblocking(self, mode):
        return self.sock.setblocking(mode)

