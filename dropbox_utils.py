#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import fcntl


class FileDescriptor(object):
    def __init__(self, fd):
        self._fd = fd

    def fileno(self):
        return self._fd

    def setblocking(self, mode):
        # get current flags
        flags = fcntl.fcntl(self._fd, fcntl.F_GETFL)

        if mode is True:
            flags &= ~os.O_NONBLOCK
        else:
            flags |= os.O_NONBLOCK
        # set new flags
        fcntl.fcntl(self._fd, fcntl.F_SETFL, flags)

    def read(self, size):
        return os.read(self._fd, size)

    def write(self, buf):
        return os.write(self._fd, buf)

    def flush(self):
        return os.fsync(self._fd)


class FakeFileObject(object):
    BUFFER_SIZE = 1024

    def __init__(self):
        # create the pipe that provides us with (poll/select)-able file descriptor
        rpipe, wpipe = os.pipe()
        self._rpipe = FileDescriptor(rpipe)
        self._wpipe = FileDescriptor(wpipe)

        # set the read fd as non blocking
        # we want our write fd to block
        self._rpipe.setblocking(False)

        self._is_readable = False

    def fileno(self):
        return self._rpipe.fileno()

    @property
    def is_readable(self):
        return self._is_readable

    @is_readable.setter
    def is_readable(self, state):
        assert isinstance(state, bool), 'Expected Bool, found %s' % (type(state), )
        if state == self._is_readable:
            return

        if state is True:
            self._wpipe.write('shit')
        else:
            self._rpipe.read(FakeFileObject.BUFFER_SIZE)
