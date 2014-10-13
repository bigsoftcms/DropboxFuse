#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import select
from multiprocessing import Process
from urllib3.exceptions import HTTPError

from dropbox_logger import DropboxLogDummy
from dropbox_exceptions import StreamReadBlock
from dropbox_cache import DataCache
from dropbox_exceptions import FileNotFoundError
from dropbox_download_ipc import DownloadShutdownRequest
from dropbox_download_ipc import DownloadRequest
from dropbox_download_ipc import DownloadCloseRequest, DownloadCloseResponse
from dropbox_download_stream import DownloadStream


class DropboxDownloadServer(Process):
    ERR_ONLY = select.POLLHUP | select.POLLERR | select.POLLNVAL
    READ_ONLY = select.POLLIN | select.POLLPRI | ERR_ONLY
    WRITE_ONLY = select.POLLOUT | ERR_ONLY
    READ_WRITE = READ_ONLY | WRITE_ONLY
    BUFSIZE = 1 * 1024 * 1024

    def __init__(self, dbclient, control_sock, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.dbclient = dbclient
        self.control_sock = control_sock
        self.dcache = DataCache(self.dbclient, log_manager=self.log_manager)
        self.streams = list()

        # for poll()
        self.output_fds = dict()
        self.input_fds = dict()
        self.poller = select.poll()
        self._register_input_fd(self.control_sock)

        super(DropboxDownloadServer, self).__init__()

    def _register_input_fd(self, obj):
        fd = obj.fileno()
        assert not fd in self.input_fds, 'FD already in input_fds'
        if hasattr(obj, 'setblocking'):
            obj.setblocking(False)
        self.input_fds[fd] = obj
        self.poller.register(fd, DropboxDownloadServer.READ_ONLY)
        self.logger.debug('registerd input fd %d for %s', fd, str(obj))

    def _register_output_fd(self, obj):
        fd = obj.fileno()
        assert not fd in self.output_fds, 'FD already in output_fds'
        if hasattr(obj, 'setblocking'):
            obj.setblocking(False)
        self.output_fds[fd] = obj
        self.poller.register(fd, DropboxDownloadServer.WRITE_ONLY)
        self.logger.debug('registerd output fd %d for %s', fd, str(obj))

    def _unregister_fd(self, fd):
        if not isinstance(fd, int):
            fd = fd.fileno()

        if fd in self.input_fds:
            del self.input_fds[fd]
            self.poller.unregister(fd)

        if fd in self.output_fds:
            del self.output_fds[fd]
            self.poller.unregister(fd)
        self.logger.debug('unregisterd fd %d', fd)

    def _socket_by_input_fd(self, fd):
        return self.input_fds.get(fd)

    def _socket_by_output_fd(self, fd):
        return self.output_fds.get(fd)

    def _dcache_by_dropbox_fd(self, fd):
        dcaches = map(lambda x: x.dcache_entry, self.streams)
        for dcache in dcaches:
            if dcache.fd == fd:
                return dcache

    def _stream_by_proxy_port(self, addr, port):
        for stream in self.streams:
            s_addr, s_port = stream.server_sock.getsockname()
            if (s_addr == addr) and (s_port == port):
                return stream

    def _stream_by_proxy_client_fd(self, fd):
        sock = self._socket_by_output_fd(fd)
        for stream in self.streams:
            if stream.client_sock == sock:
                return stream

    def _register_stream(self, stream):
        # assumes that the client socket of the stream
        # is already connected
        self.logger.info('registering stream for %s', os.path.basename(stream.path))
        # make sure the stream fp is accounted for
        if stream.dcache_entry.is_cached is False:
            self._register_input_fd(stream.dcache_entry.fp)
        self._register_output_fd(stream.client_sock)

        self.streams.append(stream)
        self.logger.debug('current streams: %s', str(self.streams))
        return stream

    def _unregister_stream(self, stream):
        self.logger.info('un-registering stream for %s', os.path.basename(stream.path))
        self.streams.remove(stream)

        fd = stream.client_sock.fileno()
        self._unregister_fd(fd)

        fp = stream.dcache_entry.fp
        fd = stream.dcache_entry.fd
        streams_with_fp = filter(lambda s: s.dcache_entry.fp == fp, self.streams)
        if len(streams_with_fp) == 0:
            self._unregister_fd(fd)
        self.logger.debug('current streams: %s', str(self.streams))

    def _serve_request(self, req):
        if isinstance(req, DownloadRequest):
            self.logger.info('serving DownloadRequest for %s', req.path)
            try:
                dcache_entry = self.dcache.get_entry(req.path, size=req.size)
            except FileNotFoundError as e:
                # ditch the request and send the client the exception
                self.logger.error('404: %s', str(e))
                self.control_sock.send(e)
                return

            stream = DownloadStream(req.path, dcache_entry, log_manager=self.log_manager)
            resp = stream.prepare_response()
            self.control_sock.send(resp)
            stream.accept_connection()
            self._register_stream(stream)

        elif isinstance(req, DownloadCloseRequest):
            self.logger.info('serving DownloadCloseRequest for %s:%d', req.addr, req.port)
            self.control_sock.send(DownloadCloseResponse())
            stream = self._stream_by_proxy_port(req.addr, req.port)
            self._unregister_stream(stream)
            del stream

    def _handle_dcache_loop(self, fd):
        dcache = self._dcache_by_dropbox_fd(fd)
        # fill the cache buffer from network
        if dcache.fp is None:
            self.logger.warn('found dcache with no fp in dcache loop')
            return

        self.logger.info('recving from %s',
                         os.path.basename(dcache.path))
        try:
            buf = dcache.fp.read(DropboxDownloadServer.BUFSIZE)
            if 0 == len(buf):
                self.logger.info('finised downloading %s', os.path.basename(dcache.path))
                self.logger.info('removing from input fds..')
                self._unregister_fd(fd)
                dcache.is_cached = True
                return

            self.logger.info('recvd %d from %s',
                             len(buf),
                             os.path.basename(dcache.path))
        except HTTPError as e:
            self.logger.error('exception: %s', str(e))
            return

        dcache.buffer += buf

    def _handle_client_stream(self, fd):
        stream = self._stream_by_proxy_client_fd(fd)
        try:
            buf = stream.read(DropboxDownloadServer.BUFSIZE)
        except StreamReadBlock as e:
            # we will try next time
            return

        if not buf:
            self.logger.info('finised uploading %s to proxy', os.path.basename(dcache.path))
            self.logger.info('removing from output fds..')
            self._unregister_fd(fd)
            return

        sent = stream.client_sock.send(buf)
        if sent != len(buf):
            self.logger.warn('sent != len(buf) -- %d != %d', sent, len(buf))

    def run(self):
        while True:
            self.logger.info('waiting for events..')
            self.logger.debug('entering poll()')
            events = self.poller.poll()
            self.logger.debug('exited poll()')

            for fd, flags in events:
                self.logger.info('got fd %d flags %d', fd, flags)
                sock = self._socket_by_input_fd(fd)
                if flags & (select.POLLHUP | select.POLLERR | select.POLLNVAL):
                    self.logger.warn('poll() detected err on fd %d flags %d', fd, flags)
                    self.logger.warn('problematic socket: %s', sock)
                    if fd in self.input_fds:
                        del self.input_fds[fd]
                    if fd in self.output_fds:
                        del self.output_fds[fd]
                    self.poller.unregister(fd)
                elif flags & (select.POLLIN | select.POLLPRI):
                    if self.control_sock is sock:
                        req = self.control_sock.recv()
                        if isinstance(req, DownloadShutdownRequest):
                            self.logger.critical('got DownloadShutdownRequest, shutting down server!')
                            return
                        self._serve_request(req)
                    else:
                        self._handle_dcache_loop(fd)
                elif flags & select.POLLOUT:
                    self._handle_client_stream(fd)
