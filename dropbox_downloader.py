#!/usr/bin/python2
# -*- coding: utf-8 -*-

import os
import select
import socket
import cPickle

from multiprocessing import Process
from urllib3.exceptions import HTTPError
from dropbox_exceptions import DownloadError, StreamReadBlock
from dropbox_cache import CacheManager, DataCache
from dropbox_exceptions import FileNotFoundError
from dropbox_logger import DropboxLogger


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


class DownloadStream(object):
    def __init__(self, path, dcache_entry):
        #logger
        self.logger = DropboxLogger(self)
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
        self.logger.info('created stream for %s', os.path.basename(self.path))

    def __del__(self):
        if self.server_sock is not None:
            self.server_sock.close()
        if self.client_sock is not None:
            self.client_sock.close()
        self.logger.info('closed stream for %s', os.path.basename(self.path))

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


class DropboxDownloaderServer(Process):
    ERR_ONLY = select.POLLHUP | select.POLLERR | select.POLLNVAL
    READ_ONLY = select.POLLIN | select.POLLPRI | ERR_ONLY
    WRITE_ONLY = select.POLLOUT | ERR_ONLY
    READ_WRITE = READ_ONLY | WRITE_ONLY
    BUFSIZE = 1 * 1024 * 1024

    def __init__(self, dbclient, control_sock):
        #logger
        self.logger = DropboxLogger(self)

        self.dbclient = dbclient
        self.control_sock = control_sock
        self.dcache = DataCache(self.dbclient)
        self.streams = list()

        # for poll()
        self.output_fds = dict()
        self.input_fds = {
            self.control_sock.fileno(): self.control_sock
        }

        self._sync_poll_fds()
        super(DropboxDownloaderServer, self).__init__()

    def _sync_poll_fds(self):
        self.logger.debug('setting poll()')
        self.logger.info('input fds: %s', self.input_fds)
        self.poller = select.poll()
        for fd in self.input_fds:
            obj = self.input_fds[fd]
            if hasattr(obj, 'setblocking'):
                obj.setblocking(False)
            self.poller.register(fd, DropboxDownloaderServer.READ_ONLY)

        self.logger.info('output fds: %s', self.output_fds)
        for fd in self.output_fds:
            obj = self.output_fds[fd]
            if hasattr(obj, 'setblocking'):
                obj.setblocking(False)
            self.poller.register(fd, DropboxDownloaderServer.WRITE_ONLY)

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
        fd = stream.dcache_entry.fd
        fp = stream.dcache_entry.fp
        if not fd in self.input_fds:
            self.input_fds[fd] = fp

        client_sock = stream.client_sock
        fd = client_sock.fileno()
        if not client_sock in self.output_fds:
            self.output_fds[fd] = client_sock

        self.streams.append(stream)
        self._sync_poll_fds()
        return stream

    def _unregister_stream(self, stream):
        self.logger.info('un-registering stream for %s', os.path.basename(stream.path))
        self.streams.remove(stream)

        fd = stream.client_sock.fileno()
        del self.output_fds[fd]

        fp = stream.dcache_entry.fp
        fd = stream.dcache_entry.fd
        streams_with_fp = filter(lambda s: s.dcache_entry.fp == fp, self.streams)
        if len(streams_with_fp) == 0:
            if fd in self.input_fds:
                del self.input_fds[fd]
        self._sync_poll_fds()

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

            stream = DownloadStream(req.path, dcache_entry)
            resp = stream.prepare_response()
            self.control_sock.send(resp)
            stream.accept_connection()
            self._register_stream(stream)

        elif isinstance(req, DownloadCloseRequest):
            self.logger.info('serving DownloadCloseRequest for %s:%d', req.addr, req.port)
            self.control_sock.send(DownloadCloseResponse())
            stream = self._stream_by_proxy_port(req.addr, req.port)
            self._unregister_stream(stream)

    def _handle_dcache_loop(self, fd):
        dcache = self._dcache_by_dropbox_fd(fd)
        # fill the cache buffer from network
        if dcache.fp is None:
            self.logger.warn('found dcache with no fp in dcache loop')
            return

        self.logger.info('recving from %s',
                         os.path.basename(dcache.path))
        try:
            buf = dcache.fp.read(DropboxDownloaderServer.BUFSIZE)
            if 0 == len(buf):
                self.logger.info('finised downloading %s', os.path.basename(dcache.path))
                self.logger.info('removing from input fds..')
                del self.input_fds[fd]
                self._sync_poll_fds()
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
            buf = stream.read(DropboxDownloaderServer.BUFSIZE)
        except StreamReadBlock as e:
            # we will try next time
            return

        if not buf:
            self.logger.info('finised uploading %s to proxy', os.path.basename(dcache.path))
            self.logger.info('removing from output fds..')
            del self.output_fds[fd]
            self._sync_poll_fds()
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


class DropboxDownloadProxy(object):
    def __init__(self, path, addr, port):
        #logger
        self.logger = DropboxLogger(self)

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


class DropboxDownloadManager(object):
    def __init__(self, dbclient):
        #logger
        self.logger = DropboxLogger(self)

        self.dbclient = dbclient
        server_sock, client_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.control_sock = ControlSocket(client_sock)
        self.mcache = CacheManager.get_cache('MetadataCache')
        self.downloads = dict()
        self.next_fd = 0
        self.server = DropboxDownloaderServer(self.dbclient, ControlSocket(server_sock))
        self.server.start()

    def __del__(self):
        self.shutdown_server()

    def download_by_fd(self, fd):
        return self.downloads.get(fd)

    def shutdown_server(self):
        self.logger.critical('shutting down server: sending shutdown request')
        msg = DownloadShutdownRequest()
        self.control_sock.send(msg)
        self.logger.critical('waiting for server')
        self.server.join()
        self.logger.critical('server died, RIP')

    def open_file(self, remote_path):
        self.logger.info('open remote file %s', remote_path)
        mcache_entry = self.mcache.get_entry(remote_path)
        # request the server to give us a data pipe handle
        # for the file `remote_path` with this expected size
        # in order to detect cache inconsistency
        msg = DownloadRequest(remote_path, mcache_entry.metadata['bytes'])
        self.control_sock.send(msg)

        #TODO: maybe use poll() or just verify that the server is alive
        resp = self.control_sock.recv()
        if not isinstance(resp, DownloadResponse):
            self.logger.error('expected DownloadResponse, got %s', str(resp))
            raise DownloadError(remote_path)

        self.logger.info('got DownloadResponse: %s:%d', resp.stream_addr, resp.stream_port)
        proxy = DropboxDownloadProxy(remote_path, resp.stream_addr, resp.stream_port)
        proxy.connect()

        fd = int(self.next_fd)
        self.downloads[fd] = proxy
        self.next_fd += 1
        self.logger.debug('allocated fd %d', fd)
        return fd

    def close_file(self, fd):
        self.logger.info('closing remote file fd %d', fd)
        download = self.download_by_fd(fd)
        if download is None:
            msg = 'no such download for fd %d' % (fd, )
            self.logger.error(msg)
            raise DownloadError(msg)

        self.logger.info('sending DownloadCloseRequest for %s:%d', download.addr, download.port)
        msg = DownloadCloseRequest(download.addr, download.port)
        self.control_sock.send(msg)

        #TODO: maybe use poll() or just verify that the server is alive
        resp = self.control_sock.recv()
        if not isinstance(resp, DownloadCloseResponse):
            self.logger.error('failed to close remote file: %s', str(resp))
            raise DownloadError(remote_path)

        del self.downloads[fd]
