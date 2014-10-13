#!/usr/bin/python2
# -*- coding: utf-8 -*-

import socket

from dropbox_logger import DropboxLogDummy
from dropbox_exceptions import DownloadError
from dropbox_cache import CacheManager
from dropbox_download_ipc import ControlSocket, DownloadShutdownRequest
from dropbox_download_ipc import DownloadRequest, DownloadResponse
from dropbox_download_ipc import DownloadCloseRequest, DownloadCloseResponse
from dropbox_download_server import DropboxDownloadServer
from dropbox_download_client import DropboxDownloadProxy


class DropboxDownloadManager(object):
    def __init__(self, dbclient, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.dbclient = dbclient
        server_sock, client_sock = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
        self.control_sock = ControlSocket(client_sock)
        self.mcache = CacheManager.get_cache('MetadataCache')
        self.downloads = dict()
        self.next_fd = 0
        self.server = DropboxDownloadServer(self.dbclient, ControlSocket(server_sock), log_manager=self.log_manager)
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
        proxy = DropboxDownloadProxy(remote_path, resp.stream_addr, resp.stream_port, log_manager=self.log_manager)
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
            raise DownloadError(str(resp))

        del self.downloads[fd]
        del download
