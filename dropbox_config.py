#!/usr/bin/python2
# -*- coding: utf-8 -*-

import json
import os

from dropbox_logger import DropboxLogDummy


class DropboxConfiguration(dict):
    def __init__(self, path, log_manager=None):
        # logger
        if log_manager is None:
            self.log_manager = None
            self.logger = DropboxLogDummy()
        else:
            self.log_manager = log_manager
            self.logger = self.log_manager.agent(self)

        self.path = path
        if os.path.isfile(path):
            try:
                fp = open(self.path, 'rb')
                config = json.load(fp)
            except:
                self.logger.warn('config file %s could not be read, using defaults')
                config = dict()
        else:
            config = dict()
        super(DropboxConfiguration, self).__init__(config)

    def commit(self):
        fp = open(self.path, 'wb')
        json.dump(self, fp)
        self.logger.info('config commited to disk')
