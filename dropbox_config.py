#!/usr/bin/python2
# -*- coding: utf-8 -*-

import json
import os


class DropboxConfiguration(dict):
    def __init__(self, path):
        self.path = path
        if os.path.isfile(path):
            try:
                fp = open(self.path, 'rb')
                config = json.load(fp)
            except Exception as e:
                print 'config file %s could not be read, using defaults'
                config = dict()
        else:
            config = dict()
        super(DropboxConfiguration, self).__init__(config)

    def commit(self):
        fp = open(self.path, 'wb')
        json.dump(self, fp)
