#!/usr/bin/python2
# -*- coding: utf-8 -*-

import logging
import sys
import traceback
import multiprocessing


class QueueHandler(logging.Handler):
    """
    This is a logging handler which sends events to a multiprocessing queue.
    """

    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        self.queue = queue
        super(QueueHandler, self).__init__()

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue.
        """
        try:
            ei = record.exc_info
            if ei:
                # just to get traceback text into record.exc_text
                dummy = self.format(record)
                # not needed any more
                record.exc_info = None
            self.queue.put_nowait(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print record, type(record), dir(record)
            self.handleError(record)


class DropboxLogServer(multiprocessing.Process):
    def __init__(self, queue):
        self.queue = queue
        self.logger = None
        super(DropboxLogServer, self).__init__()

    def run(self):
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter('%(asctime)s %(name)-24s %(levelname)-10s %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)

        while True:
            try:
                record = self.queue.get()
                # We send this as a sentinel to tell the listener to quit.
                if record is None:
                    break

                logger = logging.getLogger(record.name)
                # No level or filter logic applied - just do it!
                logger.handle(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                print >> sys.stderr, 'Whoops! Problem:'
                traceback.print_exc(file=sys.stderr)


class DropboxLogManager(object):
    def __init__(self):
        self.queue = multiprocessing.Queue()
        # create the log server
        self.process = DropboxLogServer(self.queue)
        self.start()
        # set root logger handlers
        root = logging.getLogger()
        root.addHandler(QueueHandler(self.queue))
        root.setLevel(logging.DEBUG)

    def __del__(self):
        self.stop()

    def start(self):
        self.process.start()

    def stop(self):
        self.queue.put(None)
        self.process.join(timeout=60)

    @staticmethod
    def get_logger(module):
        if not isinstance(module, (str, unicode)):
            module = module.__class__.__name__
        return logging.getLogger(module)