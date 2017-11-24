#!/usr/bin/env python

from errno import EIO, ENOENT
import logging
from stat import S_IFDIR, S_IFREG
from sys import argv, exit
from threading import Timer
from time import time

import requests
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

CLEANUP_INTERVAL = 60
CLEANUP_EXPIRED = 60

class HttpFs(LoggingMixIn, Operations):
    """A read only http/https/ftp filesystem."""

    def __init__(self, _schema):
        self.schema = _schema

        self.files = dict()
        self.cleanup_thread = self._generate_cleanup_thread(start=False)

    def init(self, path):
        self.cleanup_thread.start()

    def getattr(self, path, fh=None):
        #logging.info("getattr: {}".format(path))

        if path in self.files:
            return self.files[path]['attr']

        if path.endswith('..'):
            url = '%s:/%s' % (self.schema, path[:-2])
            #headers={'Range': 'bytes=0-1000'}
            logging.info("attr url: {}".format(url))
            head = requests.head(url)
            logging.info("head: {}".format(head.headers))
            #headers={'Range': 'bytes=0-1000'}
            #r = requests.get(url, headers=headers)
            #logging.info("status_code: {}".format(r.status_code))
                #content = r.content
            attr = dict(st_mode=(S_IFREG | 0o644), st_nlink=1,
                        st_size=int(head.headers['Content-Length']),
                        st_ctime=time(), st_mtime=time(),
                        st_atime=time())
            self.files[path] = dict(time=time(), attr=attr)
            return attr
        else:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

    def read(self, path, size, offset, fh):
        logging.info("path: {}".format(path))

        url = '%s:/%s' % (self.schema, path[:-2])
        logging.info("read url: {}".format(url))


        if self.files[path]:
            headers={'Range': 'bytes={}-{}'.format(offset,offset+size-1)}
            logging.info("sending request")
            t1 = time()
            r = requests.get(url, headers=headers)
            t2 = time()
            #logging.info("received request: {}".format(r.status_code))
            #return r.content
            logging.info('content: {} time: {}'.format(len(r.content), t2 - t1))
            return r.content
            #return self.files[path]['content'][offset:offset + size]
        raise FuseOSError(EIO)

    def destroy(self, path):
        self.cleanup_thread.cancel()

    def cleanup(self):
        now = time()
        num_files_before = len(self.files)
        self.files = {k: v for k, v in self.files.items() if now - v['time'] < CLEANUP_EXPIRED}
        num_files_after = len(self.files)
        if num_files_before != num_files_after:
            #logging.debug('Truncated cache from %d to %d files' % (num_files_before, num_files_after))
            pass
        self.cleanup_thread = self._generate_cleanup_thread()

    def _generate_cleanup_thread(self, start=True):
        cleanup_thread = Timer(CLEANUP_INTERVAL, self.cleanup)
        cleanup_thread.daemon = True

        if start:
            cleanup_thread.start()

        return cleanup_thread

import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description="""
    usage: httpfs <mountpoint> <http|https|ftp>
""")

    parser.add_argument('mountpoint')
    parser.add_argument('schema')
    #parser.add_argument('-o', '--options', default='yo',
    #					 help="Some option", type='str')

    parser.add_argument('-f', '--foreground', action='store_true', 
                        default=False,
    		        help='Run in the foreground')

    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)
    logging.debug("Starting...")
    logging.info("starting:")

    print("foreground:", args.foreground)
    fuse = FUSE(HttpFs(args.schema), args.mountpoint, foreground=args.foreground)

if __name__ == '__main__':
    main()
