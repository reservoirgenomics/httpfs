#!/usr/bin/env python
from errno import EIO, ENOENT
from stat import S_IFDIR, S_IFREG
from threading import Timer
from time import time
import functools as ft
import logging
import os
import sys

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import requests

BLOCK_SIZE = 2 ** 18

CLEANUP_INTERVAL = 60
CLEANUP_EXPIRED = 60

DISK_CACHE_SIZE_ENV = 'HTTPFS_DISK_CACHE_SIZE'
DISK_CACHE_DIR_ENV = 'HTTPFS_DISK_CACHE_DIR'

import collections
import diskcache as dc

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = collections.OrderedDict()

    def __getitem__(self, key):
        value = self.cache.pop(key)
        self.cache[key] = value
        return value

    def __setitem__(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = value

    def __contains__(self, key):
        return key in self.cache

    def __len__(self):
        return len(self.cache)


class HttpFs(LoggingMixIn, Operations):
    """
    A read only http/https/ftp filesystem.

    """
    def __init__(self, _schema):
        self.schema = _schema
        self.files = dict()
        self.cleanup_thread = self._generate_cleanup_thread(start=False)
        self.lru_cache = LRUCache(capacity=400)

        size_limit = 2**30 # 1Gb default size limit
        cache_dir = '/tmp/diskcache'
        

        if DISK_CACHE_SIZE_ENV in os.environ:
            print("setting max size:", int(os.environ[DISK_CACHE_SIZE_ENV]))
            size_limit=int(os.environ[DISK_CACHE_SIZE_ENV])
                
        if DISK_CACHE_DIR_ENV in os.environ:
            print("setting cache directory:", os.environ[DISK_CACHE_DIR_ENV])
            cache_dir = os.environ[DISK_CACHE_DIR_ENV]

        self.disk_cache = dc.Cache(cache_dir, size_limit)

        self.lru_hits = 0
        self.lru_misses = 0

    def init(self, path):
        self.cleanup_thread.start()

    def getattr(self, path, fh=None):
        #logging.info("attr path: {}".format(path))
        
        if path in self.files:
            return self.files[path]['attr']

        elif path.endswith('..'):
            url = '{}:/{}'.format(self.schema, path[:-2])
            
            # logging.info("attr url: {}".format(url))
            head = requests.head(url, allow_redirects=True)
            # logging.info("head: {}".format(head.headers))
            # logging.info("status_code: {}".format(head.status_code))

            attr = dict(
                st_mode=(S_IFREG | 0o644), 
                st_nlink=1,
                st_size=int(head.headers['Content-Length']),
                st_ctime=time(), 
                st_mtime=time(),
                st_atime=time())
            
            self.files[path] = dict(
                time=time(), 
                attr=attr)
            return attr

        else:
            return dict(st_mode=(S_IFDIR | 0o555), st_nlink=2)

    def read(self, path, size, offset, fh):
        #logging.info("read path: {}".format(path))
        if path in self.files:
            url = '{}:/{}'.format(self.schema, path[:-2])
            logging.info("read url: {}".format(url))
            logging.info("offset: {} - {} block: {}".format(offset, offset + size - 1, offset // 2 ** 18))
            output = [0 for i in range(size)]

            t1 = time()

            # nothing fetched yet
            last_fetched = -1
            curr_start = offset

            while last_fetched < offset + size:
                #print('curr_start', curr_start)
                block_num = curr_start // BLOCK_SIZE
                block_start = BLOCK_SIZE * (curr_start // BLOCK_SIZE)

                #print("block_num:", block_num, "block_start:", block_start)
                block_data = self.get_block(url, block_num)

                data_start = curr_start - (curr_start // BLOCK_SIZE) * BLOCK_SIZE
                data_end = min(BLOCK_SIZE, offset + size - block_start)

                data = block_data[data_start:data_end]

                #print("data_start:", data_start, data_end, data_end - data_start)
                for (j,d) in enumerate(data):
                    output[curr_start-offset+j] = d

                last_fetched = curr_start + (data_end - data_start)
                curr_start += (data_end - data_start)

            t2 = time()

            # logging.info("sending request")
            # logging.info(url)
            # logging.info(headers)
            logging.info("num hits: {} misses: {}"
                    .format(self.lru_hits, self.lru_misses))

            self.files[path]['time'] = t2  # extend life of cache entry

            logging.info("time: {:.2f}".format(t2 - t1))
            return bytes(output)
            
        else:
            logging.info("file not found")
            raise FuseOSError(EIO)

    def destroy(self, path):
        self.cleanup_thread.cancel()

    def cleanup(self):
        now = time()
        num_files_before = len(self.files)
        self.files = {
            k: v for k, v in self.files.items() 
                if now - v['time'] < CLEANUP_EXPIRED
        }
        num_files_after = len(self.files)
        if num_files_before != num_files_after:
            logging.info(
                'Truncated cache from {} to {} files'.format(
                    num_files_before, num_files_after))
        self.cleanup_thread = self._generate_cleanup_thread()

    def _generate_cleanup_thread(self, start=True):
        cleanup_thread = Timer(CLEANUP_INTERVAL, self.cleanup)
        cleanup_thread.daemon = True
        if start:
            cleanup_thread.start()
        return cleanup_thread

    def get_block(self, url, block_num):
        '''
        Get a data block from a URL. Blocks are 256K bytes in size

        Parameters:
        -----------
        url: string
            The url of the file we want to retrieve a block from
        block_num: int
            The # of the 256K'th block of this file
        '''
        cache_key=  "{}.{}".format(url, block_num)
        cache = self.disk_cache

        if cache_key in cache:
            self.lru_hits += 1
            return cache[cache_key]
        else:
            self.lru_misses += 1
            block_start = block_num * BLOCK_SIZE
            
            headers = {
                'Range': 'bytes={}-{}'.format(block_start, block_start + BLOCK_SIZE - 1)
            }
            r = requests.get(url, headers=headers)
            block_data = r.content
            cache[cache_key] = block_data

        return block_data


def main():
    import argparse
    parser = argparse.ArgumentParser(description="""
    usage: httpfs <mountpoint> <http|https|ftp>
""")
    parser.add_argument('mountpoint')
    parser.add_argument('schema')
    parser.add_argument(
        '-f', '--foreground', 
        action='store_true', 
        default=False,
    	help='Run in the foreground')
    args = vars(parser.parse_args())

    logging.getLogger().setLevel(logging.INFO)
    logging.info("starting:")
    logging.info("foreground: {}".format(args['foreground']))
    
    fuse = FUSE(
        HttpFs(args['schema']), 
        args['mountpoint'], 
        foreground=args['foreground']
    )


if __name__ == '__main__':
    main()

