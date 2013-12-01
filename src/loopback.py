#!/usr/bin/env python

from __future__ import with_statement

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from time import gmtime, strftime

class Loopback(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()

    def __call__(self, op, path, *args):
        return super(Loopback, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        # print 'access' + strftime("%H:%M:%S", gmtime())
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        # print 'create' + strftime("%H:%M:%S", gmtime())
        return os.open(path, os.O_WRONLY | os.O_CREAT, mode)

    def flush(self, path, fh):
        # print 'flush' + strftime("%H:%M:%S", gmtime())
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        # print 'fsync' + strftime("%H:%M:%S", gmtime())
        return os.fsync(fh)

    def getattr(self, path, fh=None):
        # print 'getattr' + strftime("%H:%M:%S", gmtime())
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
            'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    getxattr = None

    def link(self, target, source):
        # print 'link' + strftime("%H:%M:%S", gmtime())
        return os.link(source, target)

    def unlink(self, path):
        print '-----------unlink'
        return os.remove(path)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod
    open = os.open

    def read(self, path, size, offset, fh):
        # print 'read' + strftime("%H:%M:%S", gmtime())
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.read(fh, size)

    def readdir(self, path, fh):
        # print 'readdir' + strftime("%H:%M:%S", gmtime())
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, fh):
        # print 'release' + strftime("%H:%M:%S", gmtime())
        return os.close(fh)

    def rename(self, old, new):
        # print 'rename' + strftime("%H:%M:%S", gmtime())
        return os.rename(old, self.root + new)

    rmdir = os.rmdir

    def statfs(self, path):
        # print 'statfs' + strftime("%H:%M:%S", gmtime())
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        # print 'symlink' + strftime("%H:%M:%S", gmtime())
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        # print 'truncate' + strftime("%H:%M:%S", gmtime())
        with open(path, 'r+') as f:
            f.truncate(length)

    # unlink = os.unlink
    utimens = os.utime

    def write(self, path, data, offset, fh):
        # print 'write' + strftime("%H:%M:%S", gmtime())
        with self.rwlock:
            os.lseek(fh, offset, 0)
            return os.write(fh, data)


if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    fuse = FUSE(Loopback(argv[1]), argv[2], foreground=True)
