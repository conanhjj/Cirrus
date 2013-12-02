#!/usr/bin/env python

from __future__ import with_statement

from errno import EACCES
from os.path import realpath
from sys import argv, exit
from threading import Lock

import os
import sys
import time

import cloud

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from sync import SyncThread


class Cirrus(LoggingMixIn, Operations):
    def __init__(self, root):
        self.root = realpath(root)
        self.rwlock = Lock()
        self.cloud = cloud.CloudFS(self.root)
        self.local_sync = SyncThread(self.root, self.cloud)
        self.local_sync.sync(self.root)
        print "[CFS][Sync]Local file system check and sync finishes!"
        print "[CFS][Core]Cirrus file system is ready to use!"
        print "[CFS][Core]Press Ctrl+C to stop Cirrus file system!"

    def __call__(self, op, path, *args):
        return super(Cirrus, self).__call__(op, self.root + path, *args)

    def access(self, path, mode):
        if not os.access(path, mode):
            raise FuseOSError(EACCES)

    chmod = os.chmod
    chown = os.chown

    def create(self, path, mode):
        return os.open(path, os.O_WRONLY | os.O_CREAT, mode)

    def flush(self, path, fh):
        return os.fsync(fh)

    def fsync(self, path, datasync, fh):
        return os.fsync(fh)

    def getattr(self, path, fh=None):
        st = os.lstat(path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                        'st_uid'))

    getxattr = None

    def link(self, target, source):
        return os.link(source, target)

    listxattr = None
    mkdir = os.mkdir
    mknod = os.mknod
    open = os.open

    def read(self, path, size, offset, fh):
        # print 'read: ' + path
        with self.rwlock:
            os.lseek(fh, offset, 0)
            localdata = os.read(fh, size)
            # clouddata = self.cloud.read(path, size, offset)
            # if clouddata != localdata:
            #     print('Cirrus Read: local data of %s is different to cloud data' % path)
            # else:
            #     print('Cirrus Read: Local data of %s is verified by cloud data' % path)
            return localdata

    def readdir(self, path, fh):
        return ['.', '..'] + os.listdir(path)

    readlink = os.readlink

    def release(self, path, fh):
        return os.close(fh)

    def rename(self, old, new):
        return os.rename(old, self.root + new)

    def rmdir(self, path):
        self.cloud.rmdir(path)
        return os.rmdir(path)

    def statfs(self, path):
        stv = os.statvfs(path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def symlink(self, target, source):
        return os.symlink(source, target)

    def truncate(self, path, length, fh=None):
        # print "--------->local truncate called %d,%s\n" % (length, path)
        with open(path, 'r+') as f:
            f.truncate(length)
            
    def unlink(self, path):
        if path.rfind('/.') == -1:
            print '----------delete: ' + path 
            self.cloud.delete(path)
        return os.unlink(path)

    utimens = os.utime

    def write(self, path, data, offset, fh):
        # print "--------->local write called %s\n" % path
        with self.rwlock:
            os.lseek(fh, offset, 0)
            localwriteret = os.write(fh, data)
        if path.rfind('/.') == -1:
            print '----------write: ' + path
            with open(path, 'r') as f:
                'query the new file size'
                newsize = os.stat(path).st_size
                f.seek(0, 0)
                newdata = f.read(newsize)
                self.cloud.write(path, newdata)
        return localwriteret

    def start_sync(self):
        self.local_sync.start()

    def stop_sync(self):
        self.local_sync.stop()

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)
    'check whether the paty contains splash '
    if os.path.basename(argv[1]) == '':
        cirrus_path = argv[1][:-1]
    else:
        cirrus_path = argv[1]
    'Create the local back store if not exist'
    cirrus_localpath = cirrus_path +"_local"
    if not os.path.exists(cirrus_localpath):
        os.makedirs(cirrus_localpath)

    cirrus = Cirrus(cirrus_localpath)
    #cirrus.start_sync()
    try:
        fuse = FUSE(cirrus, cirrus_path, foreground=True)
    except KeyboardInterrupt:
        #cirrus.stop_sync()
        sys.exit()
    #cirrus.stop_sync()

