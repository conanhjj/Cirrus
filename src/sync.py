# encoding: UTF-8
import os
import threading
import time
import sys
from fileutil import FileUtil
from cloud import CloudFS


class SyncThread(threading.Thread):

    def __init__(self, local_dir, cloudfs):
        super(SyncThread, self).__init__()
        self.root_dir = local_dir
        self.stop_sync = False
        self.cloud = cloudfs

    def run(self):
        print "local sync thread start", self.root_dir
        self.sync(self.root_dir)

    def sync(self, local_file_name):
        fuse_file_name = local_file_name.replace("_local", "")
        print "sync thread check:", local_file_name

        if os.path.isdir(local_file_name):
            files = os.listdir(local_file_name)
            parent_directory = local_file_name + "/"

            for each_file in files:
                self.sync(parent_directory + each_file)
        else:
            #check local file_name and remote file
            if SyncThread.is_hidden_file(local_file_name):
                pass
            elif not self.is_same(local_file_name, fuse_file_name):
                print "Push different local file to cloud: " + local_file_name
                try:
                    self.cloud.write(fuse_file_name, open(local_file_name).read())
                except Exception, ex:
                    print ex
                    print "Encounter error in push"

    def is_same(self, local_file_name, fuse_file_name):
        local_md5 = FileUtil.file_md5(local_file_name)
        remote_md5 = self.cloud.query_cloudfile_md5(fuse_file_name)
        #print "file name", local_file_name
        #print "local_md5", local_md5
        #print "remote_md5", remote_md5
        return local_md5 == remote_md5

    def stop(self):
        print "local sync thread stop"
        self.stop_sync = True

    @staticmethod
    def is_hidden_file(file_name):
        pos = file_name.rfind("/")
        return len(file_name) != pos+1 and file_name[pos+1] == "."


class FakeCloud:

    def __int__(self):
        pass

    @staticmethod
    def query_cloudfile_md5(file_name):
        print file_name
        return "111"

    @staticmethod
    def write(filename, data):
        print filename, data

if __name__ == "__main__":
    t = SyncThread("/Users/Wind/cirrus", FakeCloud())
    t.start()
    time.sleep(3)
    t.stop()
