# encoding: UTF-8
import os
import threading
import time
from fileutil import FileUtil
from cloud import CloudFS


class SyncThread(threading.Thread):

    def __init__(self, file_dir, cloudfs):
        super(SyncThread, self).__init__()
        self.root_dir = file_dir
        self.stop_sync = False
        self.cloud = cloudfs

    def run(self):
        print "local sync thread start"
        while True:
            if self.stop_sync:
                break

            self.sync(self.root_dir)
            time.sleep(10)

    def sync(self, file_name):
        if os.path.isdir(file_name):
            files = os.listdir(file_name)
            parent_directory = file_name + "/"

            for each_file in files:
                self.sync(parent_directory + each_file)
        else:
            #check local file_name and remote file
            right_slash_pos = file_name.rfind("/")
            if file_name[right_slash_pos+1] == ".":   # ignore dot file
                pass

            if not self.is_same(file_name):
                print "Push different local file to cloud: " + file_name
                try:
                    self.cloud.write(file_name, open(file_name).read())
                except:
                    print "Encounter error in push"

    def is_same(self, local_file_name):
        local_md5 = FileUtil.file_md5(local_file_name)
        remote_md5 = self.cloud.query_cloudfile_md5(local_file_name)
        return local_md5 == remote_md5

    def stop(self):
        print "local sync thread stop"
        self.stop_sync = True


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
