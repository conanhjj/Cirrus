# encoding: UTF-8
import os
import threading
import time
from cloud import CloudFS


class SyncThread(threading.Thread):

    def __init__(self, file_dir, cloudFS):
        super(SyncThread, self).__init__()
        self.root_dir = file_dir
        self.stop_sync = False
        self.cloud = cloudFS

    def run(self):
        while True:
            if self.stop_sync:
                break

            self.sync(self.root_dir)


    def sync(self, file_name):
        if os.path.isdir(file_name):
            files = os.listdir(file_name)
            parent_directory = file_name + "/"

            for each_file in files:
                self.sync(parent_directory + each_file)
        else:
            #check local file_name and remote file
            if not self.is_same(file_name):
                print "Push local file to cloud: " + file_name
                self.cloud.write(file_name, open(file_name).read())

    def is_same(self, local_file_name):
        local_time = os.stat(local_file_name).st_mtime
        print local_time
        remote_time = self.cloud.get_remote_time(local_file_name)
        return local_time == remote_time

    def stop(self):
        self.stop_sync = True


class FakeCloud:

    def __int__(self):
        pass

    def get_remote_time(self, file_name):
        print file_name
        return 111

    def write(self, filename, data):
        print filename, data

if __name__ == "__main__":
    t = SyncThread("/Users/Wind/cirrus", FakeCloud())
    t.start()
    time.sleep(3)
    t.stop()
