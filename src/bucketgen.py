__author__ = 'Wind'

import os
import base64, md5

class BucketGenerator:

    def __init__(self, bucket_file="bucket"):
        self.bucket2dir = {}
        self.dir2bucket = {}
        self.bucket_file = bucket_file
        self.load_bucket_file()

    def load_bucket_file(self):
        try:
            with open(self.bucket_file, 'r') as f:
                print "----------------"
                print "load bucket from file", self.bucket_file

                mode = 0
                for line in f:
                    line = line[:-1]
                    if line == "bucket to dir":
                        mode = 1
                        continue
                    if line == "dir to bucket":
                        mode = 2
                        continue

                    if mode == 1:
                        bucket, file_dir_list = line.split(" ")
                        file_dir_list = eval(file_dir_list)
                        print file_dir_list
                        self.bucket2dir[bucket] = file_dir_list
                    elif mode == 2:
                        file_dir, bucket = line.split(" ")
                        self.dir2bucket[file_dir] = bucket
                    else:
                        raise ValueError("Illegal bucket file")

            print self.bucket2dir
            print self.dir2bucket
            print "finish loading"
            print "----------------"

        except IOError as e:
            if e.errno != 2:    # ignore no such file
                print e

    def flush_to_disk(self):
        print "----------------"
        print "flush bucket to disk"
        with open(self.bucket_file, 'w') as f:
            print "write bucket2dir:", self.bucket2dir
            f.write("bucket to dir\n")
            for (bucket, dir_list) in self.bucket2dir.items():
                f.write("%s [%s]\n" % (bucket, ','.join([str("'" + x + "'") for x in dir_list])))

            print "write dir2bucket:", self.dir2bucket
            f.write("dir to bucket\n")
            for (file_dir, bucket) in self.dir2bucket.items():
                f.write("%s %s\n" % (file_dir, bucket))
        print "finish flushing"
        print "----------------"

    def get_bucket(self, file_dir):
        if not file_dir in self.dir2bucket:
            bucket = self.__gen(file_dir)
            self.dir2bucket[file_dir] = bucket

            if not bucket in self.bucket2dir:
                self.bucket2dir[bucket] = []
            self.bucket2dir[bucket].append(file_dir)

        return self.dir2bucket[file_dir]

    def remove_file(self):
        try:
            os.remove(self.bucket_file)
        except IOError:
            pass

    @staticmethod
    def __gen(file_dir):
        m = md5.new()
        m.update(file_dir)
        #s3 boto doesn't support upper case. And also not allow "="
        # just replace the padding
        return base64.b32encode(m.digest())[0:-6].lower() #"cirrus_bucket"

if __name__ == "__main__":
    bucket_gen = BucketGenerator("new_bucket_file")
    print "get bucket:", "123/", bucket_gen.get_bucket("123/")
    print "get bucket:", "456/", bucket_gen.get_bucket("456/")
    print "get bucket:", "789/", bucket_gen.get_bucket("789/")
    bucket_gen.flush_to_disk()
    bucket_gen.remove_file()
