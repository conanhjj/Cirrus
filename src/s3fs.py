'''
Created on Nov 2, 2013

@author: Administrator
'''
import os, string
import boto
from boto.s3.key import Key

class S3FS():
    
    def __init__(self):
        self.s3 = boto.connect_s3()
    
    def ensure_get_bucket(self, bucket_name):
        bucket = self.s3.lookup(bucket_name)
        if bucket == None:
            bucket = self.s3.create_bucket(bucket_name)
            # print 'create new bucket in s3: %s' % bucket_name
        return bucket
        
    def write(self, bucket, filename, data):
        # print 'try write to s3 with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            bucket = self.ensure_get_bucket(bucket)
            k = Key(bucket)
            k.key = filename
            k.set_contents_from_string(data)
        except IOError:
            print 'Cannot write file!'
            
    def read(self, bucket, filename):
        # print 'try read from s3 with %s, %s' % (bucket,filename)
        try:
            bucket = self.s3.get_bucket(bucket)
            k = Key(bucket)
            k.key = filename
            data =  k.get_contents_as_string()
            return data
        except IOError:
            print 'Cannot read file!'

    def get_file_maxver(self, bucket, shortname):
        try:
            # print 'try read files in s3 with %s, %s' % (bucket,shortname)
            bucket = self.ensure_get_bucket(bucket)
            key_list = bucket.get_all_keys(prefix=shortname)
            ver = 0
            cur_filename = ''
            for k in key_list:
                ## print k.name
                secs = string.split(k.name, '_')
                cur_ver = int(secs[-1])
                if cur_ver > ver:
                    ver = cur_ver
                    cur_filename = k.name
            # print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except IOError:
            print 'Cannot list filenames from s3'
            return ''

    def get_all_files(self):
        try:
            rs = self.s3.get_all_buckets()
            files = []
            for b in rs:
                bucket_dic = self.get_files_from_bucket(b)
                for prefix in bucket_dic:
                    filename = b.name + '/' + prefix + '_' + bucket_dic[prefix][0] + '_' + str(bucket_dic[prefix][1])
                    files.append(filename)
                    print filename
            return files  
        except IOError:
            print 'Cannot get s3 files'

    def get_files_from_bucket(self, bucket):
        try:
            dic = dict()
            key_list = bucket.get_all_keys()
            for k in key_list:
                filename = k.name.split('_')
                prefix = filename[0]
                meta = filename[1]
                version = int(filename[2])
                if prefix in dic:
                    if dic[prefix][1] < version:
                        dic[prefix] = [meta, version]
                else:
                    dic[prefix] = [meta, version]
            return dic
        except IOError:
            print 'Cannot get s3 files'

    def delete(self, bucket_name, prefix_name):
        try:
            bucket = self.s3.lookup(bucket_name)
            key_list = bucket.get_all_keys(prefix=prefix_name)
            for k in key_list:
                k.delete()
            print 'Delete s3 file /' + bucket_name + '/' + prefix_name + ' successfully!'
        except IOError:
            print 'Cannot delete s3 file /' + bucket_name + '/' + prefix_name

    def delete_bucket(self, bucket_name):
        try:
            bucket = self.s3.lookup(bucket_name)
            if bucket is not None:
                for key in bucket.list():
                    key.delete()
                self.s3.delete_bucket(bucket)
                # print 'Delete ' + bucket_name + ' successfully'
        except IOError:
            print 'Cannot delete the bucket ' + bucket_name

    def clean(self):
        try:
            rs = self.s3.get_all_buckets()
            for b in rs:
                self.delete_bucket(b.name)
            print 'Clean s3 storage successfully'
        except IOError:
            print 'Cannot clean s3 storage'


s3fs = S3FS()
s3fs.get_all_files()







