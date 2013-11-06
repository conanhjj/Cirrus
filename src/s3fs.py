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
            print 'create new bucket in s3: %s' % bucket_name
        return bucket
        
    def write(self, bucket, filename, data):
        print 'try write to s3 with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            bucket = self.ensure_get_bucket(bucket)
            k = Key(bucket)
            k.key = filename
            k.set_contents_from_string(data)
        except IOError:
            print 'Cannot write file!'
            
    def read(self, bucket, filename):
        print 'try read from s3 with %s, %s' % (bucket,filename)
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
            print 'try read files in s3 with %s, %s' % (bucket,shortname)
            bucket = self.ensure_get_bucket(bucket)
            key_list = bucket.get_all_keys(prefix='shortname')
            ver = 0
            cur_filename = ''
            for k in key_list:
                print k.name
                secs = string.split(k, '_')
                cur_ver = int(secs[-1])
                if cur_ver > ver:
                    ver = cur_ver
                    cur_filename = k
            print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except IOError:
            print 'cannot list filenames from s3'
            return ''