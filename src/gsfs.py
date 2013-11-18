'''
Created on Nov 2, 2013

@author: Administrator
'''
import os, string
import boto
from gslib.third_party.oauth2_plugin import oauth2_plugin

# URI scheme for Google Cloud Storage.
GOOGLE_STORAGE = 'gs'

class GSFS():
    
#    def __init__(self):       
    
    def ensure_get_bucket(self, bucket_name):
        uri=boto.storage_uri(bucket_name, GOOGLE_STORAGE)
        bucket = uri.connect().lookup(bucket_name)
        if bucket == None:
            try:
                bucket = uri.create_bucket()
                print "gs create bucket done!"
            except boto.exception.StorageCreateError, e:
                print "gs create bucket failed"
                
        return bucket
        
    def write(self, bucket, filename, data):
        print 'try write to s3 with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            self.ensure_get_bucket(bucket)
            uri = boto.storage_uri(bucket + '/' + filename, GOOGLE_STORAGE)
            uri.new_key().set_contents_from_string(data)
        except IOError:
            print 'GS Cannot write file!'
            
    def read(self, bucket, filename):
        print 'try read from gs with %s, %s' % (bucket,filename)
        try:
            uri = boto.storage_uri(bucket + '/' + filename, GOOGLE_STORAGE)
            data = uri.get_key().get_contents_as_string()
            return data
        except IOError:
            print 'Cannot read file!'

    def get_file_maxver(self, bucket, shortname):
        try:
            print 'try read files in gs with %s, %s' % (bucket,shortname)
            bucket = self.ensure_get_bucket(bucket)
            key_list = bucket.get_all_keys(prefix=shortname)
            ver = 0
            cur_filename = ''
            for k in key_list:
                #print k.name
                secs = string.split(k.name, '_')
                cur_ver = int(secs[-1])
                if cur_ver > ver:
                    ver = cur_ver
                    cur_filename = k.name
            print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except IOError:
            print 'cannot list filenames from s3'
            return ''