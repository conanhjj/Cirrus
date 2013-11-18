'''
Created on Nov 2, 2013

@author: Administrator
'''
import os, string
from azure.storage import *

class AZUREFS():
    
    def __init__(self):
        self.blob_service = BlobService(account_name=os.environ['AZURE_ACCOUNT'], account_key=os.environ['AZURE_KEY'])

    
    def ensure_get_bucket(self, bucket_name):
        try:
            self.blob_service.get_container_acl(bucket_name)
        except:
            #the container is not exists.
            self.blob_service.create_container(bucket_name)
        
    def write(self, bucket, filename, data):
        print 'try write to azure with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            self.ensure_get_bucket(bucket)
            self.blob_service.put_blob(bucket, filename, data, x_ms_blob_type='BlockBlob')
        except IOError:
            print 'Azure Cannot write file!'
            
    def read(self, bucket, filename):
        print 'try read from azure with %s, %s' % (bucket,filename)
        try:
            data = self.blob_service.get_blob(bucket, filename)
            return data
        except IOError:
            print 'Cannot read file!'

    def get_file_maxver(self, bucket, shortname):
        try:
            print 'try read files in azure with %s, %s' % (bucket,shortname)
            self.ensure_get_bucket(bucket)
            blobs = self.blob_service.list_blobs(bucket)
            ver = 0
            cur_filename = ''
            for blob in blobs:
                print blob.name
                if string.find(blob.name, shortname) == 0:
                    secs = string.split(blob.name, '_')
                    cur_ver = int(secs[-1])
                    if cur_ver > ver:
                        ver = cur_ver
                        cur_filename = blob.name
            print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except IOError:
            print 'cannot list filenames from azure'
            return ''