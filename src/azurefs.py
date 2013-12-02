'''
Created on Nov 2, 2013

@author: Administrator
'''
import os, string
from azure.storage import *
from Crypto.Cipher import AES
from fec import CYPHER_IV,process_cipherkey

class AZUREFS():
    
    def __init__(self):
        self.blob_service = BlobService(account_name=os.environ['AZURE_ACCOUNT'], account_key=os.environ['AZURE_KEY'])

    
    def ensure_get_bucket(self, bucket_name):
        try:
            # self.blob_service.get_container_acl(bucket_name)
            self.blob_service.create_container(bucket_name)
            # print 'create container ' + bucket_name + ' successfully'
        except IOError:
            print 'the container exists'
        
    def write(self, bucket, filename, data):
        # print 'try write to azure with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            # print '----------write data----------'
            # print data
            self.ensure_get_bucket(bucket)
            self.blob_service.put_blob(bucket, filename, data, x_ms_blob_type='BlockBlob')
        except IOError:
            print 'Azure Cannot write file!'
            
    def read(self, bucket, filename):
        # print 'try read from azure with %s, %s' % (bucket,filename)
        try:
            data = self.blob_service.get_blob(bucket, filename)
            return data
        except IOError:
            print 'Cannot read file!'

    def get_file_maxver(self, bucket, shortname):
        try:
            # print 'try read files in azure with %s, %s' % (bucket,shortname)
            self.ensure_get_bucket(bucket)
            blobs = self.blob_service.list_blobs(bucket)
            ver = 0
            cur_filename = ''
            for blob in blobs:
                # print blob.name
                if string.find(blob.name, shortname) == 0:
                    secs = string.split(blob.name, '_')
                    cur_ver = int(secs[-1])
                    if cur_ver > ver:
                        ver = cur_ver
                        cur_filename = blob.name
            # print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except IOError:
            print 'cannot list filenames from azure'
            return ''

    def get_all_files(self, root):
        try:
            files = []
            rs = self.blob_service.list_containers()
            for b in rs:
                if not os.path.exists(root + b.name):
                    os.makedirs(root + b.name)

                bucket_dic, map_path = self.get_files_from_bucket(b)
                if map_path != '':
                    print map_path
                    cipher = AES.new(process_cipherkey('password'), AES.MODE_ECB, CYPHER_IV)
                    blob = self.blob_service.get_blob(b.name, map_path)
                    data = cipher.decrypt(blob)
                    print data

                for prefix in bucket_dic:
                    filename = b.name + '/' + prefix + '_' + bucket_dic[prefix][0] + '_' + str(bucket_dic[prefix][1])
                    files.append(filename)
                    print filename
            return files  
        except IOError:
            print 'Cannot get azure files'

    def get_files_from_bucket(self, bucket):
        try:
            dic = dict()
            map_path = ''
            blob_list = self.blob_service.list_blobs(bucket.name)
            for blob in blob_list:
                filename = blob.name.split('_')
                prefix = filename[0]
                if prefix.split('/')[-1] == '.bucketmap':
                    if map_path == '':
                        map_path = blob.name
                    else:
                        if int(map_path.split('_')[-1]) < filename[-1]:
                            map_path = blob.name
                else:
                    meta = filename[1]
                    version = int(filename[2])
                    if prefix in dic:
                        if dic[prefix][1] < version:
                            dic[prefix] = [meta, version]
                    else:
                        dic[prefix] = [meta, version]
            return dic, map_path
        except IOError:
            print 'Cannot get azure files'

    def delete(self, bucket_name, prefix_name):
        try:
            blob_list = self.blob_service.list_blobs(bucket_name, prefix=prefix_name)
            for blob in blob_list:
                self.blob_service.delete_blob(bucket_name, blob.name)
            print 'Delete azure file /' + bucket_name + '/' + prefix_name + ' successfully!'
        except IOError:
            print 'Cannot delete azure file /' + bucket_name + '/' + prefix_name


    def delete_bucket(self, bucket_name):
        try:
            self.blob_service.delete_container(bucket_name)
            print 'Delete azure folder /' + bucket_name + ' successfully'
        except IOError:
            print 'Cannot delete azure folder /' + bucket_name


    def clean(self):
        try:
            rs = self.blob_service.list_containers()
            for b in rs:
                self.blob_service.delete_container(b.name)
            print 'Clean azure storage successfully'
        except IOError:
            print 'Cannot clean azure storage storage'


# azurefs = AZUREFS()
# azurefs.get_all_files('disk_local')




