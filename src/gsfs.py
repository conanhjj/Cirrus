'''
Created on Nov 2, 2013

@author: Administrator
'''
import os, string
import boto
from gslib.third_party.oauth2_plugin import oauth2_plugin
from Crypto.Cipher import AES
from fec import CYPHER_IV,process_cipherkey

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
                # print "gs create bucket done!"
            except boto.exception.StorageCreateError, e:
                print "gs create bucket failed"
                
        return bucket
        
    def write(self, bucket, filename, data):
        # print 'try write to s3 with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            self.ensure_get_bucket(bucket)
            uri = boto.storage_uri(bucket + '/' + filename, GOOGLE_STORAGE)
            uri.new_key().set_contents_from_string(data)
        except IOError:
            print 'GS Cannot write file!'
            
    def read(self, bucket, filename):
        # print 'try read from gs with %s, %s' % (bucket,filename)
        try:
            uri = boto.storage_uri(bucket + '/' + filename, GOOGLE_STORAGE)
            data = uri.get_key().get_contents_as_string()
            return data
        except IOError:
            print 'Cannot read file!'

    def get_file_maxver(self, bucket, shortname):
        try:
            # print 'try read files in gs with %s, %s' % (bucket,shortname)
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
            print 'cannot list filenames from s3'
            return ''

    def get_all_files(self):
        try:
            uri = boto.storage_uri('', GOOGLE_STORAGE)
            files = []
            bucketmap = dict()
            for b in uri.get_all_buckets():
                bucket_dic, map_path = self.get_files_from_bucket(b)
                if map_path != '':
                    # print map_path
                    cipher = AES.new(process_cipherkey('password'), AES.MODE_ECB, CYPHER_IV)
                    uri = boto.storage_uri(b.name + '/' + map_path, GOOGLE_STORAGE)
                    data = cipher.decrypt(uri.get_key().get_contents_as_string())
                    for line in data.split('\n'):
                        # print line
                        if line == 'bucket to dir':
                            continue
                        elif line == 'dir to bucket':
                            break
                        else:
                            dirs = line.split()
                            bucketmap[dirs[0]] = eval(dirs[1])

                for prefix in bucket_dic:
                    filename = b.name + '/' + prefix + '_' + bucket_dic[prefix][0] + '_' + str(bucket_dic[prefix][1])
                    files.append(filename)
                    
            return bucketmap, files 
        except IOError:
            print 'Cannot get google files'

    def get_files_from_bucket(self, bucket):
        try:
            dic = dict()
            map_path = ''
            key_list = bucket.get_all_keys()
            for k in key_list:
                filename = k.name.split('_')
                prefix = filename[0]
                if prefix.split('/')[-1] == '.bucketmap':
                    if map_path == '':
                        map_path = k.name
                    else:
                        if int(map_path.split('_')[-1]) < filename[-1]:
                            map_path = k.name
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
            print 'Cannot get google files'

    def delete(self, bucket_name, prefix_name):
        try:
            uri = boto.storage_uri(bucket_name, GOOGLE_STORAGE)
            bucket = uri.get_bucket()
            key_list = bucket.get_all_keys(prefix=prefix_name)
            for k in key_list:
                # print 'Delete ' + k.name
                k.delete()
            print 'Delete google file /' + bucket_name + '/' + prefix_name + ' successfully!'
        except IOError:
            print 'Cannot delete google file /' + bucket_name + '/' + prefix_name

    def delete_bucket(self, bucket_name):
        try:
            uri = boto.storage_uri(bucket_name, GOOGLE_STORAGE)
            for obj in uri.get_bucket():
                obj.delete()
            # print 'Deleting bucket: %s...' % uri.bucket_name
            uri.delete_bucket()
        except IOError:
            print 'Cannot delete the bucket ' + bucket_name

    def clean(self):
        try:
            uri = boto.storage_uri('', GOOGLE_STORAGE)
            for b in uri.get_all_buckets():
                self.delete_bucket(b.name)
            print 'Clean Google cloud storage successfully'
        except IOError:
            print 'Cannot clean Google cloud storage'


# gsfs = GSFS()
# gsfs.get_all_files('disk_local')

