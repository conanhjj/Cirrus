'''
Created on Oct 27, 2013

A proxy for store the back

'''
import string
import fec
import fileutil
import bucketgen
import dropboxfs
import s3fs
import gsfs
import azurefs
import os
import time

def calc_rate(byte_size, time_in_s):
    return float(byte_size)/1024/1024/time_in_s

class CloudFS:
    def __init__(self, local_path):
        'initialize the cloud storage'
        'The below is used for fec encode/decode'
        self.local_path = local_path
        self.k = 2 # two pieces
        self.m = 3 # after encoding output three pieces
        # k, m, AES key
        self.encoder = fec.Encoder(self.k,self.m, 'password')
        self.decoder = fec.Decoder(self.k,self.m, 'password')
        self.dropbox = dropboxfs.DropboxFS()
        self.s3 = s3fs.S3FS()
        self.gs = gsfs.GSFS()
        # self.azure = azurefs.AZUREFS()
        self.clouds = [self.dropbox, self.s3, self.gs]
        # self.clean()
        self.bucketgen = bucketgen.BucketGenerator(os.path.join(local_path, '.bucketmap'), clouds = self.clouds, key = 'password')
    
    'input full path return paris of bucket name and encrypted shortfilename(no meta and ver)'
    def get_bucket_shortfilename(self, path):
        paths = fileutil.FileUtil.split_path(path)
        bucket = self.bucketgen.get_bucket(paths[0])
        filename = paths[1]
        filename_size = len(filename)
        
        encrypted_filename = self.decoder.get_encrypted_filename(filename)
        
        return bucket, encrypted_filename, filename_size

    'input full path, metastr, ver, return paris of bucket name and fullfilename'
    def get_bucket_fullfilename(self, path, metastr, ver):
        bucket, encrypted_filename, filename_size = self.get_bucket_shortfilename(path)
        filename = encrypted_filename + '_'+ metastr + '_' + str(ver)
        return bucket, filename
        
    def getmetastr(self, path):
        'query the file with the path and the max vers, and return the meta_str'
        bucket, shortfilename = self.get_bucket_shortfilename(path)
        # print "----query clouds for----bucket: %s, shortfilename: %s" % (bucket, shortfilename)
        fullname = shortfilename + '_test_meta_3'
        'split fullname by _'
        return string.split(fullname, '_')[-2] #-1 is v, -1 is meta

    def query_cloudfile_md5(self, path):
        bucket, shortname, filename_size = self.get_bucket_shortfilename(path)
        cur_filename, old_ver = self.dropbox.get_file_maxver(bucket, shortname)
        if cur_filename == '': #not found in cloud
            return '' # empty md5
        metastr = string.split(cur_filename, '_')[-2]
        fecmeta = self.decoder.decode_meta(metastr)
        return fecmeta.md5
    
    def read(self, path, size, offset):
        bucket, shortname, filename_size = self.get_bucket_shortfilename(path)
        
        'query the file with the path name and with the max version number'
        'we use dropbox as the key'
        cur_filename, old_ver = self.dropbox.get_file_maxver(bucket, shortname)
        metastr = string.split(cur_filename, '_')[-2]
        
        dropbox_block = self.dropbox.read(bucket, cur_filename)
        # print "dropbox content:\n%s" %  dropbox_block
        s3_block = self.s3.read(bucket, cur_filename)
        # print "s3 content:\n%s" %  s3_block
        #gs_block = self.gs.read(bucket, cur_filename)
        gs_block = ""
        #print "gs content:\n%s" % gs_block
        azure_block = ''
        # azure_block = self.azure.read(bucket, cur_filename)
        # print "azure content:\n%s" % azure_block        
        
        'then get each block and the meta_str'
        'throw error if the meta strs are different in different clouds'
        blocks = [dropbox_block, s3_block, gs_block, azure_block]
        'Try to get the remote data'
        decode_meta = self.decoder.decode_meta(metastr)
        decoded_filename = self.decoder.decode_filename(shortname, decode_meta.fnsz)
        # print "decoded filename is: %s\n" % decoded_filename
        decoded_data = self.decoder.decode([blocks[0],blocks[1]], decode_meta)
        # print "decoded data is\n%s" % decoded_data
        'compare the result'
        return decoded_data[offset:(offset+size)]

    
    def rename(self, old, new):
        'query the file with the old path name and with the max version number'
        'cp the file with the new name, maintain the meta'



    def write(self, path, data):
        file_size = len(data)        
        print '[CFS][Cloud]Write file: %s, size %d' % (path, file_size)
        bucket, shortname, filename_size = self.get_bucket_shortfilename(path)
        # print "filename %s, filename size %d\n" %(shortname, filename_size)
        time1 = time.time()
        shares, fecmeta = self.encoder.encode(data, filename_size)
        time2 = time.time()
        
        # print "--------fecmeta------------"
        metastr = str(fecmeta)
        # print metastr
        # for i,block in enumerate(shares):
        #     print "--------share[%d]-----" % i
        #     print str(block)
        
        #decode_meta = self.decoder.decode_meta(metastr)
        #decoded_data = self.decoder.decode(shares[0:2], decode_meta)
        #print "decoded data is\n%s" % decoded_data        
        
        cur_filename, old_ver = self.dropbox.get_file_maxver(bucket, shortname)
        
        'query the file with the path name and with the max version number'
        'no such file, ver=1, else ver = 2'
        ver = old_ver + 1
        bucket, filename = self.get_bucket_fullfilename(path,  metastr, ver)
        time3 = time.time()
        # print "---------bucket: %s, filename: %s" % (bucket, filename)
        'write to different cloud with differnt blocks'
        self.dropbox.write(bucket, filename, shares[0])
        self.s3.write(bucket, filename, shares[1])
        self.gs.write(bucket, filename, shares[2])
        #self.azure.write(bucket, filename, shares[2])
        time4 = time.time()
        print '[CFS][Cloud]Write: Encode time %.3f, Rate %.3f MB/s' % (time2-time1, calc_rate(file_size, time2-time1))
        print '[CFS][Cloud]Write: Prepare meta time %.3f' % (time3-time2)
        print '[CFS][Cloud]Write: Upload time %.3f, Rate %.3f MB/s' % (time4-time3, calc_rate(file_size, time4-time3))

    def delete(self, path):
        bucket, prefix, filename_size = self.get_bucket_shortfilename(path)
        print '--------bucket: ' + bucket
        print '--------prefix: ' + prefix
        self.dropbox.delete(bucket, prefix)
        self.s3.delete(bucket, prefix)
        self.gs.delete(bucket, prefix)
        # self.azure.delete(bucket, prefix)

    def rmdir(self, path):
        bucket = self.bucketgen.get_bucket(path)
        self.dropbox.delete_bucket(bucket)
        self.s3.delete_bucket(bucket)
        self.gs.delete_bucket(bucket)
        # self.azure.delete_bucket(bucket)

    def get_all_files(self):
        bucketmap, files = self.gs.get_all_files()
        # print bucketmap
        # print '-------files-------'
        for file in files:
            # print file
            encoded_bucket = file[0:file.find('/')]
            encoded_filename = file[file.find('/')+1:file.find('_')]
            # metastr = file.split('_')[-2]
            # print 'metastr: ' + metastr
            # print 'encoded bucket name: ' + encoded_bucket
            # print 'encoded file name: ' + file[file.find('/', 2)+1:]

            decoded = self.decoder.decode_meta_from_filename(str(file[file.find('/')+1:]))
            filename = decoded[0]
            filesize = int(decoded[1])
            bucket = bucketmap[encoded_bucket][0]

            path = bucket + filename
            # print 'decode: ' + path
            # print '--------data-------'
            data = self.read(path, filesize, 0)
            # print data
            self.write_local(path, data)

    def write_local(self, path, data):
        start = 2
        end = 0 
        while True:
            end = path.find('/', start)
            if end != -1:
                dir = path[:end]
                print dir
                if not os.path.exists(dir):
                    os.makedirs(dir)
                start = end + 1
            else:
                break
        out = open(path, 'w')
        out.write(data)
        out.close()

    def clean(self):
        self.dropbox.clean()
        self.s3.clean()
        self.gs.clean()
        #self.azure.clean()

# cloud = CloudFS('disk_local')
# cloud.get_all_files()
