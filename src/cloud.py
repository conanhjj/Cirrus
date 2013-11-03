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

class CloudFS:
    def __init__(self):
        'initialize the cloud storage'
        'The below is used for fec encode/decode'
        self.k = 2 # two pieces
        self.m = 3 # after encoding output three pieces
        self.encoder = fec.Encoder(self.k,self.m, 'password')
        self.decoder = fec.Decoder(self.k,self.m, 'password')
        self.bucketgen = bucketgen.BucketGenerator()
        self.dropbox = dropboxfs.DropboxFS()
        self.s3 = s3fs.S3FS()

    'input full path return paris of bucket name and shortfilename(no meta and ver)'
    def get_bucket_shortfilename(self, path):
        paths = fileutil.FileUtil.split_path(path)
        bucket = self.bucketgen.get_bucket(paths[0])
        filename = paths[1]
        return bucket, filename

    'input full path, metastr, ver, return paris of bucket name and fullfilename'
    def get_bucket_fullfilename(self, path, metastr, ver):
        bucket, shortname = self.get_bucket_shortfilename(path)
        filename = shortname + '_'+ metastr + '_' + str(ver)
        return bucket, filename

        
    def getmetastr(self, path):
        'query the file with the path and the max vers, and return the meta_str'
        bucket, shortfilename = self.get_bucket_shortfilename(path)
        print "----query clouds for----bucket: %s, shortfilename: %s" % (bucket, shortfilename)
        fullname = shortfilename + '_test_meta_3'
        'split fullname by _'
        return string.split(fullname, '_')[-2] #-1 is v, -1 is meta
    
    def read(self, path, size, offset):
        bucket, shortname = self.get_bucket_shortfilename(path)
        
        'query the file with the path name and with the max version number'
        'we use dropbox as the key'
        cur_filename, old_ver = self.dropbox.get_file_maxver(bucket, shortname)
        metastr = string.split(cur_filename, '_')[-2]
        
        dropbox_block = self.dropbox.read(bucket, cur_filename)
        print "dropbox content: %s" %  dropbox_block
        s3_block = self.s3.read(bucket, cur_filename)
        print "s3 content: %s" %  s3_block
        
        
        'then get each block and the meta_str'
        'throw error if the meta strs are different in different clouds'
        blocks = [dropbox_block, s3_block, 'cloud2 data']
        'Try to get the remote data'
        decode_meta = self.decoder.decode_meta(metastr)
        decoded_data = self.decoder.decode(blocks[0:2], decode_meta)
        print "decoded data is %s" % decoded_data
        'compare the result'
        return decoded_data[offset:(offset+size)]

    
    def rename(self, old, new):
        'query the file with the old path name and with the max version number'
        'cp the file with the new name, maintain the meta'
        
    def write(self, path, data):
        shares, fecmeta = self.encoder.encode(data)
        print "--------fecmeta------------"
        metastr = str(fecmeta)
        print metastr
        for i,block in enumerate(shares):
            print "--------share[%d]-----" % i
            print str(block)
        
        
        bucket, shortname = self.get_bucket_shortfilename(path)
        
        cur_filename, old_ver = self.dropbox.get_file_maxver(bucket, shortname)
        
        'query the file with the path name and with the max version number'
        'no such file, ver=1, else ver = 2'
        ver = old_ver + 1
        bucket, filename = self.get_bucket_fullfilename(path,  metastr, ver)
        print "---------bucket: %s, filename: %s" % (bucket, filename)
        'write to different cloud with differnt blocks'
        self.dropbox.write(bucket, filename, shares[0])
        self.s3.write(bucket, filename, shares[1])
        