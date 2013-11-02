'''
Created on Oct 27, 2013

A proxy for store the back

'''
import string
import fec
import fileutil
import bucketgen




    

class CloudFS:
    def __init__(self):
        'initialize the cloud storage'
        'The below is used for fec encode/decode'
        self.k = 2 # two pieces
        self.m = 3 # after encoding output three pieces
        self.encoder = fec.Encoder(self.k,self.m, 'password')
        self.decoder = fec.Decoder(self.k,self.m, 'password')
        self.bucketgen = bucketgen.BucketGenerator()

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
        return string.split(fullname, '_')[1]
    
    def read(self, path, size, offset):
        'query the file with the path name and with the max version number'

        'then get each block and the meta_str'
        'throw error if the meta strs are different in different clouds'
        metastr = self.getmetastr(path)
        blocks = ['cloud0 data', 'cloud1 data', 'cloud2 data']
        'Try to get the remote data'
        #decode_meta = self.decoder.decode_meta(metastr)
        #decoded_data = self.decoder.decode(blocks[1:], decode_meta)
        'compare the result'
        #return decoded_data[offset:(offset+size)]
        return metastr

    
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
        
        
        
        'query the file with the path name and with the max version number'
        'no such file, ver=1, else ver = 2'
        old_ver = 1
        ver = old_ver + 1
        bucket, name = self.get_bucket_fullfilename(path,  metastr, ver)
        print "---------bucket: %s, filename: %s" % (bucket, name)
        'write to different cloud with differnt blocks'
        
        