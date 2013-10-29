'''
Created on Oct 27, 2013

A proxy for store the back

'''
import fec

class CloudFS:
    def __init__(self):
        'initialize the cloud storage'
        'The below is used for fec encode/decode'
        self.k = 2 # two pieces
        self.m = 3 # after encoding output three pieces
        self.encoder = fec.Encoder(self.k,self.m, 'password')
        self.decoder = fec.Decoder(self.k,self.m, 'password')

        
    def getmetastr(self, path):
        'query the file with the path and the max vers, and return the meta_str'
        metastr = 'abcdefg'
        return metastr
    
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
        name = path + '_' + metastr + '_' + str(ver)
        print "--------- filename: %s" % name
        'write to different cloud with differnt blocks'
        
        