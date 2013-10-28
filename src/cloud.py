'''
Created on Oct 27, 2013

A proxy for store the back

'''


class CloudFS:
    def __init__(self):
        'initialize the cloud storage'
        self.cloud = 3
    
    def read(self, path):
        'query the file with the path name and with the max version number'
        'then get each block and the meta_str'
        'throw error if the meta strs are different in different clouds'
        metastr = 'abcdefg'
        blocks = ['cloud0 data', 'cloud1 data', 'cloud2 data']
        return blocks,metastr
    
    
    def rename(self, old, new):
        'query the file with the old path name and with the max version number'
        'cp the file with the new name, maintain the meta'
        
    def truncate(self, path, metastr):
        'if cloud fs cannot be truncate, just cp the file with the path name to a file name with the new meta'
        
    def write(self, path, blocks, metastr):
        'query the file with the path name and with the max version number'
        'no such file, ver=1, else ver = 2'
        old_ver = 1
        ver = old_ver + 1
        name = path + '_' + metastr + '_' + ver
        'write to different cloud with differnt blocks'
        
        