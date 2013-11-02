'''
Created on Nov 2, 2013

@author: Administrator
'''
import dropbox, os, string


def dropboxfilename(bucket, filename):
    return '/'+bucket+'/'+filename

class DropboxFS():
    
    def __init__(self):
        access_token = os.environ['DROPBOX_ACCESS_TOKEN']
        self.client = dropbox.client.DropboxClient(access_token)
        
        
    def write(self, bucket, filename, data):
        print 'try write to dropbox with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            tmp_file = open(filename, 'w')
            tmp_file.write(data)
            tmp_file.close()
            'then upload'
            f = open(filename)
            self.client.put_file(dropboxfilename(bucket, filename), f)
            f.close()
            os.remove(filename)
            print 'Put file successfully!'
        except IOError:
            print 'Cannot find local file!'            
            
        
    def read(self, bucket, filename):
        print 'try read from dropbox with %s, %s' % (bucket,filename)
        try:
            f, metadata = self.client.get_file_and_metadata(dropboxfilename(bucket, filename))
            data = f.read()
            f.close()
            print 'Get file successfully!'
            return data
        except dropbox.rest.ErrorResponse:
            print 'Cannot get file!'
            return ''
    
    def get_file_maxver(self, bucket, shortname):
        try:
            print 'try read files in  dropbox with %s, %s' % (bucket,shortname)
            resp = self.client.metadata('/'+bucket+'/') 
            ver = 0
            cur_filename = ''
            if 'contents' in resp:
                for f in resp['contents']:
                    name = os.path.basename(f['path'])
                    print name
                    if string.find(name, shortname) == 0:
                        secs = string.split(name, '_')
                        cur_ver = int(secs[-1])
                        if cur_ver > ver:
                            ver = cur_ver
                            cur_filename = name
            print 'file the max ver filename %s' % cur_filename
            return ver
        except dropbox.rest.ErrorResponse:
            print 'cannot list filenames'
            return ''