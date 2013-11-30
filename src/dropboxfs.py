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

    def ensure_get_bucket(self, bucket_name):
        bucket_full_name = '/'+bucket_name+'/'
        try:
            self.client.metadata(bucket_full_name)
        except dropbox.rest.ErrorResponse:
            #not found the bucket
            self.client.file_create_folder(bucket_full_name)
            print 'create new bucket in dropbox: %s' % bucket_name
        return bucket_name
        
    def write(self, bucket, filename, data):
        print 'try write to dropbox with %s, %s' % (bucket,filename)
        'not know how to directly write content, create a local file then rm it'
        try:
            self.ensure_get_bucket(bucket)
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
            return data
        except dropbox.rest.ErrorResponse:
            print 'Cannot get file!'
            return ''
    
    'return old filename and max ver'
    def get_file_maxver(self, bucket, shortname):
        try:
            self.ensure_get_bucket(bucket)
            print 'try read files in  dropbox with %s, %s' % (bucket,shortname)
            resp = self.client.metadata('/'+bucket+'/') 
            ver = 0
            cur_filename = ''
            if 'contents' in resp:
                for f in resp['contents']:
                    name = os.path.basename(f['path'])
                    #print name
                    if string.find(name, shortname) == 0:
                        secs = string.split(name, '_')
                        cur_ver = int(secs[-1])
                        if cur_ver > ver:
                            ver = cur_ver
                            cur_filename = name
            print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except dropbox.rest.ErrorResponse:
            print 'cannot list filenames'
            return ''