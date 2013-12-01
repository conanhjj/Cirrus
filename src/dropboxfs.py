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
            # not found the bucket
            self.client.file_create_folder(bucket_full_name)
            print 'create new bucket in dropbox: %s' % bucket_name
        return bucket_name
        
    def write(self, bucket, filename, data):
        # print 'try write to dropbox with %s, %s' % (bucket,filename)
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
            # print 'Put file successfully!'
        except IOError:
            print 'Cannot find local file!'            
            
        
    def read(self, bucket, filename):
        # print 'try read from dropbox with %s, %s' % (bucket,filename)
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
            # print 'try read files in  dropbox with %s, %s' % (bucket,shortname)
            resp = self.client.metadata('/'+bucket+'/') 
            ver = 0
            cur_filename = ''
            if 'contents' in resp:
                for f in resp['contents']:
                    name = os.path.basename(f['path'])
                    if string.find(name, shortname) == 0:
                        secs = string.split(name, '_')
                        cur_ver = int(secs[-1])
                        if cur_ver > ver:
                            ver = cur_ver
                            cur_filename = name
            # print 'file the max ver filename %s' % cur_filename
            return cur_filename, ver
        except dropbox.rest.ErrorResponse:
            print 'cannot list filenames'
            return ''

    def get_all_files(self, root):
        try:
            resp = self.client.metadata('/')
            files = []
            if 'contents' in resp:
                for f in resp['contents']:
                    bucket = f['path']
                    bucket_dic = self.get_files_from_bucket(bucket)
                    if not os.path.exists(root + bucket):
                        os.makedirs(root + bucket)
                    for prefix in bucket_dic:
                        filename = prefix + '_' + bucket_dic[prefix][0] + '_' + str(bucket_dic[prefix][1])
                        files.append(filename)
                        print filename
                        # fh = self.client.get_file(filename).read()


            return files    
        except dropbox.rest.ErrorResponse:
            print 'Cannot get dropbox files'

    def get_files_from_bucket(self, bucket):
        try:
            resp = self.client.metadata(bucket + '/')
            dic = dict()
            if 'contents' in resp:
                for f in resp['contents']:
                    filename = f['path'].split('_')
                    prefix = filename[0]
                    meta = filename[1]
                    version = int(filename[2])
                    if prefix in dic:
                        if dic[prefix][1] < version:
                            dic[prefix] = [meta, version]
                    else:
                        dic[prefix] = [meta, version]
            return dic
        except dropbox.rest.ErrorResponse:
            print 'Cannot get dropbox files'
    

    def delete(self, bucket, prefix):
        try:
            resp = self.client.metadata('/'+bucket+'/') 
            if 'contents' in resp:
                for f in resp['contents']:
                    name = os.path.basename(f['path'])
                    if prefix == name.split('_')[-3]:
                        self.client.file_delete('/' + bucket + '/' + name)
            print 'Delete dropbox file /' + bucket + '/' + prefix + ' successfully!'
        except dropbox.rest.ErrorResponse:
            print 'Cannot delete dropbox file /' + bucket + '/' + prefix 

    def delete_bucket(self, bucket):
        try:
            self.client.file_delete('/' + bucket)
            print 'Delete dropbox bucket /' + bucket + ' successfully!'
        except dropbox.rest.ErrorResponse:
            print 'Cannot delete dropbox bucket /' + bucket 

    def clean(self):
        try:
            resp = self.client.metadata('/')
            if 'contents' in resp:
                for f in resp['contents']:
                    self.client.file_delete(f['path'])
            print 'Clean dropbox successfully'
        except IOError:
            print 'Cannot clean dropbox storage'


# dropboxfs = DropboxFS()
# dropboxfs.get_all_files('disk_local')


