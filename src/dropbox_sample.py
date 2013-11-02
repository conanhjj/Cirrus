import dropbox, os

access_token = os.environ['DROPBOX_ACCESS_TOKEN']
client = dropbox.client.DropboxClient(access_token)

# put a local file to Dropbox
try:
    f = open('file.txt')
    client.put_file('/hello', f)
    f.close()
    print 'Put file successfully!'
except IOError:
    print 'Cannot find local file!'

# get a Dropbox file
try:
    f, metadata = client.get_file_and_metadata('/hello')
    out = open('hello.txt', 'w')
    out.write(f.read())
    out.close()
    f.close()
    print 'Get file successfully!'
except dropbox.rest.ErrorResponse:
    print 'Cannot get file!'

# list all Dropbox files
resp = client.metadata('/') 
if 'contents' in resp:
    for f in resp['contents']:
        name = os.path.basename(f['path'])
        print name

# delete a Dropbox file
try:
    client.file_delete('/abc')
    print 'Delete file successfully!'
except dropbox.rest.ErrorResponse:
    print 'Cannot delete file!'







