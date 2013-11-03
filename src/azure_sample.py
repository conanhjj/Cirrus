#!/usr/bin/python

import os
from azure.storage import *

blob_service = BlobService(account_name=os.environ['AZURE_ACCOUNT'], account_key=os.environ['AZURE_KEY'])

# create a container
container_name = 'cirrus-bucket'
# blob_service.create_container(container_name)

# put an object to the container
blob_name = 'myblob'
myblob = 'hello world'
blob_service.put_blob(container_name, blob_name, myblob, x_ms_blob_type='BlockBlob')

# get an object
blob = blob_service.get_blob(container_name, blob_name)
print blob

# list all blobs in a container
blobs = blob_service.list_blobs(container_name)
for blob in blobs:
    print(blob.name)

# delete a blob
blob_service.delete_blob(container_name, blob_name)



