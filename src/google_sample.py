#!/usr/bin/python

import StringIO
import os
import shutil
import tempfile
import time
from gslib.third_party.oauth2_plugin import oauth2_plugin

import boto

# URI scheme for Google Cloud Storage.
GOOGLE_STORAGE = 'gs'

bucket_name = 'cirrus_bucket'

# put an object
filename = 'hello'
uri = boto.storage_uri(bucket_name + '/' + filename, GOOGLE_STORAGE)
uri.new_key().set_contents_from_string('hello world')

# get an object 
content = uri.get_key().get_contents_as_string()
print content

# list all objects in a bucket
uri =  boto.storage_uri(bucket_name, GOOGLE_STORAGE)
for obj in uri.get_bucket():
  print obj.name

# delete an object
uri = boto.storage_uri(bucket_name + '/' + filename, GOOGLE_STORAGE)
uri.get_key().delete()


