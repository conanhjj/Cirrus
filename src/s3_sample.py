import boto
from boto.s3.key import Key

s3 = boto.connect_s3()

bucket_name = "example_bucketadffdks"
bucket = s3.get_bucket(bucket_name)
# bucket = s3.create_bucket(bucket_name)

k = Key(bucket)

# put an object (key, content) to the bucket
k.key = '1'
k.set_contents_from_string('Hello World!')
k.key = '2'
k.set_contents_from_string('Hello World!')
k.key = '3'
k.set_contents_from_string('Hello World!')

# get the object by key
k.key = 'foobar'
content = k.get_contents_as_string()
print content

# list all keys in bucket
key_list = bucket.get_all_keys(prefix='foo')
for k in key_list:
	print k.name

# Buckets cannot be deleted unless they're empty. Since we still have a
# reference to the key (object), we can just delete it.
# print "Deleting the object."
# k.delete()

# Now that the bucket is empty, we can delete it.
# print "Deleting the bucket."
# s3.delete_bucket(bucket_name)
