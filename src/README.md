## Cirrus File System


Please install FUSE and all the fusepy, Crypto,  zfec ,boto, dropbox.
- FUSE: http://fuse.sourceforge.net/
- fusepy: https://github.com/terencehonles/fusepy
- crypto https://www.dlitz.net/software/pycrypto/
- zfec https://pypi.python.org/pypi/zfec
- boto https://pypi.python.org/pypi/boto
- dropbox https://pypi.python.org/pypi/dropbox



### Sample Test
Define all the cloud fs access key
```bash
export AWS_ACCESS_KEY_ID=your_key_id
export AWS_SECRET_ACCESS_KEY=your_access_key_id
export DROPBOX_ACCESS_TOKEN=your_dropbox_token
```

Start the Cirrus folder
```bash
python cirrus.py /tmp/cirrusdir #Suppose there is a directory /tmp/cirrusdir
```

In another session
```bash
echo abcd > /tmp/fusepy/test
echo efg >> /tmp/fusepy/test
```

The cirrus fs console will output the log 
- writing the content
- AES encoding/decoding
- Erasure code encoding/decoding
- upload/download to from clouds

