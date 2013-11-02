## Cirrus File System


Please install FUSE and all the fusepy, Crypto,  zfec and boto.
- FUSE: http://fuse.sourceforge.net/
- fusepy: https://github.com/terencehonles/fusepy
- crypto https://www.dlitz.net/software/pycrypto/
- zfec https://pypi.python.org/pypi/zfec
- boto https://pypi.python.org/pypi/boto


### Sample Test
Start the Cirrus folder
```bash
python cirrus.py /tmp/cirrusdir #Suppose there is a directory /tmp/fusepy
```

In another session
```bash
echo abcd > /tmp/fusepy/test
echo efg >> /tmp/fusepy/test
```

The cirrus fs console will output the log of writing the content, and doing encode and decode test.

