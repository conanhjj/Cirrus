## Memory File System

It is the example from fusepy.

Please install FUSE, fusepy(python package), and zfec(python package) first
- FUSE: http://fuse.sourceforge.net/
- fusepy: https://github.com/terencehonles/fusepy
- zfec https://pypi.python.org/pypi/zfec

### Sample Test
Start the fuse memeory disk
```bash
python memoryfs.py /tmp/fusepy/ #Suppose there is a directory /tmp/fusepy
```

In another session
```bash
echo abcd > /tmp/fusepy/test
echo efg >> /tmp/fusepy/test
```

The fuse memory disk console will output the log of writing the content, and doing encode and decode test.

