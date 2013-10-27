# Copyright 2010 Cornell University. All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions 
# are met:
# 
#   1. Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# 
#   2. Redistributions in binary form must reproduce the above 
# copyright notice, this list of conditions and the following 
# disclaimer in the documentation and/or other materials provided 
# with the distribution.
# 
#   3. Neither the name of the University nor the names of its 
# contributors may be used to endorse or promote products derived 
# from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY CORNELL UNIVERSITY  ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL CORNELL UNIVERSITY OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY 
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
# DAMAGE.
# 
# The views and conclusions contained in the software and documentation
# are those of the authors and should not be interpreted as representing 
# official policies, either expressed or implied, of Cornell University.
# 


# UIUC CS523 Fall2013 Cirrus project modification
# Add Simple AES encryption using pycrypto package (https://pypi.python.org/pypi/pycrypto)
# 


import sys
try:
    import zfec
except ImportError, e:
    print >> sys.stderr, """Fatal error: Cannot import zfec erasure coding library.
Zfec can be obtained from http://pypi.python.org/pypi/zfec
"""
    sys.exit(1)

import struct, hashlib, base64, md5
from fractions import gcd
#from racs.util.stats import *

try:
    from Crypto.Cipher import AES
except ImportError, e:
    print >> sys.stderr, """Fatal error: Cannot import Python Cryptography Toolkit (pycrypto).
pycrypto can be obtained from https://pypi.python.org/pypi/pycrypto
"""
    sys.exit(1)


METABYTES = 3
CYPHER_BLOCK_BYTES = 16 #Use 16 as the cipher's key length and block size
CYPHER_IV = 'CS523Fa13 Cirrus' # Must be 16 byte

# Use md5 to process the input key as md5, and the size is 16
def process_cipherkey(key):
    m = md5.new()
    m.update(key)
    return m.digest()

# The padding will consider both k the split number, and the block size CYPHER_BLOCK_BYTES

def compute_padding(k, data_len):
    base_len = (k * CYPHER_BLOCK_BYTES) / gcd(k, CYPHER_BLOCK_BYTES);
    r = data_len % base_len
    if r == 0:
        return 0
    else:
        return base_len - r

    
def div_ceil(n, d):
    """
    The smallest integer k such that k*d >= n.
    """
    return (n/d) + (n%d != 0)

class FECMeta:
    short_header = 'racsmeta'
    header = 'x-amz-meta-'+short_header

    # stored in x-amz-meta-racs header
    _fmt = 'I32s'
    _fields = "size md5".split()
    meta_length = struct.calcsize(_fmt)

    def __init__(self, size, md5):
        self.size = size # size of original file
        self.md5 = md5

    @classmethod
    def read(cls, packed):
        packed = base64.b64decode(packed)
        kw = dict(zip(cls._fields,struct.unpack(cls._fmt, packed)))
        return cls(**kw)

    def __len__(self):
        return self.meta_length
    
    def __str__(self):
        values = [getattr(self, field) for field in self._fields]
        packed = struct.pack(self._fmt, *values)
        return base64.b64encode(packed)


class ShareMeta:
    # Metainformation stored in each encoded share
    # (not to be confused with metadata headers. ShareMeta is part of the share!)
    _fmt = 'B'
    _fields = ['sharenum']
    meta_length = struct.calcsize(_fmt)

    def __init__(self, sharenum): 
        self.sharenum = sharenum
    
    @classmethod
    def strip(cls, share):
        s = cls.read(share)
        return s, share[len(s):]

    @classmethod
    def prepend_to(cls, block, **metakw):
        s = cls(**metakw)
        return str(s) + block

    @classmethod
    def read(cls, packed):
        kw = dict(zip(cls._fields,struct.unpack(cls._fmt, packed[:cls.meta_length])))
        return cls(**kw)

    def __len__(self):
        return self.meta_length
    
    def __str__(self):
        values = [getattr(self, field) for field in self._fields]
        return struct.pack(self._fmt, *values)


class Encoder(object):
    def __init__(self, k, m, key):
        self.fec = zfec.Encoder(k, m)
        self.k = k
        self.m = m
        self.cipher = AES.new(process_cipherkey(key), AES.MODE_CBC, CYPHER_IV)

    def encode(self, data, md5=None):
        """
        @param data: string

        @return: a sequence of m blocks -- any k of which suffice to
            reconstruct the input data.  Each block has
        """
#        elapsed = Stopwatch()

        if md5 is None:
            m = hashlib.md5()
            m.update(data)
            md5 = m.hexdigest()
        
        osize = len(data)
        # pad data
        padding_bytes = compute_padding(self.fec.k, osize)
        
        padded_data = data + '\0' * padding_bytes
        padded_data = self.cipher.encrypt(padded_data) #use AES encoding

        if len(padded_data) % self.fec.k != 0:
            raise Exception # sanity check

        chunksize = (osize + padding_bytes) / self.fec.k

        #chunksize = div_ceil(len(data), self.fec.k)
        #l = [ data[i*chunksize:(i+1)*chunksize] + "\x00" * min(chunksize, (((i+1)*chunksize)-osize)) for i in range(self.fec.k) ]
        segments = [ padded_data[i*chunksize:(i+1)*chunksize] for i in xrange(self.fec.k) ]
        blocks = self.fec.encode(segments)

        fecmeta = FECMeta(size = osize, md5 = md5)

        for i,block in enumerate(blocks):
            blocks[i] = ShareMeta.prepend_to(block, sharenum = i)

        sanity_check = False

        if sanity_check:
            d = Decoder(self.fec.k, self.fec.m)
            vdat = d.decode(blocks[:self.fec.k], fecmeta)
            vm = hashlib.md5()
            vm.update(vdat)
            vmd5 = vm.hexdigest()
            if len(vdat) != osize:
                raise Exception # sanity check fail
            if vmd5 != md5:
                raise Exception # sanity check fail

#        record_event("zfec:encode bytes", len(data))()
#        record_event("zfec:encode time", elapsed())()


        return blocks, fecmeta



class Decoder(object):
    def __init__(self, k, m, key):
        self.fec = zfec.Decoder(k, m)
        self.cipher = AES.new(process_cipherkey(key), AES.MODE_CBC, CYPHER_IV)

    def decode(self, shares, fecmeta): #, sharenums, padlen):
        """
        @param padlen: the number of bytes of padding to strip off;  Note that
            the padlen is always equal to (blocksize times k) minus the length
            of data.  (Therefore, padlen can be 0.)
        """
#        elapsed = Stopwatch()
        raw_shares = []
        sharenums = []
        for mb in shares:
            meta, share = ShareMeta.strip(mb)
            sharenums.append(meta.sharenum)
            raw_shares.append(share)

        padding_bytes = compute_padding(self.fec.k, fecmeta.size) 

        data = ''.join(self.fec.decode(raw_shares, sharenums))
        data = self.cipher.decrypt(data) 

        if padding_bytes > 0: 
            data = data[:-padding_bytes]

        if len(data) != fecmeta.size:
            raise Exception # Sanity check

#        record_event("zfec:decode bytes", len(data))()
#        record_event("zfec:decode time", elapsed())()

        return data
