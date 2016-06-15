#!/usr/bin/env python

"""
Reads in the CDX files and counts things in Redis. It also keeps a LevelDB
index of seen URLs so that the same URL isn't counted twice. LevelDB is used
because there are lotsa URLs and we don't have a Redis cluster to store
them all in memory.
"""

CDX_ROOT = "/museums/cdx/"

import re
import sys
import glob
import gzip
import surt
import redis
import shutil
import leveldb

# open pristine redis db for keeping counts of things
r = redis.StrictRedis()
r.flushdb()

# open pristine leveldb for keeping track of urls
shutil.rmtree('seendb')
seen = leveldb.LevelDB('seendb')

# iterate through each cdx file
for cdx_file in glob.glob(CDX_ROOT + "*.cdx.gz"):
    print cdx_file

    # read each line in the cdx file
    for line in gzip.open(cdx_file):

        # skip cdx header
        if line[1:4] == 'CDX': 
            continue
       
        # get a few things
        cols = line.split(" ")
        surt = cols[0]
        url = cols[2]
        mime_type = cols[3]
        status_code = cols[4]
        size = cols[8]
        digest = cols[5]

        # if we've already seen this surt we can ignore it
        # this prevents tallying the same resource from separate crawls
        if seen.Get(url, default=False):
            print "seen %s" % url
            continue
        else:
            seen.Put(url, "1")

        # get host portion of the surt
        m = re.match(r'(^.+?)\)', surt)
        if not m:
            # this can happen with whois records, etc
            continue
        surt_host = m.group(1)


        # increment stuff in redis
        r.zincrby('hosts', surt_host, 1)
        r.zincrby('size', surt_host, int(size))
        r.zincrby('mime-%s' % mime_type, surt_host, 1)
        r.zincrby('status-%s' % status_code, surt_host, 1)

        # keep track of hosts with a robots.txt file
        if surt.endswith(')/robots.txt') and status_code == "200":
            r.sadd('robots', surt_host)
