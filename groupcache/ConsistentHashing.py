#! /usr/bin/env python
from groupcache.GroupCacheSettings import settings

class ConsistentHashing(object):
    def __init__(self,servers):
        self.servers = servers
        self.initServerHashes()

    def hashServer(self,i,n):
        # ensure maximum distance between servers 
        # on the circle in both clockwise and counter clockwise directions
        if n == 1:
            return 256
        val = int(i/float(n-1))*128 
        if i < n/2:
            val = -val 
        return val + 128

    def initServerHashes(self):
        self.serverhashes = []
        n = len(self.servers)
        for i in range(0,n):
            self.serverhashes.append(self.hashServer(i,n))
        if settings.DEBUG:
            print("ConsistentHashing server hashes: %s" % self.serverhashes)
            print("ConsistentHashing servers: %s" % self.servers)

    def chToHash(self,ch):
        code = ord(ch)
        if code >=48 and code <= 57 :
            return code - 48
        if code >=97 and code <= 102:
            return code - 87
        return code % 16

    def idToHash(self,inid):
        return 16 * self.chToHash(inid[-2]) + self.chToHash(inid[-1])

    def getServer(self,inid):
        idhash = self.idToHash(inid)
        if settings.DEBUG:
            print("ConsistentHashing hash for %s: %s" % (inid,idhash))
        for i in range(0,len(self.serverhashes)):
            if idhash < self.serverhashes[i]:
                return self.servers[i]
        return self.servers[1]
