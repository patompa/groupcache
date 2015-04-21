#! /usr/bin/env python
import Pyro4
import time

ns = Pyro4.locateNS()

SERVERS = 1
group_map = {}
client_map = {}

def loadServer(i):
  group_uri = ns.lookup("group.cache.%d" % i)
  group_map[i] =  group_uri
  client_uri = ns.lookup("client.cache.%d" % i)
  client_map[i] = client_uri

for i in range(1,SERVERS+1):
    loadServer(i)

def loadServers(n):
  for i in range(1,n+1):
    loadServer(i)

def hashClient(client_id):
    # TODO: Add consistent hashing algorithm
    return 1

def hashGroup(group_id):
    # TODO: Add consistent hashing algorithm
    return 1

def getClient(client_id):
    client_uri = client_map[hashClient(client_id)]
    return Pyro4.Proxy(client_uri)

def getGroup(group_id):
    group_uri = group_map[hashGroup(group_id)]
    return Pyro4.Proxy(group_uri)

class CacheClient(object):
    def __init__(self):
        pass

    def publish(self,client_id,group_id,message):
        group_cache = getGroup(group_id)
        client_ids = group_cache.getClients(group_id)
        ok = False
        for cid in client_ids:
            if cid == client_id:
                continue
            client_cache = getClient(cid)
            client_cache.addMessage(cid,message)
            ok = True
        return ok

    def subscribe(self,client_id,group_id):
        group_cache = getGroup(group_id)
        client_cache = getClient(client_id)
        client_cache.addClient(client_id)
        return group_cache.addClient(client_id,group_id)

    def pull(self,client_id):
        client_cache = getClient(client_id)
        return client_cache.getMessages(client_id)

    def isEmpty(self,group_id):
        group_cache = getGroup(group_id)
        return group_cache.isEmpty(group_id)

if __name__ == '__main__':
    import sys
    command = sys.argv[1]
    client = CacheClient()
    result = ""
    if command == "publish":
        result = client.publish(sys.argv[2],sys.argv[3],sys.argv[4])
    if command == "subscribe":
        result = client.subscribe(sys.argv[2],sys.argv[3])
    if command == "pull":
        result = client.pull(sys.argv[2])
    if command == "isempty":
        result = client.isEmpty(sys.argv[2])
    print result


