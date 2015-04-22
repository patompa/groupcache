#! /usr/bin/env python
import Pyro4
import time
from groupcache.ConsistentHashing import ConsistentHashing
from groupcache.GroupCacheSettings import settings

ns = Pyro4.locateNS()

group_map = {}
client_map = {}

default_servers = ['1']

hasher = ConsistentHashing(default_servers)


def loadCacheServers(servers):
    hasher = ConsistentHashing(servers)
    for server in servers:
        loadServer(server)

def loadServer(server):
    group_uri = ns.lookup("group.cache.%s" % server)
    group_map[server] =  group_uri
    client_uri = ns.lookup("client.cache.%s" % server)
    client_map[server] = client_uri

for default_server in default_servers:
    loadServer(default_server)

def getClient(client_id):
    client_uri = client_map[hasher.getServer(client_id)]
    return Pyro4.Proxy(client_uri)

def getGroup(group_id):
    group_uri = group_map[hasher.getServer(group_id)]
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
