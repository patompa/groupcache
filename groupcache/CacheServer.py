#! /usr/bin/env python
import Pyro4
from RWLock import RWLock
from threading import Thread
import time
from groupcache.GroupCacheSettings import settings
from groupcache.GroupCache import GroupCache, GroupEvictionThread
from groupcache.ClientCache import ClientCache, ClientEvictionThread

Pyro4.config.THREADPOOL_SIZE=settings.THREADS
import daemon as d


def main():
    import sys
    try:
      group_cache = GroupCache()
      client_cache = ClientCache()
      daemon = Pyro4.Daemon()
      group_cache_uri = daemon.register(group_cache)
      client_cache_uri = daemon.register(client_cache)
      ns = Pyro4.locateNS()
      server_id = sys.argv[1]
      ns.register("group.cache.%s" % server_id,group_cache_uri)
      ns.register("client.cache.%s" % server_id,client_cache_uri)
      print("Starting group.cache.%s" % server_id)
      print("Starting client.cache.%s" % server_id)
      groupThread = GroupEvictionThread()
      groupThread.start()
      clientThread = ClientEvictionThread()
      clientThread.start()
      daemon.requestLoop()
    finally:
       print("Stopping evictors...")
       groupThread._Thread__stop()
       clientThread._Thread__stop()

if __name__ == "__main__":
  import sys
  if len(sys.argv) > 2 and sys.argv[2] == "-d":
    with d.DaemonContext():
      main()
  else:
      main()
