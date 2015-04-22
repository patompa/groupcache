#! /usr/bin/env python
from groupcache.CacheClient import CacheClient
import threading
import sys
import time
import random
import uuid

numthreads = int(sys.argv[1])
iterations = int(sys.argv[2])
delay = float(sys.argv[3])


client = CacheClient()

class PullThread(threading.Thread):
    def __init__(self,sid,gid):
        threading.Thread.__init__(self)
        self.sid = sid
        self.gid = gid
        self.client = CacheClient()

    def run(self):
        while (True):
            data = self.client.pull(self.sid)
            if data is None:
                continue
            for message in data:
                if message['action'] == "stop":
                    print("%s stopped" % (self.sid))
                    return
                elapsed = time.time() - message['starttime']
                print("%s %s %s %.3f" % (message['group'],self.gid,message['seq'],elapsed))

       

class PublishThread(threading.Thread):
    def __init__(self,iterations, delay):
        threading.Thread.__init__(self)
        self.iterations = iterations
        self.delay = delay
        self.gid = self.guid()
        self.sid = self.guid()
        self.pid = self.guid()
        print("Starting %s %s %s"  % (self.gid,self.sid,self.pid))
        self.client = CacheClient()

    def guid(self):
        return str(uuid.uuid4()).replace('-','') 

    def run(self):
        self.client.subscribe(self.sid,self.gid)
        PullThread(self.sid,self.gid).start() 
        for i in range(0,self.iterations):
            started = time.time()
            self.client.publish(self.pid,self.gid,{'group':self.gid,'starttime': time.time(),'action':'perftest','seq':i})
            print("Publishing message %d %.3f" % (i,time.time()-started))
            time.sleep(self.delay)
        self.client.publish(self.pid,self.gid,{'action':'stop'})


for i in range(0,numthreads):
    print("Starting thread %d" % i)
    PublishThread(iterations,delay).start()

