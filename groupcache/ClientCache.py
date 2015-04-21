#! /usr/bin/env python
import Pyro4
from RWLock import RWLock
from threading import Thread, Event
import time
from groupcache.GroupCacheSettings import settings

all_clients = {}
all_clients_lock = RWLock()

def getClient(client_id):
    all_clients_lock.reader_acquire()
    try:
        if not client_id in all_clients:
            return None
        return all_clients[client_id]
    finally:
        all_clients_lock.reader_release()

def removeClient(client_id):
    all_clients_lock.writer_acquire()
    try:
        del all_clients[client_id]
    finally:
        all_clients_lock.writer_release()

def hasClient(client_id):
    all_clients_lock.reader_acquire()
    try:
        return client_id in all_clients
    finally:
        all_clients_lock.reader_release()

def newClient(client):
    all_clients_lock.writer_acquire()
    try:
        all_clients[client.client_id] = client
    finally:
        all_clients_lock.writer_release()

def getClientKeys():
    all_clients_lock.reader_acquire()
    try:
        return all_clients.keys()
    finally:
        all_clients_lock.reader_release()

class ClientEvictionThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        while True:
          try:
           time.sleep(settings.EVICTION_INTERVAL)
           keys = getClientKeys()
           for k in keys:
             client = getClient(k)
             if client is None:
                continue
             if client.hasExpired():
                removeClient(k)
          except KeyboardInterrupt:
               return

class ClientEntry(object):
    def __init__(self,client_id,ttl):
        self.client_id = client_id
        self.ttl = ttl
        self.last_used = time.time()
        self.lock = RWLock()
        self.message_lock = RWLock()
        self.messages = []
        self.event = Event()

    def addMessage(self,message):
       self.message_lock.writer_acquire()
       try:
           self.messages.append(message)
           self.event.set()
       finally:
           self.message_lock.writer_release()
       self.touch()

    def getLockMessages(self):
       self.message_lock.writer_acquire()
       try:
           new_messages = self.messages
           self.messages = []
           self.event.clear()
           return new_messages
       finally:
           self.message_lock.writer_release()
       return []

    def getMessages(self,wait_time):
       new_messages = []
       new_messages = self.getLockMessages()
       self.touch()
       if len(new_messages) == 0:
           new_messages = self.waitForMessages(wait_time)
       return new_messages

    def waitForMessages(self,wait_time):
      has_more = self.event.wait(wait_time)
      if not has_more:
          return []
      new_messages = self.getLockMessages()
      return new_messages

    def hasExpired(self):
       self.lock.reader_acquire()
       try:
           return (time.time() - self.last_used) > self.ttl
       finally:
           self.lock.reader_release()

    def touch(self):
       self.lock.writer_acquire()
       try:
           self.last_used = time.time()
       finally:
           self.lock.writer_release()

class ClientCache(object):
  def __init__(self):
      pass

  def addClient(self,client_id):
      if not hasClient(client_id):
         newClient(ClientEntry(client_id,settings.CLIENT_TTL))
      else:
         client =  getClient(client_id)
         if client is not None:
             client.touch()

  def getMessages(self,client_id):
      client = getClient(client_id)
      return client.getMessages(settings.CLIENT_WAIT)

  def addMessage(self,client_id,message):
      client = getClient(client_id)
      if client is not None:
          msg = client.addMessage(message)
          return msg
