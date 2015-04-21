#! /usr/bin/env python
import Pyro4
from RWLock import RWLock
from threading import Thread
import time
from groupcache.GroupCacheSettings import settings


all_groups = {}
all_groups_lock = RWLock()

def getGroup(group_id):
    my_group = None
    all_groups_lock.reader_acquire()
    try:
        if group_id in all_groups:
          my_group = all_groups[group_id]
    finally:
        all_groups_lock.reader_release()
    return my_group

def removeGroup(group_id):
    all_groups_lock.writer_acquire()
    try:
        del all_groups[group_id]
    finally:
        all_groups_lock.writer_release()

def hasGroup(group_id):
    all_groups_lock.reader_acquire()
    try:
        return group_id in all_groups
    finally:
        all_groups_lock.reader_release()

def addGroup(group_id,ttl):
    my_group = None
    all_groups_lock.writer_acquire()
    try:
        if group_id in all_groups:
            return all_groups[group_id]
        my_group = GroupEntry(group_id,ttl)
        all_groups[group_id] = my_group
    finally:
        all_groups_lock.writer_release()
    return my_group

def getGroupKeys():
    all_groups_lock.reader_acquire()
    try:
        return all_groups.keys()
    finally:
        all_groups_lock.reader_release()

class GroupEvictionThread(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        while True:
          try:
           time.sleep(settings.EVICTION_INTERVAL)
           keys = getGroupKeys()
           for k in keys:
             group = getGroup(k)
             if group is None:
                continue
             if group.hasExpired():
                removeGroup(k)
          except KeyboardInterrupt:
              return

class GroupEntry(object):
    def __init__(self,group_id,ttl):
        self.group_id = group_id
        self.ttl = ttl
        self.last_used = time.time()
        self.client_ids = {}
        self.lock = RWLock()

    def getClients(self):
        self.lock.reader_acquire()
        try:
            return self.client_ids.keys()
        finally:
            self.lock.reader_release()

    def addClient(self,client_id,ttl):
        self.touch()

        if not self.hasClient(client_id):
            self.newClient(client_id)
   

    def isEmpty(self):
        self.lock.reader_acquire()
        try:
            return len(self.client_ids) == 0
        finally:
            self.lock.reader_release()

    def hasClient(self,client_id):
        self.lock.reader_acquire()
        try:
            return client_id in self.client_ids
        finally:
            self.lock.reader_release()

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

    def newClient(self,client_id):
       self.lock.writer_acquire()
       try:
           self.client_ids[client_id] = client_id
       finally:
           self.lock.writer_release()

class GroupCache(object):
  def __init__(self):
      pass

  def addClient(self,client_id, group_id):
      group = addGroup(group_id,settings.GROUP_TTL)
      group.addClient(client_id,settings.CLIENT_TTL)

  def getClients(self,group_id):
      group = getGroup(group_id)
      if group is None:
          return []
      return group.getClients()

  def isEmpty(self,group_id):
      group_exists = hasGroup(group_id)
      if not group_exists:
          return True
      group = getGroup(group_id)
      return group.isEmpty()
