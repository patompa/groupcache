#! /usr/bin/env python
from django.http import HttpResponse
from json import dumps, loads
from django.conf import settings
import logging
log = logging.getLogger(__name__)

from CacheClient import CacheClient

def debug(msg):
  import time
  log.debug("%.2f %s" % (time.time(),msg))

def prefix(group):
  if not group is None and group != "":
    group = "rtc_" + group
  return group

def addHeaders(response):
   response['Access-Control-Allow-Origin'] = '*'
   response['Access-Control-Allow-Method'] = 'OPTIONS,POST,GET'
   response['Access-Control-Allow-Headers'] = 'Content-Type'
   response['Access-Control-Allow-Content-Type'] = 'application/json'
   return response

def toResponse(result):
  return addHeaders(HttpResponse(dumps(result)))

def subscribe(request):
   clientid = ""
   group=""
   if request.POST.has_key('client'):
     clientid = str(request.POST['client'])
   if request.POST.has_key('group'):
     group = str(request.POST['group'])
   if request.GET.has_key('client'):
     clientid = str(request.GET['client'])
   if request.GET.has_key('group'):
     group = str(request.GET['group'])

   token = clientid

   if not token == "" and not group == "":
     client = CacheClient()
     groups = group.split(',')
     for g in groups:
       client.subscribe(token,prefix(g))
   return toResponse({'token':token})
   
def pull(request):
   clientid = str(request.GET['client'])
   client = CacheClient()
   try:
     messages = client.pull(clientid)
   except Exception,inst:
      messages = None
      debug("error pulling %s %s" % (clientid,inst))
   result = []
   if messages is not None:
     for message in messages:
       result.append(loads(message))
   return toResponse({'messages':result})

def isEmpty(group):
  client = CacheClient()
  group = prefix(group)
  return client.isEmpty(group)

def checkempty(request):
  import json
  group = str(request.GET['group'])
  return toResponse({'isEmpty': isEmpty(group)})

def publish(request):
  import json
  group = prefix(str(request.GET['group']))
  intoken = str(request.GET['token'])
  client_ip = request.META['REMOTE_ADDR']
  isOk = True
  try:
    cache = CacheClient()
    message = request.raw_post_data
    isOk = cache.publish(intoken,group,message)
  except Exception, inst:
    debug("error publish %s %s %s" % (group,intoken,inst))
    return toResponse({'error':"%s" % inst})
  return toResponse({'status':isOk})
