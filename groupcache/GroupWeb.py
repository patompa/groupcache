#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from SocketServer import ThreadingMixIn
from threading import Thread
import sys
from CacheClient import CacheClient

PORT_NUMBER = 9595

class myHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.do_POST()

    def do_POST(self):
        print self.path
        import json
        from urlparse import urlparse, parse_qs
        params = parse_qs(urlparse(self.path).query)

        print "got params %s" % params
        group = "rrtc_" + params['group'][0]
        print "got group %s" % group
        intoken = params['token'][0]
        cache = CacheClient()
        content_len = int(self.headers.getheader('content-length', 0))
        message = self.rfile.read(content_len)
        isOk = cache.publish(intoken,group,message)
        self.sendResult(json.dumps({'status':isOk}),params)

    def sendResult(self,result,params):
        self.send_response(200)
        self.send_header('Content-type','application/json')
        self.end_headers()
        if params.has_key('callback'):
           result = "%s('%s')" % (params['callback'][0],result)
        self.wfile.write(result)

    def debug(self,msg):
        import time
        out = open('groupweb.log','a')
        out.write("[%.0f %s] %s\n" % (time.time()*1000,time.ctime(),msg))
        out.close()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

try:
        server = ThreadedHTTPServer(('', PORT_NUMBER), myHandler)
        print 'Started httpserver on port ' , PORT_NUMBER

        server.serve_forever()

except KeyboardInterrupt:
        print '^C received, shutting down the web server'
        server.socket.close()
