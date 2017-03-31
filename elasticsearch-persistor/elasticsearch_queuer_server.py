#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cherrypy
import getopt
import sys 
import json
from datetime import datetime
from redis_client import RedisClient
from redis_conf import redis_conf

HOST = '0.0.0.0'
PORT = 9902
THREAD_POOL = 1000
ENCODE_ON = True
ENCODING = 'utf-8'

""" 
    Elasticsearch persistor rest api server

"""
class Root:

    @cherrypy.expose
    def index(self):
        return "Elasticsearch persistor is alive!!"

    @cherrypy.expose
    def polling(self):
        condition = True
        minute = datetime.now().minute
        if(minute % 5==0):
            return json.dumps({"status":"DONE", "minute":minute})
        else:
            return json.dumps({"status":"PROCESSING", "minute": minute})
        

@cherrypy.expose
class StringGeneratorWebService(object):

    @cherrypy.tools.accept(media='text/plain')
    def GET(self):
        return "Elasticsearch persistor is alive!!"

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        doc = cherrypy.request.json
        rc = RedisClient(redis_conf["host"], redis_conf["port"], redis_conf["db"], redis_conf["collection"])
        dumped_doc = json.dumps(doc)
        rc.put(dumped_doc)
        return {"received doc": dumped_doc}

def usage(command):
    print("""
            %s  [options]
            options:
            --help  Show help
          """ % command)

def main(argv):
    
    # Command line arguments
    try:
        optlist , args = getopt.getopt(argv , 'p:b:' , ['help'])
        for opt, value in optlist:
            if opt == '--help':
                usage(argv[0])
                sys.exit()

        conf = {
            '/': {
                'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                'tools.sessions.on': True,
                'tools.response_headers.on': True,
                'tools.response_headers.headers': [('Content-Type', 'text/plain')],
            },
            'global': {
                'server.socket_host': HOST,
                'server.socket_port': PORT,
                'server.thread_pool': THREAD_POOL,
                'tools.encode.on': ENCODE_ON,
                'tools.encode.encoding': ENCODING
                }        
            }

        cherrypy.quickstart(StringGeneratorWebService(),  config=conf)

    except getopt.GetoptError as err:
        logging.error(err)
        usage()
        sys.exit(2)

if __name__ == '__main__':
    main(sys.argv[1:])
