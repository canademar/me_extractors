#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cherrypy
import getopt
import sys 
import json
from datetime import datetime

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
    def datetime(self):
        datestr = datetime.now().strftime("%Y%m%d%H%M%S")
        return json.dumps({"datestr":datestr})
        

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

        cherrypy.quickstart(Root(),  config=conf)

    except getopt.GetoptError as err:
        logging.error(err)
        usage()
        sys.exit(2)

if __name__ == '__main__':
    main(sys.argv[1:])
