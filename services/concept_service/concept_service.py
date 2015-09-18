#!/usr/bin/env python
# -*- coding: utf-8 -*-
import tornado.ioloop
from tornado.web import Application, RequestHandler, asynchronous
from tornado.ioloop import IOLoop
from ner import Ner
import time
import json
import sys
import logging
    
# Main class
class NerService(tornado.web.RequestHandler):
        
    def initialize(self, concepts_inlinks, stopwords, entities, inlinks_threshold=400, MAX_WORDS=400, MAX_CHARS=2000):
        """
        """
        self.MAX_WORDS = MAX_WORDS
        self.MAX_CHARS = MAX_CHARS
        self.ner = Ner(concepts_inlinks, entities, stopwords, inlinks_threshold=inlinks_threshold, max_words=MAX_WORDS)
        
    def get(self):
        # Get parameters
        inlinks_threshold = int(self.get_argument("inlinks_threshold", default=400))
        self.ner.inlinks_threshold=inlinks_threshold
        text = self.get_argument("text")
        # Check warnings if exists
        warning = []
        if len(text) > self.MAX_CHARS:
            warning.append('Only the first %d chars will be processed. This request is over this limit.' % self.MAX_CHARS)
        if  len(text.split(' ')) > self.MAX_WORDS:
            warning.append('Only the first %d words will be processed. This request is over this limit.' % self.MAX_WORDS)
        result = json.loads(self.ner.fetch_entities(text))
        # Erase text at response
        del(result['text'])
        # if exists warning, append the flags to the output
        if len(warning) > 0:
            result['warnings'] = warning
        self.write(result)

    def post(self):
        results = list()
        for line in str(self.request.body, 'utf8').split('\n'):
            if line: 
                fields = line.split('\t')
                text = fields[0]
                #concepts = self.__format_post_result(self.ner.fetch_entities(text))
                concepts = self.ner.fetch_entities(text)
                print(concepts['results'])
                #result_fields = [concepts] + fields
                #result = '\t'.join(result_fields)
                concept_names = list(concepts['results'].keys())
                results.append({"text":text, "concepts":concept_names})
        #for temp in results:
        #    self.write('%s\n' % (temp))
        print(results)
        self.write({"response":results})

    def __format_post_result(self, response):
        response_dict = json.loads(response)
        concepts = response_dict['results'].keys()        
        if concepts:
            return ";;".join(concepts)
        else:
            return ""
        

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
# data structures to load inside the data
concepts_inlinks={}
stopwords=set()
entities=set()

# Restrictions
MAX_WORDS = 400
MAX_CHARS = MAX_WORDS * 50

logging.info("Loading concepts...")
with(open('data/pagelinks_all.tsv', encoding='utf-8', errors='ignore')) as concepts_file:
    for concept in concepts_file.readlines():
        parts = concept.split('\t')
        concepts_inlinks[parts[0]]=parts[1]
logging.info("%s concepts loaded." % len(concepts_inlinks))

#logging.warning("Loading entities...")
#with(open('data/entities.txt', encoding='utf-8', errors='ignore')) as entities_file:
#    for entity in entities_file.readlines():
#        entities.add(entity[0:len(entity)-1])
#logging.warning("%s entities loaded." % len(entities))

logging.info("Loading stopwords...")
with(open('data/stopwords.txt', encoding='utf-8', errors='ignore')) as sw_file:
    for sw in sw_file:
        stopwords.add(sw.replace('\n','').lower())
logging.info("%s stopwords loaded." % len(stopwords))

logging.info("Concept service Started")

# run application
app = tornado.web.Application([
    (r"/", NerService, dict(concepts_inlinks = concepts_inlinks, 
                            stopwords = stopwords, 
                            entities = entities,
                            MAX_WORDS = MAX_WORDS,
                            MAX_CHARS = MAX_CHARS))])
    

app.listen(sys.argv[1])
IOLoop.instance().start()

    
