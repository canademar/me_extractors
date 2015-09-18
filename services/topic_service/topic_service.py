# -*- coding: utf-8 -*-

import json
from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
import logging
import sys
from urllib.parse import unquote_plus
from example_taxonomy import taxonomy as example_taxonomy

#from taxonomy_cache import TaxonomyCache
from classification import Classificator

class ClassificationService(RequestHandler):

    def initialize(self):
        #self.cache = TaxonomyCache()
        self.classificator = Classificator()

    def get(self):
        logging.debug("Received GET request")
        service_id = self.get_argument('service_id', default=None)
        text = self.get_argument('text')
        lang = self.get_argument('lang', default=None)
        taxonomy = self.get_argument('taxonomy', default=None)
        if taxonomy:
            taxonomy = json.loads(taxonomy)
        else:
            #taxonomy = self.cache.get_taxonomy(service_id, lang)
            taxonomy = example_taxonomy
        logging.debug(taxonomy)
        classification = self.classificator.classify(taxonomy, text)
        logging.debug(classification)
        result = self.__get_concepts_from_classification(classification)
        if result:
            self.write(json.dumps(result))
        else:
            self.write(json.dumps(['N/A']))

    def post(self):
        results = list()
        logging.debug("Received POST request")
        for line in str(self.request.body, 'utf8').split('\n'):
            fields = line.split('\t')
            text = unquote_plus(unquote_plus(fields[0]))
            logging.debug("Classificating %s" % text)
            taxonomy = example_taxonomy
            classification = self.classificator.classify(taxonomy, text)
            #concepts = self.__format_post_classification(classification)
            #result = '\t'.join([concepts] + fields)
            #logging.debug("Result is [%s]" % result)
            result = {"text":text, "topics":self.__get_concepts_from_classification(classification)}
            results.append(result)
        #for temp in results:
        #    self.write('%s\n' % (temp))
        self.write({"response":results})

    def __get_concepts_from_classification(self, classification):
        unique_concepts = set()
        for word, concepts in classification.items():
            unique_concepts|=set(concepts)
        return list(unique_concepts)

    def __format_post_classification(self, classification):
        concepts = self.__get_concepts_from_classification(classification)
        if concepts:
            return ';;'.join(concepts)
        else:
            return 'N/A'

print('Topic classification service started')

app = Application([(r'/', ClassificationService)])
app.listen(sys.argv[1])
IOLoop.instance().start()


