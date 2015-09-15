# -*- coding: utf-8 -*-

import json
from tornado.web import Application, RequestHandler
from tornado.ioloop import IOLoop
import logging
import sys
from urllib.parse import unquote_plus

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
            from example_taxonomy import taxonomy
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
            if len(fields) == 4:
                line_number = fields[0]
                service_id = unquote_plus(unquote_plus(fields[1]))
                lang = fields[2]
                text = unquote_plus(unquote_plus(fields[3]))
                logging.debug("Classificating %s" % text)
                taxonomy = self.cache.get_taxonomy(service_id, lang)
                classification = self.classificator.classify(taxonomy, text)
                concepts = self.__format_post_classification(classification)
                result = '\t'.join([line_number, service_id, lang, concepts])
                logging.debug("Result is [%s]" % result)
                results.append(result)
            if len(fields) == 3:
                line_number = fields[0]
                taxonomy = json.loads(fields[1])
                classification = self.classificator.classify(taxonomy, text)
                concepts = self.__format_post_classification(classification)
                result = '\t'.join([line_number, concepts])
                results.append(result)
        for temp in results:
            self.write('%s#@@#' % (temp))

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

print('Classification service started')

app = Application([(r'/', ClassificationService)])
app.listen(sys.argv[1])
IOLoop.instance().start()


