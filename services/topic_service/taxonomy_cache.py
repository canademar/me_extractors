# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime

from commons import *

import sys
sys.path.append('./conf')
from db_conf import *
import logging
from collections import defaultdict

class TaxonomyCache:

    def __init__(self):
        self.conn_roiboard = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER,
                                  passwd=DB_PWD, db=DB_NAME)
        self.conn_roiboard.autocommit(True)
        self.conn_informa = pymysql.connect(host=INFORMA_DB_HOST, port=INFORMA_DB_PORT, user=INFORMA_DB_USER,
                                  passwd=INFORMA_DB_PWD, db=INFORMA_DB_NAME)
        self.conn_informa.autocommit(True)
        self.taxonomy_cache = defaultdict(dict) # {service_id: {lang1: {word1:[concept1, concept2], word2:[concept1, concept3]}}}

    def get_taxonomy(self, service_id, lang):
        if self.taxonomy_cache.get(service_id, {}).get(lang, {}) and not self.taxonomy_cache[service_id][lang].has_expired():
            return self.taxonomy_cache[service_id][lang].value
        else:
            taxonomy = self.__fetch_taxonomy_from_db(service_id, lang)
            self.taxonomy_cache[service_id][lang] = CacheValue(taxonomy)
            return taxonomy

    def __get_cursor(self, service_id):
        if int(service_id) > 1000000:
            return self.conn_informa.cursor(pymysql.cursors.DictCursor)
        return self.conn_roiboard.cursor(pymysql.cursors.DictCursor)

    def __fetch_taxonomy_from_db(self, service_id, lang):
        query = """
        select concept.name, concept_vocabulary.word  
        from concept, concept_vocabulary, language where concept.track_id  = %s
        and concept.id = concept_vocabulary.concept_id and language.name = '%s' 
        and language.id = concept_vocabulary.language_id;""" % (service_id, lang)
        cursor = self.__get_cursor(service_id)
        cursor.execute(query)
        taxonomy = {}
        for row in cursor.fetchall():
            concept = row['name']
            word = row['word']
            word = remove_accents(word.lower().strip())
            if word not in taxonomy:
                taxonomy[word] = []
            taxonomy[word].append(concept.strip())
        cursor.close()
        return taxonomy

class CacheValue:

    def __init__(self, value):
        self.date = datetime.now()
        self.value = value

    def has_expired(self):
        now = datetime.now()
        return (now - self.date).seconds > 3600*24

if __name__ == '__main__':
    cache = TaxonomyCache()
    print(cache.get_taxonomy('3'))


