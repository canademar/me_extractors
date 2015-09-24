# -*- coding: utf-8 -*-
import json
import logging
import sys

sys.path.append("../services/topic_service")
from example_taxonomy import taxonomy 
from classification import Classificator

sys.path.append("../services/concept_service")
from ner import Ner


def pipeline():
    classificator = Classificator()
    ner = initializeNer()
    text = "Cristiano Ronaldo es el jefe del vestuario"
    topics = extract_topics(classificator, text)
    logging.info(topics)
    concepts = extract_concepts(ner, text)
    logging.info("Concepts:%s" % concepts)


def extract_concepts(ner, text):
    entities = ner.fetch_entities(text)
    concepts = list(entities["results"].keys())
    return concepts

def initializeNer():
    concepts_inlinks = {}
    stopwords = set()
    entities = set()

    logging.info("Loading concepts...")
    with(open('../services/concept_service/data/pagelinks_all.tsv', encoding='utf-8', errors='ignore')) as concepts_file:
        for concept in concepts_file.readlines():
            parts = concept.split('\t')
            concepts_inlinks[parts[0]]=parts[1]
    logging.info("%s concepts loaded." % len(concepts_inlinks))

    logging.info("Loading stopwords...")
    with(open('../services/concept_service/data/stopwords.txt', encoding='utf-8', errors='ignore')) as sw_file:
        for sw in sw_file:
            stopwords.add(sw.replace('\n','').lower())
    logging.info("%s stopwords loaded." % len(stopwords))
    return Ner(concepts_inlinks, entities, stopwords, inlinks_threshold=400, max_words=500)
    

def extract_topics(classificator, text):
    classification = classificator.classify(taxonomy, text)
    topics = set()
    for word in classification.keys():
        for topic in classification[word]:
            topics.add(topic)
    topics = list(topics)
    return topics

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    pipeline()
