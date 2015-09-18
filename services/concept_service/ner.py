# -*- coding: utf-8 -*-
import logging
import sys
from inlinks import Inlinks

class Ner(object):

    def __init__(self, concepts, entities, stopwords, inlinks_threshold=100, max_words=400):
        self.inlinks_threshold = inlinks_threshold
        self.concepts = concepts
        self.entities = entities
        self.stopwords = stopwords
        self.MAX_WORDS = max_words

    def fetch_entities(self, text):
        # Process only MAX_WORDS words
        chunks = text.split(' ')
        text = ' '.join(chunks[0 : self.MAX_WORDS])
        # Flatten text
        text = self.flatten(text)  
        # Calculate all possible overlays
        inlinks_processor = Inlinks(text, self.inlinks_threshold, self.concepts, self.stopwords, self.entities, False)
        # Return json repsonse
        return inlinks_processor.process()

    def flatten(self , text):
        """
        """
        text = text.lower();
        text = text.replace('\t' , '')
        text = text.replace('\n' , '')
        text = text.replace('á' , 'a')
        text = text.replace('é' , 'e')
        text = text.replace('í' , 'i')
        text = text.replace('ó' , 'o')
        text = text.replace('ú' , 'u')
        text = text.replace('"' , '')
        text = text.replace('(' , '')
        text = text.replace(')' , '')
        text = text.replace('!' , '')
        text = text.replace('¡' , '')
        text = text.replace('?' , '')
        text = text.replace('¿' , '')
        text_mark = text.replace(',' , '')
        text_mark = text_mark.replace(';' , '')
        text_mark = text_mark.replace(':' , '')
        text_mark = text_mark.replace('.' , '')
        return text_mark

if __name__=="__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    # data structures to load inside the data
    concepts_inlinks={}
    stopwords=set()
    entities=set()
    
    # Restrictions
    MAX_WORDS = 400
    MAX_CHARS = MAX_WORDS * 50
    
    
    print("This is a NER service")
    
    logging.warning("Loading concepts...")
    with(open('data/pagelinks_all.tsv', encoding='utf-8', errors='ignore')) as concepts_file:
        for concept in concepts_file.readlines():
            parts = concept.split('\t')
            concepts_inlinks[parts[0]]=parts[1]
    logging.warning("%s concepts loaded." % len(concepts_inlinks))
    
    logging.warning("Loading entities...")
    with(open('data/entities.txt', encoding='utf-8', errors='ignore')) as entities_file:
        for entity in entities_file.readlines():
            entities.add(entity[0:len(entity)-1])
    logging.warning("%s entities loaded." % len(entities))
    
    logging.warning("Loading stopwords...")
    with(open('data/stopwords.txt', encoding='utf-8', errors='ignore')) as sw_file:
        for sw in sw_file:
            stopwords.add(sw.replace('\n','').lower())
    logging.warning("%s stopwords loaded." % len(stopwords))
    logging.debug("Stopwords: %s" % stopwords)
    logging.debug("Is cia a stopword? %s" % ('cia' in stopwords))
    
    
    
    # run application
    ner = Ner(concepts_inlinks, entities, stopwords)
    #ner.fetch_entities("EEUU cierra la investigación sobre las torturas de la CIA patata sin acusados pero el ceo lo niega")
    ner.fetch_entities("EEUU cierra la investigación sobre las torturas de Florentino a Iker Casillas")
