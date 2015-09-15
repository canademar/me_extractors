# -*- coding: utf-8 -*-

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
        inlinks_processor = Inlinks(text, self.inlinks_threshold, self.concepts, self.stopwords, self.entities, True)
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