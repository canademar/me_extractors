import json
import logging
import urllib2
from glob import glob
from datetime import datetime
from elasticsearch import Elasticsearch, helpers


def index_tweets():
  es = Elasticsearch(["http://mixednode1:9200"], use_ssl=False)
  inputs = glob("filtered/*/*.json")
  logging.info(inputs)
  for filename in inputs:
      logging.info("going to index %s" % filename)
      with open(filename, 'r') as input:
          docs = json.loads(input.read())
          if es.exists(index="tweets",id=docs[0]["id"]):
              logging.info("Skipping")
              continue
          total = len(docs)
          i = 0
          index_doc = []
          for doc in docs:
              #es.index(index="tweets", doc_type="opinion", id=doc["id"], body=doc)
              i +=1    
              doc["created_at"] = parse_date(doc["created_at"]) 
              index_doc.append({"_index":"tweets", "_type":"tweet", "id":doc["id"], "_source":doc})
              if len(index_doc)==500:
                  logging.info("indexing %s/%s" % (i, total))
                  helpers.bulk(es, index_doc)
                  index_doc = []
          if len(index_doc)!=0:
              logging.info("indexing %s/%s" % (i, total))
              helpers.bulk(es, index_doc)
              
           
        
      

def parse_date(date_str):
    return datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')

              

    
    


if __name__ == '__main__':
    index_tweets()

