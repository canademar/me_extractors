import json
import urllib2
from glob import glob
from elasticsearch import Elasticsearch, helpers


def main():
  es = Elasticsearch(["http://mixednode1:9200"], use_ssl=False)
  inputs = glob("filtered/*.json")
  print inputs
  for filename in inputs:
      print "going to index %s" % filename
      with open(filename, 'r') as input:
          docs = json.loads(input.read())
          if es.exists(index="tweets",id=docs[0]["id"]):
              print "Skipping"
              continue
          total = len(docs)
          i = 0
          index_doc = []
          for doc in docs:
              #es.index(index="tweets", doc_type="opinion", id=doc["id"], body=doc)
              i +=1     
              index_doc.append({"_index":"tweets", "_type":"opinion", "id":doc["id"], "_source":doc})
              if len(index_doc)==500:
                  print "indexing %s/%s" % (i, total)
                  helpers.bulk(es, index_doc)
                  index_doc = []
          if len(index_doc)!=0:
              print "indexing %s/%s" % (i, total)
              helpers.bulk(es, index_doc)
              
           
        
      

              

    
    


if __name__ == '__main__':
    main()

