import json
import urllib2
from glob import glob
from elasticsearch import Elasticsearch


def main():
  es = Elasticsearch(["http://mixednode1:9200"], use_ssl=False)
  inputs = glob("filtered/*.json")
  for filename in inputs:
      with open(filename, 'r') as input:
          docs = json.loads(input.read())
          i = 0
          for doc in docs:
              print doc

              es.index(index="tweets", doc_type="opinion", id=doc["id"], body=doc)



if __name__ == '__main__':
    main()

