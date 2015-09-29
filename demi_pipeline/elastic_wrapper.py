import json
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

DW_NEWS = "dw_news"
TWEETS = "tweets"

class ElasticWrapper:

    

    def __init__(self):
         self.es = Elasticsearch(["http://mixednode1:9200"], use_ssl=False)

    def insert(self, docs, index, docs_type):
          total = len(docs)
          i = 0
          index_doc = []
          for doc in docs:
              i +=1    
              index_doc.append({"_index":index, "_type":docs_type, "_id":doc["id"], "_source":doc})
              if len(index_doc)==1000:
                  logging.info("indexing %s/%s" % (i, total))
                  helpers.bulk(self.es, index_doc)
                  index_doc = []
          if len(index_doc)!=0:
              logging.info("indexing %s/%s" % (i, total))
              helpers.bulk(self.es, index_doc)

    def get_one_tweet(self):
        res = self.es.search(index=TWEETS, body={"query": {"match_all": {}}})
        result = res['hits']['hits'][0]["_source"]
        return result

    def get_day_tweets(self, day, offset):
        from_date = day.strftime("%Y-%m-%dT00:00:00Z")
        to_date = day.strftime("%Y-%m-%dT23:59:59Z")
        res = self.es.search(index=TWEETS, body={"query": {"range": {"created_at":{"gte":from_date, "lte":to_date}}}, "size":1000, "from":offset})
        result = res['hits']['hits']
        result = list(map((lambda x: x['_source']), result))
        return result

    def tweets_count_for_day(self, day): 
        from_date = day.strftime("%Y-%m-%dT00:00:00Z")
        to_date = day.strftime("%Y-%m-%dT23:59:59Z")
        res = self.es.search(index=TWEETS, body={"query": {"range": {"created_at":{"gte":from_date, "lte":to_date}}}, "size":0})
        result = res['hits']['total']
        return result

    def articles_count_from(self, day): 
        from_date = day.strftime("%Y-%m-%dT00:00:00Z")
        res = self.es.search(index=DW_NEWS, body={"query": {"range": {"created_at":{"gte":from_date}}}, "size":0})
        result = res['hits']['total']
        return result

    def get_articles_from(self, day, offset):
        from_date = day.strftime("%Y-%m-%dT00:00:00Z")
        res = self.es.search(index=DW_NEWS, body={"query": {"range": {"created_at":{"gte":from_date}}}, "size":1000, "from":offset})
        result = res['hits']['hits']
        result = list(map((lambda x: x['_source']), result))
        return result

      
if __name__=="__main__":
    es = ElasticWrapper()
    day = datetime(2011, 9, 5)
    res = es.tweets_count_for_day(day)
    print(res)
    res = es.get_tweets_for_day(day,0)
    print(res[0])

                  
