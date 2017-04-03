import requests
import json
import time
import sys
from redis_client import RedisClient
from elastic_conf import elastic_conf
from redis_conf import redis_conf

def persist_redis(redis_conf, elastic_conf):
    rc = RedisClient(redis_conf["host"], redis_conf["port"], redis_conf["db"], redis_conf["collection"])
    length = rc.length()
    print(length)
    i = 0
    bulk_size = elastic_conf["bulk_size"]
    while(i<length):
        docs = rc.popMany(bulk_size) 
        print("Inserting %s documents" % len(docs))
        persist_elasticsearch(docs, elastic_conf)
        i+=bulk_size
       

def persist_elasticsearch(docs, elastic_conf):
    header = {"index":{"_index": elastic_conf["index"], "_type": elastic_conf["type"]}}
    bulk = []
    for doc in docs:
        bulk.append(json.dumps(header))
        bulk.append(doc)
    bulkstring = "\n".join(bulk)+"\n"
    print(bulkstring)
    response = requests.post("%s/_bulk" % elastic_conf["url"], data=bulkstring)
    time.sleep(0.1)
    return str(type(response))
    

if __name__ =="__main__":
    #docs = [{"name":"doc1","some":"thing"}, {"name":"doc2", "some":"otherthing"}]
    #elastic_conf = {"index":"test_index", "type":"test_type", "url":"http://localhost:9200/_bulk", "bulk_size":10}
    #response = persist_elasticsearch(docs, {"index":"test_index", "type":"test_type", "url":"http://localhost:9200/_bulk"})
    #print(response)
    if len(sys.argv)>1:
        selected_elastic_conf=json.loads(sys.argv[1])
    else:
        selected_elastic_conf=elastic_conf
    print("Selected elastic conf: %s " % selected_elastic_conf)
    while(True):
        print("Persisting")
        persist_redis(redis_conf, selected_elastic_conf)
        time.sleep(60)
