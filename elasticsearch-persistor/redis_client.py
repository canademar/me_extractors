import redis
from redis_conf import redis_conf


class RedisClient:


    def __init__(self, host, port, db, collection):
        self.rdb = redis.StrictRedis(host=host, port=port, db=db)
        self.collection = collection

    def length(self):
        return self.rdb.llen(self.collection)

    def put(self, document):
        print("inserting")
        self.rdb.lpush(self.collection,document)
    def get(self):
        result =  self.rdb.rpop(self.collection)
        if result!=None:
            result = result.decode('utf-8')
        return result
    def popMany(self, num):
        results = []
        for i in range(num):
            doc = self.get()
            if doc != None:
                results.append(doc)
        return results


if __name__=='__main__':
     print("Now")
     redisClient = RedisClient(redis_conf["host"], redis_conf["port"], redis_conf["db"], redis_conf["collection"])
     redisClient.put('{"some":"document"}')
     print(redisClient.get())
     for i in range(111):
        redisClient.put('{"id":"num_%s"}' % i)
     print(redisClient.popMany(100))
