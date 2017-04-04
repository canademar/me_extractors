# Elasticsearch persistor

This service tries to provide an intermediate service to write in Elasticsearch.
It will deploy a server that will receive POST request with a document to index and will save that to a redis list.
Another running service will periodically retrieve the redis data and post it to some elasticsearch service.

# Usage
Edit the configuration of elastic_conf.py and redis_conf.py
Use ./start_all.sh

Then POST request can be send against the port 9902. Each POST request is expected to have a body of a single json document to be indexed.

# Docker usage

Build the image. Remember to change the corresponding configurations.

    docker build -t elasticsearch-persistor 

Where elasticsearch-persistor is the name given to this image.


When using the docker image, the elasticsearch configuration can be passed as an argument.

Do:

    docker run': docker run -d -P elasticsearch-persistor '{"bulk_size": 1000, "type": "test_type", "url": "http://172.17.0.1:9200", "index": "test_index"}'

where elasticsearch-persistor is the name of this image.

## Marathon usage

When using this image as part of a Marathon deployment it will be neccessary to pass the command argument so the server knows where is the elasticsearch service.

An example configuration is included as elasticpersistor.json


