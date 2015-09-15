NER Service
======================

Named Entity Recognition service calculated using Wikipedia articles.

Installation
------------

Python 3 is necessary to run the service. Required libraries are:
* tornado

Starting and stopping the service
---------------------------------

There is a script that starts and stops the service with the desired configuration:

	./launcher.sh start
	./launcher.sh stop

This same script contains the configuration for running the service with a given
number of separated processes, using the port range as defined.

Calling the service
-------------------

This service admits both GET and POST requests.

### Calling the service via GET

An example call would be:

	http://[ip_address]:[port]/?inlinks_threshold=100&text=EEUU%20cierra%20la%20investigación%20sobre%20las%20torturas%20de%20la%20CIA%20sin%20acusados

This call returns a JSON object with the elapsed time, the detected entities and
some additional information.

### Calling the service via POST

The POST interface admits a TSV file inside the body:

	line_number	service_id	text
	line_number	service_id	text
	...
	line_number	service_id	text

The response is also a TSV-formatted text:

	line_number	service_id	service_id	concepts
	line_number	service_id	service_id	concepts
	...
	line_number	service_id	service_id	concepts


