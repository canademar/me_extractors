Classification Service
======================

Classifies the given text within a set of concepts, retrieved using an unique
service id and a language.

Installation
------------

Python 3 is necessary to run the service. Required libraries are:
* tornado
* pymysql

The service requires an available mysql instance, containing two tables named
`concept` and `concept_vocabulary`, with an structure like this:
* `concept` table

<table>
	<tr>
		<th>Field</th>
		<th>Type</th>
		<th>Null</th>
		<th>Key</th>
		<th>Default</th>
		<th>Extra</th>
	</tr>
	<tr>
		<td>id</td>
		<td>bigint(20)</td>
		<td>NO</td>
		<td>PRI</td>
		<td>NULL</td>
		<td>auto_increment</td>
	</tr>
	<tr>
		<td>name</td>
		<td>varchar(255)</td>
		<td>NO</td>
		<td></td>
		<td>NULL</td>
		<td></td>
	</tr>
	<tr>
		<td>track_id</td>
		<td>bigint(20)</td>
		<td>YES</td>
		<td>MUL</td>
		<td>NULL</td>
		<td></td>
	</tr>
</table>

* `concept_vocabulary` table

<table>
<tr>
	<th>Field</th>
	<th>Type</th>
	<th>Null</th>
	<th>Key</th>
	<th>Default</th>
	<th>Extra</th>
</tr>
<tr>
	<td>id</td>
	<td>bigint(20)</td>
	<td>NO</td>
	<td>PRI</td>
	<td>NULL</td>
	<td>auto_increment</td>
</tr>
<tr>
	<td>concept_id</td>
	<td>bigint(20)</td>
	<td>YES</td>
	<td>MUL</td>
	<td>NULL</td>
	<td></td>
</tr>
<tr>
	<td>word</td>
	<td>varchar(255)</td>
	<td>NO</td>
	<td></td>
	<td>NULL</td>
	<td></td>
</tr>
</table>

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

	http://[ip_address]:[port]/?service_id=3&text=La tarifa de movistar es muy mala&lang=es

This call returns a JSON array with the matched concepts for the given text, language
and service, or "N/A" if there is not any match.

### Calling the service via POST

The POST interface admits a TSV file inside the body:

	line_number	service_id	lang	text
	line_number	service_id	lang	text
	...
	line_number	service_id	lang	text

The response is also a TSV-formatted text:

	line_number	service_id	service_id	lang	concepts
	line_number	service_id	service_id	lang	concepts
	...
	line_number	service_id	service_id	lang	concepts


