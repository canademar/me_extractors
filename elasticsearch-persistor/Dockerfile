FROM python:3-onbuild
MAINTAINER Carlos Navarro cnavarro@paradigmadigital.com
RUN apt-get update
RUN apt-get install redis-tools -y
RUN apt-get install redis-server -y
WORKDIR /usr/src/app
#CMD ["/etc/init.d/redis-server","start","&&","python3", "elasticsearch_queuer_server.py"]
CMD ./start_all.sh start && tail -f logs/elasticsearch_queuer_server.log
EXPOSE 9902

