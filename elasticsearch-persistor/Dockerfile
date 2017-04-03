FROM python:3-onbuild
MAINTAINER Carlos Navarro cnavarro@paradigmadigital.com
RUN apt-get update
RUN apt-get install redis-tools -y
RUN apt-get install redis-server -y
WORKDIR /usr/src/app
ENTRYPOINT ["./es_indexer_service.sh"]
CMD [""]
EXPOSE 9902

