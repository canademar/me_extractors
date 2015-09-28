#!/usr/bin/python
import sys
from glob import glob
import os
import logging
from download_but import download_new_files, DOWNLOAD_FOLDER
from filter_twitter import filter_twitter_files
from elastic_insert import index_tweets

FILTERED_DATA = "filtered/*/*"

def main():
    old_files = glob(DOWNLOAD_FOLDER + "*/*")
    logging.info("Cleaning filtered data")
    clean_filtered_data()
    logging.info("Downloading new files")
    download_new_files()
    logging.info("Filtering data")
    filter_twitter_files()
    logging.info("Indexing tweets")
    index_tweets()
    logging.info("Removing old files: %s" % str(old_files))
    for file_path in old_files:
        os.remove(file_path)


def clean_filtered_data():
    paths = glob(FILTERED_DATA)
    for path in paths:
        os.remove(path)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, filename="but_tw_indexer.log", filemode="w")
    #logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    tracer = logging.getLogger('elasticsearch.trace')
    tracer.setLevel(logging.INFO)

    main()



