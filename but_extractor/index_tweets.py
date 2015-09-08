#!/usr/bin/python
from glob import glob
import os
from download_but import download_new_files, DOWNLOAD_FOLDER
from filter_twitter import filter_twitter_files
from elastic_insert import index_tweets

FILTERED_DATA = "filtered/*/*"

def main():
    old_files = glob(DOWNLOAD_FOLDER + "*/*")
    print "Cleaning filtered data"
    clean_filtered_data()
    print "Downloading new files"
    download_new_files()
    print "Filtering data"
    filter_twitter_files()
    print "Indexing tweets"
    index_tweets()
    print "Removing old files: %s" % str(old_files)
    for file_path in old_files:
        os.remove(file_path)


def clean_filtered_data():
    paths = glob(FILTERED_DATA)
    for path in paths:
        os.remove(path)

if __name__ == "__main__":
    main()



