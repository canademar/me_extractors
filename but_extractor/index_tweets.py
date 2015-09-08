#!/usr/bin/python
import shutil
from download_but import download_new_files
from filter_twitter import filter_twitter_files
from elastic_insert import index_tweets

FILTERED_DATA = "filtered/"

def main():
    print "Cleaning filtered data"
    clean_filtered_data()
    print "Downloading new files"
    download_new_files()
    print "Filtering data"
    filter_twitter_files()
    print "Indexing tweets"
    index_tweets()


def clean_filtered_data():
    shutil.rmtree(FILTERED_DATA) 

if __name__ == "__main__":
    main()



