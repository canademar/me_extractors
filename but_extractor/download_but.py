#!/usr/bin/python
import sys
import logging
import glob
import re
import requests
import os

URL_FILE_LIST = "http://athena3.fit.vutbr.cz:8081/timestamp_all/%s"
DOWNLOAD_FOLDER = "data/"
DEFAULT_SINCE_DATE = "2015-09-01_00-00-00"

def download_new_files():
    file_list = get_file_list()
    logging.info(file_list)
    for url in file_list:
        save_file(url)


def get_since_date():
    sorted_files = sorted(glob.glob(DOWNLOAD_FOLDER + "*/*"))
    logging.info(sorted_files)
    if len(sorted_files)==0:
        return DEFAULT_SINCE_DATE
    last = sorted_files[-1]
    since_date = re.findall("\/([^\/]+).json", last)[0]
    return since_date
    

def save_file(url):
    response = requests.get(url)
    content = response.content
    logging.info("Url: %s" % url)
    name = re.findall("\/([^\/]+.json)", url)[0]
    keyword = re.findall("8001\/([^\/]+)", url)[0]
    path = DOWNLOAD_FOLDER + "/"+keyword+"/"+name
    logging.info("Saving " + path)
    with open(path, 'w') as output:
        output.write(content)
    

def get_file_list():
    since_date = get_since_date()
    logging.info("Getting files from: %s" % since_date)
    response = requests.get(URL_FILE_LIST % since_date)
    content = response.content
    file_list = content.split("\n")
    file_list = [filename for filename in file_list if filename!='']
    return file_list
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.debug, stream=sys.stdout)
    download_new_files()
