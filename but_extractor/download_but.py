#!/usr/bin/python
import glob
import re
import requests
import os

URL_FILE_LIST = "http://athena3.fit.vutbr.cz:8081/timestamp_all/%s"
DOWNLOAD_FOLDER = "data/"

def main():
    old_files = glob.glob(DOWNLOAD_FOLDER + "*")
    file_list = get_file_list()
    print file_list
    for url in file_list:
        save_file(url)
    for file_path in old_files:
        os.remove(file_path)


def get_since_date():
    sorted_files = sorted(glob.glob(DOWNLOAD_FOLDER + "*"))
    last = sorted_files[-1]
    since_date = re.findall("\/([^\/]+).json", last)[0]
    return since_date
    

def save_file(url):
    response = requests.get(url)
    content = response.content
    name = re.findall("\/([^\/]+.json)", url)[0]
    print "Saving " + name
    with open(DOWNLOAD_FOLDER + name, 'w') as output:
        output.write(content)
    

def get_file_list():
    since_date = get_since_date()
    print "Getting files from: %s" % since_date
    response = requests.get(URL_FILE_LIST % since_date)
    content = response.content
    file_list = content.split("\n")
    file_list = [filename for filename in file_list if filename!='']
    return file_list
    

if __name__ == "__main__":
    main()
