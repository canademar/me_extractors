#!/usr/bin/python
import requests
import re

URL_FILE_LIST = "http://athena3.fit.vutbr.cz:8081/timestamp_all/2015-08-01_08-00-00"
DOWNLOAD_FOLDER = "data/"

def main():
    file_list = get_file_list()
    print file_list
    for url in file_list:
        save_file(url)

def save_file(url):
    response = requests.get(url)
    content = response.content
    name = re.findall("\/([^\/]+.json)", url)[0]
    print "Saving " + name
    with open(DOWNLOAD_FOLDER + name, 'w') as output:
        output.write(content)
    

def get_file_list():
    response = requests.get(URL_FILE_LIST)
    content = response.content
    file_list = content.split("\n")
    return file_list
    

if __name__ == "__main__":
    main()
