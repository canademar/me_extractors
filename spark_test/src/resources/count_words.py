#!/usr/bin/python
#Encoding UTF-8
import sys


def count_words(text):
    parts = text.split(" ")
    return len(parts)


if __name__ == '__main__':
    if len(sys.argv)!=2:
        print("Missing text line. Args:" + str(sys.argv))
    else:
        print count_words(sys.argv[1])
   
   
