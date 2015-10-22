#!/usr/bin/python
#Encoding UTF-8
import fileinput

def count_words(text):
    parts = text.split(" ")
    return len(parts)


if __name__ == '__main__':
    for line in fileinput.input():
        print("%s\t%s") % (line.strip(), count_words(line))
   
   
