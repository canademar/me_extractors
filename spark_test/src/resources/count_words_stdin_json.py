#!/usr/bin/python
#Encoding UTF-8
import fileinput
import json

def count_words(json_input):
    doc = json.loads(json_input)
    raise Exception("doc: %s, %s" % (type(doc), doc))
    text = doc["text"]
    parts = text.split(" ")
    doc["count"] = len(parts)
    return json.dumps(doc)


if __name__ == '__main__':
    for line in fileinput.input():
        print("%s") % (count_words(line))
   
   
