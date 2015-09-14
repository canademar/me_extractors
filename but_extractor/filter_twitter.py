#!/usr/bin/python
import logging
from glob import glob
import json
from datetime import datetime
import time
import re


ALLOWED_LANGUAGES = ["es", "en", "cz"]
TWEET_FIELDS = {"id":str, "text":str, "lang":str, "created_at":datetime, "user":{"screen_name":str}}#, "entities":[{"hashtags":{"text":list}, "user_mentions":{"screen_name":list}}]} 



def filter_twitter_files():
    inputs = glob("data/*/*.json")
    for filename in inputs:
        filter_file(filename)

def filter_file(path):
    name = re.findall("\/([^\/]+.json)", path)[0]
    keyword = re.findall("\/([^\/]+)\/.+json", path)[0]
    logging.info("Filtering %s %s" %(keyword, name))
    filtered_tweets = []
    with open(path,'r') as input_file:
        input_json = input_file.read()
        if input_json[-2]==",":
            logging.info("Cleaning...")
            input_json = input_json[:-2]+"]"
        data = json.loads(input_json)
        for tweet in data:
            if tweet['lang'] not in ALLOWED_LANGUAGES:
                continue
            filtered_tweet = filter_tweet(tweet, keyword)
            filtered_tweets.append(filtered_tweet)
    path = 'filtered/%s/%s' % (keyword, name)
    with open(path, 'w') as output:
        output.write(json.dumps(filtered_tweets))

def filter_tweet(tweet, keyword):
    filtered_tweet = {}
    filtered_tweet["id"] = tweet["id"]
    filtered_tweet["keyword"] = keyword
    filtered_tweet["project"] = keyword
    filtered_tweet["text"] = tweet["text"]
    filtered_tweet["lang"] = tweet["lang"]
    #filtered_tweet["created_at"] = parse_date(tweet["created_at"])
    filtered_tweet["created_at"] = tweet["created_at"]
    filtered_tweet["screen_name"] = tweet["user"]["screen_name"]
    filtered_tweet["hashtags"] = []
    for hashtag in tweet["entities"]["hashtags"]:
        filtered_tweet["hashtags"].append(hashtag["text"])
    filtered_tweet["mentions"] = []
    for mention in tweet["entities"]["user_mentions"]:
        filtered_tweet["mentions"].append(mention["screen_name"])
    return filtered_tweet



#def filter_tweet(tweet):
#    filtered = {}
#    for field_name, types in TWEET_FIELDS.iteritems():
#        (name, value) = extract_field(tweet, field_name, types) 
#        filtered[name] = value
#    return filtered
#
#def extract_field(tweet, field_name, types):
#    if types == str:
#        name = field_name
#        value = tweet[field_name]
#    elif types == datetime:
#        name = field_name
#        value = parse_date(tweet["created_at"])
#    return (name, value)

def parse_date(date_str):
    return datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y')


if __name__=="__main__":
    filter_twitter_files()
