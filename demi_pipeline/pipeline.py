# -*- coding: utf-8 -*-
import json
import logging
import sys
from datetime import datetime, timedelta

from elastic_wrapper import ElasticWrapper

sys.path.append("../services/topic_service")
from example_taxonomy import taxonomy 
from classification import Classificator

sys.path.append("../services/concept_service")
from ner import Ner

START_DAY = datetime(2011, 9, 6)
DAYS_LOG_PATH = "days.log"


def pipeline():
    classificator = Classificator(taxonomy)
    ner = initializeNer()
    elastic = ElasticWrapper()
    day = START_DAY
    today = datetime.now()
    days_log = open(DAYS_LOG_PATH, 'a')
    while(day < today):
        t0 = datetime.now()
        logging.info("Quering for day: %s" % day.strftime("%Y-%m-%dT%H:%M:%SZ"))
        days_log.write("Quering for day: %s\n" % day.strftime("%Y-%m-%dT%H:%M:%SZ"))                
        tweets_count = elastic.tweets_count_for_day(day)
        count = 0
        while(count<tweets_count):
            tweets = elastic.get_day_tweets(day, count)
            count += 1000
            logging.info("Received %s tweets" % len(tweets))
            logging.info("Got %s/%s tweets" % (count, tweets_count))
            days_log.write("Got %s/%s tweets\n" % (count+len(tweets), tweets_count))
            result_tweets = []
            for tweet in tweets:
                logging.debug("Analyzing tweet: %s" % tweet)
                result_tweet = analyze_doc(classificator, ner, tweet)
                logging.debug("Result tweet: %s" % result_tweet)
                result_tweet["source"] = "twitter"
                result_tweets.append(result_tweet)
            elastic.insert(result_tweets, "analyzed", "tweet")
        t1 = datetime.now()
        logging.info("This day took %s" % (t1-t0))
        days_log.write("This day took %s\n" % (t1-t0) )
        day = day + timedelta(days=1)
    logging.info("Finished!!!")
    days_log.write("This day took %s\n" % (t1-t0) )
    days_log.close()


def pipeline_dw():
    classificator = Classificator(taxonomy)
    ner = initializeNer()
    elastic = ElasticWrapper()
    day = START_DAY
    today = datetime.now()
    days_log = open(DAYS_LOG_PATH, 'a')
    t0 = datetime.now()
    logging.info("Quering for day: %s" % day.strftime("%Y-%m-%dT%H:%M:%SZ"))
    days_log.write("Quering for day: %s\n" % day.strftime("%Y-%m-%dT%H:%M:%SZ"))                
    articles_count = elastic.articles_count_from(day)
    count = 0
    while(count<articles_count):
        articles = elastic.get_articles_from(day, count)
        count += 1000
        logging.info("Received %s articles" % len(articles))
        logging.info("Got %s/%s articles" % (count, articles_count))
        days_log.write("Got %s/%s articles\n" % (count+len(articles), articles_count))
        result_articles = []
        for article in articles:
            logging.debug("Analyzing article: %s" % article)
            result_article = analyze_doc(classificator, ner, article)
            logging.debug("Result article: %s" % result_article)
            result_article["source"] = "dw"
            result_articles.append(result_article)
        elastic.insert(result_articles, "analyzed", "article")
    t1 = datetime.now()
    logging.info("This day took %s" % (t1-t0))
    days_log.write("This day took %s\n" % (t1-t0) )
    logging.info("Finished!!!")
    days_log.write("This day took %s\n" % (t1-t0) )
    days_log.close()

#def analyze_tweet(classificator, ner, tweet):
#    topics = extract_topics(classificator, tweet["text"])
#    logging.debug("Topics:%s" % topics)
#    concepts = extract_concepts(ner, tweet["text"])
#    logging.debug("Concepts:%s" % concepts)
#    result_tweet = tweet.copy()
#    result_tweet["topics"] = topics
#    result_tweet["concepts"] = concepts
#    return result_tweet
    
def analyze_doc(classificator, ner, doc):
    topics = extract_topics(classificator, doc["text"])
    logging.debug("Topics:%s" % topics)
    concepts = extract_concepts(ner, doc["text"])
    logging.debug("Concepts:%s" % concepts)
    result_doc = doc.copy()
    result_doc["topics"] = topics
    result_doc["concepts"] = concepts
    return result_doc

def extract_concepts(ner, text):
    entities = ner.fetch_entities(text)
    concepts = list(entities["results"].keys())
    return concepts

def initializeNer():
    concepts_inlinks = {}
    stopwords = set()
    entities = set()

    logging.info("Loading concepts...")
    with(open('../services/concept_service/data/pagelinks_all.tsv', encoding='utf-8', errors='ignore')) as concepts_file:
        for concept in concepts_file.readlines():
            parts = concept.split('\t')
            concepts_inlinks[parts[0]]=parts[1]
    logging.info("%s concepts loaded." % len(concepts_inlinks))

    logging.info("Loading stopwords...")
    with(open('../services/concept_service/data/stopwords.txt', encoding='utf-8', errors='ignore')) as sw_file:
        for sw in sw_file:
            stopwords.add(sw.replace('\n','').lower())
    logging.info("%s stopwords loaded." % len(stopwords))
    return Ner(concepts_inlinks, entities, stopwords, inlinks_threshold=400, max_words=500)
    

def extract_topics(classificator, text):
    classification = classificator.classify(text)
    topics = set()
    for word in classification.keys():
        for topic in classification[word]:
            topics.add(topic)
    topics = list(topics)
    return topics

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    pipeline_dw()
