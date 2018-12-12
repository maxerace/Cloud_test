# -*- coding: utf-8 -*-
import sys
import ast
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import requests
from operator import add
import csv
import numpy as np
import threading
import time
from requests_oauthlib import OAuth1Session
import requests_oauthlib
import string
from time import gmtime, strftime

#nltk libraries
import nltk
from nltk.classify import NaiveBayesClassifier
from nltk.classify import NaiveBayesClassifier
from nltk.corpus import subjectivity
from nltk.sentiment import SentimentAnalyzer
from nltk.sentiment.util import *
from nltk.corpus import stopwords


BATCH_INTERVAL = 45  # How frequently to update
BLOCKSIZE = 1000  # How many tweets per update

# Set up spark objects
sc  = SparkContext('local[1]', 'TwitterSampleStream')
ssc = StreamingContext(sc, BATCH_INTERVAL)

#build nltk training model
#read in the text file and create a dataset in memory
train_file = sc.textFile("hdfs://localhost:8020/user/cloudera/final/Sentiment Analysis Dataset.csv")
train_header = train_file.take(1)[0]
train_data_raw = train_file.filter(lambda line: line != train_header)

#define a function that splits apart the rows for each line and 
#creates a tuple and removes punctuation
def get_row(line):
  row = line.split(',')
  sentiment = row[1]
  tweet = row[3].strip()
  translator = str.maketrans({key: None for key in string.punctuation})
  tweet = tweet.translate(translator)
  tweet = tweet.split(' ')
  tweet_lower = []
  for word in tweet:
    tweet_lower.append(word.lower())
  return (tweet_lower, sentiment)

#call the function on each row in the dataset
train_data = train_data_raw.map(lambda line: get_row(line))

#create a SentimentAnalyzer object
sentim_analyzer = SentimentAnalyzer()

#get list of stopwords (with _NEG) to use as a filter
stopwords_all = []
for word in stopwords.words('english'):
  stopwords_all.append(word)
  stopwords_all.append(word + '_NEG')

#take 10,000 Tweets from this training dataset for this example and get all the words
#that are not stop words
train_data_sample = train_data.take(10000)
all_words_neg = sentim_analyzer.all_words([mark_negation(doc) for doc in train_data_sample])
all_words_neg_nostops = [x for x in all_words_neg if x not in stopwords_all]

#create unigram features and extract features
unigram_feats = sentim_analyzer.unigram_word_feats(all_words_neg_nostops, top_n=200)
sentim_analyzer.add_feat_extractor(extract_unigram_feats, unigrams=unigram_feats)
training_set = sentim_analyzer.apply_features(train_data_sample)

#train the model
trainer = NaiveBayesClassifier.train
classifier = sentim_analyzer.train(trainer, training_set)

#classify test sentences
test_sentence1 = [(['this', 'program', 'is', 'bad'], '')]
test_sentence2 = [(['tough', 'day', 'at', 'work', 'today'], '')]
test_sentence3 = [(['good', 'wonderful', 'amazing', 'awesome'], '')]
test_set = sentim_analyzer.apply_features(test_sentence1)
test_set2 = sentim_analyzer.apply_features(test_sentence2)
test_set3 = sentim_analyzer.apply_features(test_sentence3)

classifier.classify(test_set[0][0])
classifier.classify(test_set2[0][0])
classifier.classify(test_set3[0][0])


#set up Twitter authentication
key = "wIXZ2Jow7HR2G4dpEYKmQESxr"
secret = "RzVYymL8Wl1l04DHYxZNeBIdefGyyGuL63AVUnQGjSBfgALdKo"
token = "62518878-RgDidPPHtq8IqWiSuWX3phe07Of0dPjuB0vTGs7pN"
token_secret = "ehkDmqMLxlzMmqt3Ny8QL0zgROi5nRDT0Tm7xTFTr9oc4"

#specify the URL and a search term
search_term='Trump'
sample_url = 'https://stream.twitter.com/1.1/statuses/sample.json'
filter_url = 'https://stream.twitter.com/1.1/statuses/filter.json?track='+search_term
#’auth’ represents the authorization that will be passed to Twitter
auth = requests_oauthlib.OAuth1(key, secret, token, token_secret)


# Setup Stream
rdd = ssc.sparkContext.parallelize([0])
stream = ssc.queueStream([], default=rdd)

#define a function that makes a GET request to the Twitter resource and returns a 
#specified number of Tweets (blocksize)
def tfunc(t, rdd):
  return rdd.flatMap(lambda x: stream_twitter_data())

def stream_twitter_data():
  response = requests.get(filter_url, auth=auth, stream=True)
  print(filter_url, response)
  count = 0
  for line in response.iter_lines():
#    print(line)
    try:
      if count > BLOCKSIZE:
        break
      post = json.loads(line.decode('utf-8'))
      contents = [post['text']]
      count += 1
      yield str(contents)
    except:
      result = False

stream = stream.transform(tfunc)

coord_stream = stream.map(lambda line: ast.literal_eval(line))

#classify incoming tweets by applying the features of the model to each tweet
def classify_tweet(tweet):
  sentence = [(tweet, '')]
  test_set = sentim_analyzer.apply_features(sentence)
  print(tweet, classifier.classify(test_set[0][0]))
  return(tweet, classifier.classify(test_set[0][0]))

def get_tweet_text(rdd):
  for line in rdd:
    tweet = line.strip()
    translator = str.maketrans({key: None for key in string.punctuation})
    tweet = tweet.translate(translator)
    tweet = tweet.split(' ')
    tweet_lower = []
    for word in tweet:
      tweet_lower.append(word.lower())
    return(classify_tweet(tweet_lower))

results = []

#save the results of the batch of Tweets along with a timestamp
def output_rdd(rdd):
  global results
  pairs = rdd.map(lambda x: (get_tweet_text(x)[1],1))
  counts = pairs.reduceByKey(add)
  output = []
  for count in counts.collect():
    output.append(count)
  result = [time.strftime("%I:%M:%S"), output]
  results.append(result)
  print(result)

#Call the above functions for each RDD in the stream’s batch
coord_stream.foreachRDD(lambda t, rdd: output_rdd(rdd))

# Start streaming
ssc.start()
ssc.awaitTermination()

cont = True

while cont:
  if len(results) >10:
    cont = False

ssc.stop()

#save results
csvfile = '/user/cloudera/final/r'+time.strftime("%I%M%S")+search_term
results_rdd = sc.parallelize(results)
results_rdd.saveAsTextFile(csvfile)

