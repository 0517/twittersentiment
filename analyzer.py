# When running a Spark Streaming program locally, do not use “local” or “local[1]” as the master URL. Either of these means that only one thread will be used for running tasks locally. If you are using an input DStream based on a receiver (e.g. sockets, Kafka, Flume, etc.), then the single thread will be used to run the receiver, leaving no thread for processing the received data. Hence, when running locally, always use “local[n]” as the master URL, where n > number of receivers to run (see Spark Properties for information on how to set the master).
# Extending the logic to running on a cluster, the number of cores allocated to the Spark Streaming application must be more than the number of receivers. Otherwise the system will receive data, but not be able to process it.

import json
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from gensim.parsing.preprocessing import preprocess_string, strip_tags, strip_punctuation, strip_multiple_whitespaces, remove_stopwords, strip_numeric
import re

BATCH_INTERVAL = 10
WINDOWS_LENGTH = 60
SLIDING_INTERVAL = 20


CUSTOM_FILTERS = [lambda x: re.sub(r'^https?:\/\/.*[\r\n]*', '', x, flags=re.MULTILINE).lower(), strip_punctuation, strip_multiple_whitespaces, remove_stopwords, strip_numeric]

def read_as_json(data):
	try:
		json_str = json.loads(data)
	except json.decoder.JSONDecodeError as e:
		return False

	return json_str

def tokenize(data):
	text = data["text"]
	preprocess_string(text, CUSTOM_FILTERS)

# tweet object
# https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json#extendedtweet
# exmaple response
# https://developer.twitter.com/en/docs/tweets/post-and-engage/api-reference/post-statuses-update
# geo detail
# https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/geo-objects

sc = SparkContext(appName="SentimentMap", master="local[4]")
ssc = StreamingContext(sc, BATCH_INTERVAL)
dstream = ssc.socketTextStream("localhost", 9994)
dstream_tweets = dstream.map(lambda data: read_as_json(data)) \
						.filter(lambda data: data) \
						.filter(lambda data: "created_at" in data) \
						.flatMap(lambda data: [(word, 1) for word in data['text'].split()]) \
						.reduceByKey(lambda x, y: x+y).pprint()

ssc.start()
ssc.awaitTermination()
