from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///home/shanmukha/Desktop/sparkCourse/assets/Book")

def normalize_text(line):
    return re.compile(r"\W+",re.UNICODE).split(line.lower())

# Split lines into words
words = lines.flatMap(normalize_text)
wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
sortedWordCounts = wordCounts.map(lambda x:(x[1],x[0])).sortByKey()
# Display results
for count,word in sortedWordCounts.collect():
    cleanWord = word.encode('ascii', 'ignore').decode("ascii")
    if cleanWord:
        print(cleanWord, count)
