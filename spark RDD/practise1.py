from pyspark import SparkConf,SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///home/shanmukha/Desktop/sparkCourse/ml-100k/u.data")
# print(type(lines))# RDD object
ratings = lines.map(lambda x:x.split()[2])
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for k, v in sortedResults.items():
    print(k,v)