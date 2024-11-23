from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///home/shanmukha/Desktop/sparkCourse/assets/customer-orders.csv")

def itemsInformation(line):
    fields = line.split(",")
    return fields[0],float(fields[2])

itemInfo = lines.map(itemsInformation)
totAmountPerItem = itemInfo.reduceByKey(lambda x,y:(x+y)).map(lambda x:(x[1],x[0])).sortByKey()
for cost,item in totAmountPerItem.collect():
    print("item:",item,"cost",cost)