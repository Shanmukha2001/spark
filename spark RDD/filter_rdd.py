from pyspark import SparkConf,SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

def parse(line):
    line = line.split(",")
    farenheit = float(line[3])*0.1*(9.0/5.0)+32.0
    return (line[0],line[2],farenheit)

lines = sc.textFile("file:///home/shanmukha/Desktop/sparkCourse/assets/1800.csv")
parsedLines = lines.map(parse)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x:(x[0],x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y:max(x,y))
results = minTemps.collect()
for res in results:
    print(res)