from pyspark import SparkConf, SparkContext
import collections

def parseFile(line):
    fields = line.split(",")
    age, numFriends = int(fields[2]), int(fields[3])
    return (age, numFriends)
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)
lines = sc.textFile("file:///home/shanmukha/Desktop/sparkCourse/assets/fakefriends.csv")
rdd = lines.map(parseFile)
age_tot_friends = rdd.mapValues(lambda numFriends: (numFriends, 1))
aggregated = age_tot_friends.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
age_avg_friends = aggregated.mapValues(lambda x: x[0] // x[1])
res = age_avg_friends.collect()
for item in res:
    print(item)
