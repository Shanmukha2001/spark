from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import functions as func


# Create a SparkSession
spark = SparkSession.builder.appName("SparkSql").getOrCreate()

# Read CSV with header and schema inferred
people = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///home/shanmukha/Desktop/sparkCourse/assets/fakefriends-header.csv")

# Group by age and calculate the average number of friends
people.groupBy("age").agg(avg("friends").alias("average_friends")).show()

people.groupBy("age").agg(func.round(avg("friends"),2).alias("average_friends")).show()
spark.stop()