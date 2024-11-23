from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSql").getOrCreate()
people = spark.read.option("header","true").option("inferSchema","true").csv("file:///home/shanmukha/Desktop/sparkCourse/assets/fakefriends-header.csv")
people.printSchema()
people.select("name").show()
people.filter(people.age < 21).show()
people.select(people.name,people.age+10).show()
spark.stop()