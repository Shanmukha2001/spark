from pyspark.sql import SparkSession
from pyspark.sql import Row

def mapper(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]),name = str(fields[1].encode('utf-8')),age=int(fields[2]),numFriends=int(fields[3]))

spark = SparkSession.builder.appName("SparkSql").getOrCreate()
lines = spark.sparkContext.textFile("file:///home/shanmukha/Desktop/sparkCourse/assets/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age>=13 AND age<=19")
for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()
spark.stop()