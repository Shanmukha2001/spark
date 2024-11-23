from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("customer-order").getOrCreate()
schema = StructType([ \
                     StructField("cust_id", FloatType(), True), \
                     StructField("unwanted", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

inputDF = spark.read.schema(schema).csv("file:///home/shanmukha/Desktop/sparkCourse/assets/customer-orders.csv")
df = inputDF.select(inputDF.cust_id,inputDF.amount)
cust_tot_amount = df.groupBy("cust_id").agg(F.round(F.sum("amount"),2).alias("total_spent"))
cust_tot_amount= cust_tot_amount.sort("total_spent")
cust_tot_amount.show(cust_tot_amount.count()).show()
spark.stop()