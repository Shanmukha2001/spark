from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

# Define schema
schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

# Read names file
names = spark.read.schema(schema).option("sep", " ").csv("/home/shanmukha/Desktop/sparkCourse/assets/Marvel-Names")

# Read the graph data (this contains superhero connections)
lines = spark.read.text("/home/shanmukha/Desktop/sparkCourse/assets/Marvel-Graph")

# Processing the connections to count co-appearances
connections_df = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Sort by connections in ascending order (the most obscure superhero will be first)
min_connections_count = connections_df.agg(func.min("connections")).first()[0]
min_connections = connections_df.filter(func.col("connections") == min_connections_count)
min_connections_with_names = min_connections.join(names,"id")
min_connections_with_names.select("name").show()