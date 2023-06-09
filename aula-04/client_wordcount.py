from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import os

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "C:/Spark/spark-3.4.0-bin-hadoop3"


spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()
lines = spark.readStream\
    .format("socket")\
    .option("host", "localhost")\
    .option("port", 9009) \
    .load()

words = lines.select(
    f.explode(
        f.split(lines.value, " ")
    ).alias("word")
)

wordCounts = words.groupBy("word").count()

query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination()