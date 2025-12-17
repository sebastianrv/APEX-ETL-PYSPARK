from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadProcessedData").getOrCreate()

df = spark.read.parquet("data/processed")

df.printSchema()
df.limit(100).show(truncate=False)

spark.stop()

