from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadProcessedData").getOrCreate()

df = spark.read.parquet("data/processed")

df.printSchema()
df.limit(10).show(truncate=False)

spark.stop()

