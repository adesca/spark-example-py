from pyspark.sql import SparkSession

# creating spark instance
spark = SparkSession.builder.appName("ActivityStream").getOrCreate()
# creating a spark dataframe that shows headers on print
crimesDataFrame = spark.read.csv("data/crimes/crimes.csv", header=True)

crimesDataFrame.show(5)
