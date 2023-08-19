from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local[3]") \
    .appName("eksi") \
    .config("spark.driver.memory", "2G") \
    .getOrCreate()

df = spark.read.option("header", True).csv("./eksi_cleaned.csv")



spark.stop()