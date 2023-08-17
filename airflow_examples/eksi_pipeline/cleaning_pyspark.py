from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# creating the sparksession, uses 3 cores and 2G of memoery (RAM)
spark = SparkSession.builder \
    .master("local[3]") \
    .appName("eksi") \
    .config("spark.driver.memory", "2G") \
    .getOrCreate()

# creating header schema
schema = StructType([\
    StructField("title", StringType(), True),\
    StructField("date", StringType(), True),\
    StructField("hour", StringType(), True)])

df = spark.read.schema(schema) \
    .option("delimiter", ",") \
    .option("encoding", "UTF-8") \
    .csv("./eksi.csv")

df_with_timestamp = df.withColumn(
    "datetime",
    F.unix_timestamp(F.concat_ws(" ", df["date"], df["hour"]), "dd.MM.yyyy HH:mm").cast("timestamp")
)

df_with_timestamp = df_with_timestamp.groupBy("title").agg(F.min(df_with_timestamp.datetime).alias("mintime"))

df_cleaned = df_with_timestamp.select("title", "mintime").distinct().orderBy(F.col("title").asc())

# you can just show (this is an action, if no action is given spark will not start the DAG)
# df_cleaned.show()

# we should repartition into 1 partition, bc spark can create multiple files
df_cleaned = df_cleaned.repartition(1)

# spark will write to a folder
df_cleaned.write.option("header", True).mode("overwrite").csv("./eksi_cleaned")

spark.stop()