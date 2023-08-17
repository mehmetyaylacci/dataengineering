from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local[3]") \
    .appName("eksi") \
    .config("spark.driver.memory", "2G") \
    .getOrCreate()

df = spark.read.option("header", True).csv("./eksi_cleaned.csv")

df = df.withColumn('words', F.split(df['title'], ' '))
# df = df.withColumn('num_words', F.size(df['words']))
# df_grouped = df.groupBy('date').agg(F.sum('num_words').alias('total words'))
# df_grouped.show()

# Explode the 'words' column into separate rows
df_exploded = df.select('date', F.explode(df.words).alias('word'))

# Group by 'word' and count the number of occurrences
df_word_count = df_exploded.groupBy('word').count().orderBy(F.col('count').desc())

df_word_count.show()

input()

spark.stop()