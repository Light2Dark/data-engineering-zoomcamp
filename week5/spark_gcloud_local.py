# %%
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types

spark = SparkSession.builder \
    .master('spark://data-eng-vm.asia-southeast1-a.c.data-eng-first.internal:7077') \
    .appName('test') \
    .getOrCreate()

# %%
df_green = spark.read.parquet('data/pq/green/*/*')
df_green.show()

df_yellow = spark.read.parquet('data/pq/yellow/*/*')
df_yellow.printSchema()

# %%
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

df_green.columns
df_yellow.columns

# %%
# preserving the order of the columns
common_columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


