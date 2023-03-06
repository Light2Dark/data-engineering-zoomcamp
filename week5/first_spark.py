# %%
import pyspark
from pyspark.sql import SparkSession

# %%
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# %%
df = spark.read \
    .option('header', 'true') \
    .csv("fhvhv_tripdata_2021-01.csv.gz")

# %%
df.show()
# df.head(5)
df.schema

# %%
# !head -n 101 fhvhv_tripdata_2021-01.csv.gz > head.csv
df_subset = df.limit(100)
df_subset.write.option("header", "true") \
   .csv("head.csv")

# %%
!head -n 10 head.csv
!wc -l head.csv

# %%
import pandas as pd
df_pandas = pd.read_csv("head.csv")
# df_pandas.head(5)

df_pandas.info()

# %%
spark.createDataFrame(df_pandas).schema

# %%
from pyspark.sql import types

schema = types.StructType(
  [types.StructField('hvfhs_license_num', types.StringType(), True), 
  types.StructField('dispatching_base_num', types.StringType(), True), 
  types.StructField('pickup_datetime', types.TimestampType(), True), 
  types.StructField('dropoff_datetime', types.TimestampType(), True), 
  types.StructField('PULocationID', types.IntegerType(), True), 
  types.StructField('DOLocationID', types.IntegerType(), True),
  types.StructField('SR_Flag', types.StringType(), True)]
)

df = spark.read \
    .option('header', 'true') \
    .schema(schema) \
    .csv("fhvhv_tripdata_2021-01.csv.gz")

# %%
df.head(10)
# df.schema

# %%
df = df.repartition(24) # repartitioning so that more executors in Spark custer can take up processing each partition.
df.write.parquet('fhvhv/2021/01/')

# %%


