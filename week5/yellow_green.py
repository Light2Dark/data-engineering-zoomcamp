# %%
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('test') \
    .getOrCreate()
    
df = spark.read \
    .option("header", 'true') \
    .csv('data/raw/green/2020/green_tripdata_2020-01.csv.gz')

# %%
df.printSchema()

# %%
df_pandas = pd.read_csv("data/raw/green/2020/green_tripdata_2020-01.csv.gz", nrows=1000)
df_pandas.dtypes

# %%
spark.createDataFrame(df_pandas).schema

# %%
from pyspark.sql import types

green_schema = types.StructType(
  [
    types.StructField('VendorID', types.IntegerType(), True), 
    types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('RatecodeID', types.IntegerType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('passenger_count', types.IntegerType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', types.DoubleType(), True), 
    types.StructField('ehail_fee', types.DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', types.DoubleType(), True), 
    types.StructField('payment_type', types.IntegerType(), True), 
    types.StructField('trip_type', types.IntegerType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True)
  ]
)

df_green = spark.read \
    .option("header", 'true') \
    .schema(green_schema) \
    .csv('data/raw/green/2020/green_tripdata_2020-01.csv.gz')

# %%
df_green.printSchema()

# %%
df_yel = pd.read_csv("data/raw/yellow/2020/yellow_tripdata_2020-02.csv.gz", nrows=1000)
# df_yel.head()
df_yel.dtypes

# %%
spark.createDataFrame(df_yel).schema

# %%
yellow_schema = types.StructType(
  [
    types.StructField('VendorID', types.IntegerType(), True), 
    types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
    types.StructField('passenger_count', types.IntegerType(), True), 
    types.StructField('trip_distance', types.DoubleType(), True), 
    types.StructField('RatecodeID', types.IntegerType(), True), 
    types.StructField('store_and_fwd_flag', types.StringType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('payment_type', types.IntegerType(), True), 
    types.StructField('fare_amount', types.DoubleType(), True), 
    types.StructField('extra', types.DoubleType(), True), 
    types.StructField('mta_tax', types.DoubleType(), True), 
    types.StructField('tip_amount', types.DoubleType(), True), 
    types.StructField('tolls_amount', types.DoubleType(), True), 
    types.StructField('improvement_surcharge', types.DoubleType(), True), 
    types.StructField('total_amount', types.DoubleType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True)
  ]
)

df_yellow = spark.read \
    .option("header", 'true') \
    .schema(yellow_schema) \
    .csv('data/raw/yellow/2020/yellow_tripdata_2020-02.csv.gz')

df_yellow.printSchema()

# %%
# Writing files to parquet with repartitioning
year = 2020
month = 1
colour = "green"

def read_write_to_parquet(colour: str, year: int, schema):
    for month in range(1,13):
        input_filename = f'{colour}_tripdata_{year}-{month:02d}.csv.gz'
        input_path = f'data/raw/{colour}/{year}/{input_filename}'
        output_path = f'data/pq/{colour}/{year}/{month:02d}/'

        try:
            print(f'processing data for {input_path}')
            df = spark.read \
                .option("header", 'true') \
                .schema(schema) \
                .csv(input_path)

            print(f'writing to parquet in {output_path}')
            df \
                .repartition(4) \
                .write.parquet(output_path)
        except Exception as e:
            print(e)

# %%
# read_write_to_parquet('green', '2020', green_schema)
read_write_to_parquet("green", '2021', green_schema)
read_write_to_parquet("yellow", '2020', yellow_schema)
read_write_to_parquet("yellow", '2021', yellow_schema)

# %%



