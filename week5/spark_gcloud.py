# %%
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = '/home/sham/.config/google_creds_de_first.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.11.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

# %%
sc.stop()
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

# %%
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

# %%
df_green = spark.read.parquet('gs://taxi-data-dataeng/spark/pq/green/*/*')
df_green.show()

# df_yellow = spark.read.parquet('data/pq/yellow/*/*')
# df_yellow.printSchema()

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
set(df_green.columns) & set(df_yellow.columns) # check if all columns are present

# %%
# preserving the order of the columns
common_columns = []
yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)

# %%
df_green_select = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_select = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

# %%
df_trips_data = df_green_select.unionAll(df_yellow_select)
df_trips_data.groupBy('service_type').count().show()

# %%
# To run SQL queries, need to register df as tempTable
df_trips_data.registerTempTable('trips_data')

spark.sql("""
    SELECT service_type, COUNT(1) as num_trips
    FROM trips_data 
    GROUP BY service_type
    LIMIT 100
""").show()

# %%
df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# %%
df_result.coalesce(1).write.parquet('data/report/revenue/', mode='overwrite')

# %%



