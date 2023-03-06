import argparse
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument('--input_green', required=True, help="Input green taxi data")
parser.add_argument('--input_yellow', required=True, help="Input yellow taxi data")
parser.add_argument('--output', required=True, help="Output path")

args = parser.parse_args()
input_green = args.input_green
input_yellow = args.input_yellow
output_path = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df_green = spark.read.parquet(input_green)
df_green.show()

df_yellow = spark.read.parquet(input_yellow)
df_yellow.printSchema()

df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# preserving the order of the columns
common_columns = ['VendorID',
 'pickup_datetime',
 'dropoff_datetime',
 'store_and_fwd_flag',
 'RatecodeID',
 'PULocationID',
 'DOLocationID',
 'passenger_count',
 'trip_distance',
 'fare_amount',
 'extra',
 'mta_tax',
 'tip_amount',
 'tolls_amount',
 'improvement_surcharge',
 'total_amount',
 'payment_type',
 'congestion_surcharge']


df_green_select = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))

df_yellow_select = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_select.unionAll(df_yellow_select)
df_trips_data.groupBy('service_type').count().show()

df_trips_data.registerTempTable('trips_data')

spark.sql("""
    SELECT service_type, COUNT(1) as num_trips
    FROM trips_data 
    GROUP BY service_type
    LIMIT 100
""").show()


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

df_result.coalesce(1).write.parquet(output_path, mode='overwrite')