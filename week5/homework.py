import pyspark
from pyspark.sql import SparkSession

# wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz
# wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option('header', 'true') \
    .csv("fhvhv/fhvhv_tripdata_2021-06.csv.gz")

df.show()

df \
    .repartition(12) \
    .write.parquet('fhvhv/2021/06')

import pandas as pd
from pyspark.sql import SparkSession, types

# df_subset = df.limit(100)
# df_subset.write.option("header", "true") \
#    .csv("head_fhvhv")

df_pandas = pd.read_csv("head_fhvhv.csv", nrows=100)
df_pandas.info()

df = (spark.read.format("csv").options(header="true")
    .load("head_fhvhv.csv"))


fhvhv_schema = types.StructType(
  [
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropoff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
  ]
)

df_fhvhv = spark.read \
    .option("header", 'true') \
    .schema(fhvhv_schema) \
    .csv('fhvhv/fhvhv_tripdata_2021-06.csv.gz')

from pyspark.sql import functions as F

df_fhvhv.select() \
    .filter(F.to_date(df_fhvhv.pickup_datetime) == '2021-06-15') \
    .count()

df_fhvhv.registerTempTable('trips_fhvhv')

spark.sql("""
SELECT 
    MAX(timestampdiff(minute, pickup_datetime, dropoff_datetime)) as timeDiffMinutes
FROM trips_fhvhv
LIMIT 100
""").show()

df_zones = spark.read \
    .option('header', 'true') \
    .csv('taxi_zone_lookup.csv')

df_zones.show()

df_res = df_fhvhv.join(df_zones, df_zones.LocationID == df_fhvhv.PULocationID)
df_res = df_res.drop('dispatching_base_num', 'dropoff_datetime', 'pickup_datetime', 'SR_Flag', 'Affiliated_base_num')

df_res.createOrReplaceTempView('df_result')

spark.sql("""
WITH num_zones AS (
    SELECT
        LocationID,
        Borough, 
        Zone,
        COUNT(1) as num_zones
    FROM df_result
    GROUP BY LocationID, Borough, Zone
)
SELECT LocationID, Borough, Zone, MAX(num_zones) as max_zone
FROM num_zones
GROUP BY LocationID, Borough, Zone
""").show()