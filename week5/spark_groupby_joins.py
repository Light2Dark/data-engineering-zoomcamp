# %%
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('test') \
    .getOrCreate()

# %%
df_green = spark.read.parquet('data/pq/green/*/*')
df_green.registerTempTable('green')

# %%
df_green_revenue = spark.sql("""
SELECT
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")

df_green_revenue \
    .repartition(24) \
    .write.parquet('data/report/revenue/green')

# %%
df_yellow = spark.read.parquet('data/pq/yellow/*/*')
df_yellow.registerTempTable('yellow')

df_yellow_revenue = spark.sql("""
SELECT
    date_trunc('hour', tpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    yellow
WHERE
    tpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
ORDER BY
    1
""")

# %%
df_yellow_revenue \
    .repartition(24) \
    .write.parquet('data/report/revenue/yellow')

# %%
# Joining both tables together & create new cols, outer join because wanna have records for both.

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')

df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

# %%
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
df_join.show()

# %%
df_join.write.parquet('data/report/revenue/total')

# %%
df_zones = spark.read \
    .option('header', 'true') \
    .csv('taxi_zone_lookup.csv')
df_zones.show()

# %%
df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)

df_result.drop('LocationID', 'zone').write.parquet('data/report/revenue/revenue-zones')

# %%



