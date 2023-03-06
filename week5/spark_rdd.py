# %%
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('test') \
    .getOrCreate()

query="""
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
"""

# %%
df_green = spark.read.parquet('data/pq/green/*/*')

# %%
df_green.rdd.take(5)

# %%
rdd = df_green \
    .select('lpep_pickup_datetime', 'PULocationID', 'total_amount') \
    .rdd
rdd.take(5)

# %%
rows = rdd.take(5)
row = rows[0]

# we need key & value to do the group by. Cause Spark will group by using the keys and putting same keys in same partition.
def prep_for_grouping(row):
    hour = row.lpep_pickup_datetime.replace(minute=0,second=0,microsecond=0)
    zone = row.PULocationID
    key = (hour, zone)
    
    amount = row.total_amount
    count = 1
    value = (amount, count)

    return (key, value)

def calculate_revenue_reduce(left_val, right_val):
    left_amount, left_count = left_val
    right_amount, right_count = right_val
    
    sum_amount = left_amount + right_amount
    sum_count = left_count + right_count
    return (sum_amount, sum_count)

# %%
from collections import namedtuple

revenue_row = namedtuple('revenue_row', ['hour', 'zone', 'revenue', 'count'])

def unwrap(row):
    return revenue_row(
        hour=row[0][0], 
        zone=row[0][1], 
        revenue=row[1][0], 
        count=row[1][1]
    )

# %%
from pyspark.sql import types

result_schema = types.StructType(
  [
    types.StructField('hour', types.TimestampType(), True), 
    types.StructField('zone', types.IntegerType(), True), 
    types.StructField('revenue', types.DoubleType(), True), 
    types.StructField('count', types.IntegerType(), True)
  ]
)

# %%
from datetime import datetime

start = datetime(year=2020, month=1, day=1)

df_result = rdd \
    .filter(lambda row: row.lpep_pickup_datetime >= start) \
    .map(prep_for_grouping) \
    .reduceByKey(calculate_revenue_reduce) \
    .map(unwrap) \
    .toDF(result_schema)

# %%
df_result.show()
df_result.write.parquet('data/report/revenue/rdd-green/')

# %%



