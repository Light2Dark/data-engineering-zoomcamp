# %%
import pandas as pd
import pyspark
from pyspark.sql import SparkSession, types

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('test') \
    .getOrCreate()
    
df = spark.read.parquet('fhvhv/2021/01/')

# %%
df.show()
df.printSchema()

# %%
df.select('hvfhs_license_num', 'pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
    .filter(df.hvfhs_license_num == 'HV0005') 
# lazy loaded aka transformations

# but we can already do these sort of statements in SQL, why use Spark? Because Spark is more flexible. 
# eg: built-in funcs & user-defined funcs

# %%
from pyspark.sql import functions as F

def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'
    
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())

# %%
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('baseId', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('dispatching_base_num','pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()

# %%



