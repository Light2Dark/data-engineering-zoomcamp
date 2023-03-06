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
# Create an ML Model to predict duration of a trip

df_green = spark.read.parquet('data/pq/green/*/*')
df_green

# %%
columns = ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']

duration_rdd = df_green \
    .select(columns) \
    .rdd

# %%
# model = ...
def model_predict(df):
    y_pred = df.trip_distance * 5
    return y_pred

# %%
import pandas as pd

def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    predictions = model_predict(df)
    df['predicted_duration'] = predictions
    
    for row in df.itertuples():
        yield row

# %%
# duration_rdd.mapPartitions(apply_model_in_batch).collect()

df_predicts = duration_rdd \
    .mapPartitions(apply_model_in_batch) \
    .toDF() \
    .drop('Index')

df_predicts

# %%



