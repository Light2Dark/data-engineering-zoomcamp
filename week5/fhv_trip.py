import pyspark
from pyspark.sql import SparkSession

# wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option('header', 'true') \
    .csv("fhvhv_tripdata_2021-01.csv.gz")