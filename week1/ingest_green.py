import pandas as pd
from sqlalchemy import create_engine

def ingest_green_taxi():
    engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")
    df_iter = pd.read_csv("../files/green_tripdata_2019-01.csv", iterator=True, chunksize=100000)
    
    while (df := next(df_iter, None)) is not None:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name="green_taxi", con=engine, if_exists="append")
        print("inserting a chunk..")
    
    print("done.")       