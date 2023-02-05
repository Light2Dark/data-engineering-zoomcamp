import pandas as pd
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import os
from prefect_sqlalchemy import SqlAlchemyConnector
# with SqlAlchemyConnector.load("postgres-connector") as database_block:


@task(log_prints=True, retries=3, tags=["extract, download"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(url: str) -> pd.DataFrame:
    if url.endswith(".gz"):
        csv_name = "files/yellow_taxi_data.csv.gz"
    else:
        csv_name = "files/yellow_taxi_data.csv"
    os.system(f"wget {url} -O {csv_name}")
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    print(f"pre: missing passenger count: {df.passenger_count.isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df.passenger_count.isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3)
def load(user: str, pwd: str, host: str, db: str, table_name: str, port: int, df: pd.DataFrame):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:    
        time_start = time()
        df.to_sql(name=table_name, con=engine, if_exists='replace')
        time_end = time()
        print(f"finished inserting {table_name} data in {time_end-time_start:.2f}s")
    
@flow(name="Subflow for logging", log_prints=True)
def log_subflow(table_name: str):
    print(f"logging for subflow {table_name}")
 
@flow(name="ingest_data", log_prints=True)
def main_flow():
    """Prefect flow for ingesting data"""
    user = "root"
    pwd = "root"
    host = "localhost"
    db_name = "ny_taxi"
    table_name = "yellow_taxi_data"
    port = "5432"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-07.csv.gz"
    csv_file_path = "files/yellow_tripdata_2021-07.csv"
    
    raw_data = extract(url)
    transformed_data = transform(raw_data)
    load(user, pwd, host, db_name, table_name, port, transformed_data)
    
    log_subflow(table_name)

 
if __name__ == "__main__":    
    main_flow()