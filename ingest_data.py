import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from time import time
import argparse
# import os

def ingest(user: str, pwd: str, host: str, db: str, table_name: str, port: int, file_path: str):
    # download the csv file
    csv_name: str = file_path
    # os.system(f"wget {url} -O {csv_name}") # not working currently as it is a zipped file

    df = pd.read_csv(csv_name, nrows=100)
    schema = pd.io.sql.get_schema(df, table_name)
    print(schema)

    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")
    engine.connect()
    # print(pd.io.sql.get_schema(df, table_name, con=engine))

    # use batch ingesting using an iterator (load a large csv file in chunks aka batches)
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    # df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    while (df := next(df_iter, None)) is not None:
            time_start = time()
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)     # specify datetime cols to have datetime type
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            time_end = time()
            print(f"inserting a chunk..took {time_end-time_start:.2f}s")

def ingest_exec():
    parser = argparse.ArgumentParser(description='Creates a table in postgresql database and populates it with data from csv file')
    
    parser.add_argument('--user', type=str, help='username for postgresql database')
    parser.add_argument('--pwd', type=str, help='password for postgresql database')
    parser.add_argument('--host', type=str, help='hostname for postgresql database')
    parser.add_argument('--db', type=str, help='database name for postgresql database')
    parser.add_argument('--table_name', type=str, help='table name for postgresql database')
    parser.add_argument('--port', type=int, help='port for postgresql database')
    parser.add_argument('--file_path', type=str, help='file name for taxi data csv file')
    
    args = parser.parse_args()
    ingest(args.user, args.pwd, args.host, args.db, args.table_name, args.port, args.file_path)


def ingest_zones(user: str, pwd: str, host: str, db: str, table_name: str, port: int, url: str = None):
    df = pd.read_csv("taxi_zone_lookup.csv")
    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")
    engine.connect()
    df.to_sql(name=table_name, con=engine, if_exists = 'replace')
    
def connect_to_db(user: str, pwd: str, host: str, db: str, port: int) -> Connection:
    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")
    return engine.connect()
    
def query_db(db_connection: Connection, query: str) -> str:
    cursorResult = db_connection.execute(query)
    result = cursorResult.fetchall()
    print(f"Query result {result}")
    
 

if __name__ == "__main__":    
    connection = connect_to_db("root", "root", "localhost", "ny_taxi", "5432")
    query = "SELECT COUNT(1) FROM yellow_taxi_data"
    query_db(connection, query)