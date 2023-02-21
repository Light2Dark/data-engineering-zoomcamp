import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta

@flow(name="parent_flow", log_prints=True)
def etl_parent_flow(months: list[int] = [6,7], colours: list[str] = ["yellow"], years: list[int] = [2021]):
  """Will run the main flow multiple times to collect data from diff months, colours, years etc."""
  for month in months:
    etl_web_to_gcs(month, colours[0], years[0])

@flow(name="etl_web_to_gcs", log_prints=True)
def etl_web_to_gcs(month: int, colour: str, year: int):
  """Main flow integrated with Google Cloud Storage"""
  filepath = f"{colour}_tripdata_{year}-{month:02d}"
  dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{filepath}.csv.gz"
  
  df = extract(dataset_url)
  df_transformed = transform(df, colour)
  write_local(df_transformed, filepath)
  write_gcs(filepath)
  
# cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1) for some reason does not work w Docker
@task(name="extract_data", retries=3, log_prints=True, tags="extract")
def extract(url: str) -> pd.DataFrame:
  """Download, read and return a dataframe from url which is a csv file"""
  # assert url == "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-07.csv.gz", "wrong url"
  print(f"reading dataset from {url}")
  return pd.read_csv(url)

@task(name="transform_data", log_prints=True, tags="transform")
def transform(df: pd.DataFrame, colour: str) -> pd.DataFrame:
  """Fix dtype issues, return dataframe"""
  
  if colour == "green":
    datetime_cols = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
  elif colour == "yellow":
    datetime_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    
  for col in datetime_cols:
    df[col] = pd.to_datetime(df[col])
  
  print(df.head(5))
  print(f"columns: {df.columns}")
  print(f"rows: {len(df)}")
  return df

@task(name="write_local", log_prints=True, tags="load_local")
def write_local(df: pd.DataFrame, filepath: str):
  """Write dataframe out locally as Parquet file"""
  print(f"saving to local file: {filepath}")
  df.to_parquet(f"./week2/data_github/{filepath}.parquet", index=False)  # change to ./week2/data_github
  
# cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
@task(name="write_gcs", log_prints=True, tags="load_gcs", retries=3)
def write_gcs(filename: str, filepath: str):
  """Uploads file from filepath to filename in GCS"""
  gcp_cloud_storage_bucket_block = GcsBucket.load("taxi-gcp")
  path_uploaded_to = gcp_cloud_storage_bucket_block.upload_from_path(from_path=filepath, to_path=filename) # from_path=./week2/data_github/{filepath}.parquet
  print(f"Uploaded to {path_uploaded_to} in GCS")
  
  
  
@flow(name="etl_parent_flow_large", log_prints=True)
def etl_parent_flow_large(colour: str, year: int, months: list[int]):
  for month in months:
    etl_large(colour, year, month)
  
@flow(name="etl_large", log_prints=True)
def etl_large(colour: str, year: int, month: int):
  filename = f"{colour}_tripdata_{year}-{month:02d}"
  url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{colour}/{filename}.csv.gz"
  filepath = f"./week2/data/{filename}.parquet"
  
  df = pd.read_csv(url, nrows=10)
  df.head(0).to_parquet(filepath, index=False, engine="fastparquet")
  
  df_iter = pd.read_csv(url, iterator=True, chunksize=100000)
  while (df := next(df_iter, None)) is not None:
    df.to_parquet(filepath, index=False, engine="fastparquet", append=True)
  print(f"locally downloaded {filename} and saved it as parquet")
  
  write_gcs(f"{colour}/{filename}.parquet", filepath)
  
if __name__ == "__main__":
  # etl_parent_flow(months=[4], colours=["green"], years=[2019])
  etl_parent_flow_large(colour="yellow", year="2019", months=list(range(1,13)))
  etl_parent_flow_large(colour="yellow", year="2020", months=list(range(1,13)))
  etl_parent_flow_large(colour="green", year="2019", months=list(range(1,13)))
  etl_parent_flow_large(colour="green", year="2020", months=list(range(1,13)))
  