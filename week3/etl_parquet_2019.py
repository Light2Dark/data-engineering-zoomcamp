import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta

@flow(name="parent_flow", log_prints=True)
def etl_parent_flow(months: list[int], year: str):
  """Runs the ETL flow for a collection of months"""
  total_rows = 0
  for month in months:
    rows = etl_web_to_gcs(month, year)
    total_rows += rows
  print(f"total rows: {total_rows}")
    
@flow(name="etl_web_to_gcs", log_prints=True)
def etl_web_to_gcs(month: int, year: int) -> int:
  """Main ETL Flow to upload data to GCS bucket. Returns number of rows for dataframe uploaded."""
  filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
  filepath = f"./week3/data/{filename}"
  dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{filename}"
  
  df = extract(dataset_url, filename, filepath)
  (df_transformed, rows) = transform(df)
  write_gcs(df_transformed, filename)
  return rows
  
@task(name="extract_data", retries=3, log_prints=True, tags="extract", cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(url: str, filename: str, filepath: str) -> pd.DataFrame:
  """Download, read and return a dataframe from url. Stores locally at filepath with filename"""
  print(f"reading dataset from {url}")
  
  df = pd.read_csv(url, nrows=10)
  df.head(0).to_parquet(filepath, index=False, engine="fastparquet")
  
  df_iter = pd.read_csv(url, iterator=True, chunksize=100000)
  while (df := next(df_iter, None)) is not None:
    df.to_parquet(filepath, index=False, engine="fastparquet", append=True)
  print(f"locally downloaded {filename} and saved it as parquet")

@task(name="transform_data", log_prints=True, tags="transform")
def transform(df: pd.DataFrame) -> list[pd.DataFrame, int]:
  """Fix dtype issues, return dataframe and number of rows"""
  df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
  df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
  df = df.astype({"PUlocationID": "int", "DOlocationID": "int"})
  
  print(df.head(5))
  print(f"columns: {df.columns}")
  print(f"rows: {len(df)}")
  return df, len(df)
  
@task(name="write_gcs", log_prints=True, tags="load_gcs", retries=3)
def write_gcs(df: pd.DataFrame, filename: str):
  """Write dataframe to GCS bucket"""
  gcp_cloud_storage_bucket_block = GcsBucket.load("taxi-gcp")
  path_uploaded_to = gcp_cloud_storage_bucket_block.upload_from_path(from_path=f"./week3/data/{filename}", to_path=filename)
  print(f"Uploaded to {path_uploaded_to}")
  
if __name__ == "__main__":
  # etl_large("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz")
  etl_parent_flow(months=list(range(1,13)), year="2019")