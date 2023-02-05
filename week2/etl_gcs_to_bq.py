from prefect import flow, task
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from datetime import timedelta

@flow(name="main gcs to bq flow", log_prints=True)
def etl_gcs_to_bq():
  """Main flow to upload data from GCS to BigQuery"""
  colour = "yellow"
  month = 7
  year = 2021
  
  path = extract_from_gcs(colour, year, month)
  df = transform_from_gcs(path)
  load_to_bq(df, "taxi_data.yellow")
  
@task(name="extract_from_gcs", retries=3, log_prints=True, tags="extract_gcs", cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(colour: str, year: int, month: int) -> str:
  """Download trip data from GCS bucket to local machine, returns path where file is saved"""
  gcs_path = f"{colour}_tripdata_{year}-{month:02d}.parquet"
  gcs_block: GcsBucket = GcsBucket.load("taxi-gcp")
    
  save_path = f"./data/{gcs_path}"
  gcs_block.get_directory(from_path=gcs_path, local_path=save_path)
  print(f"saved file in {save_path}")
  return save_path

@task(name="transform data", log_prints=True, tags="transform_bq")
def transform_from_gcs(path: str) -> pd.DataFrame:
  """Data cleaning, returns dataframe"""
  df = pd.read_parquet(path)
  print(f"Pre-transform missing passenger count: {df['passenger_count'].isna().sum()}")
  df.dropna(subset=["passenger_count"], inplace=True) # or fillna(0)
  print(f"Post-transform missing passenger count: {df['passenger_count'].isna().sum()}")
  return df

@task(name="load_to_bq", log_prints=True, tags="load_bq")
def load_to_bq(df: pd.DataFrame, to_path_upload: str):
  """Uploads dataframe to BigQuery in to_path_upload"""
  gcp_credentials_block: GcpCredentials = GcpCredentials.load("gcp-credentials")
  df.to_gbq(
    destination_table=to_path_upload,
    project_id="data-eng-first",
    credentials=gcp_credentials_block.get_credentials_from_service_account(),
    chunksize=100_000,
    if_exists="append"
  )

if __name__ == "__main__":
  etl_gcs_to_bq()