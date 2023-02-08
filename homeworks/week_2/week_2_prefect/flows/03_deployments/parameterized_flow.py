from datetime import timedelta
from pathlib import Path

import pandas as pd

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(name='fetching data', retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Read data from web into pandas DataFrame
    """

    df = pd.read_csv(dataset_url)
    return df


@task(name='cleaning data', log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix dtype issues
    """

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"total columns: {df.shape[0]}")
    return df


@task(name='writing data locally')
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Write DataFrame out locally as parquet file
    """

    path = Path(f"../../data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(name='writing data to gcs')
def write_gcs(path: Path) -> None:
    """
    Upload local parquet file to GCS
    """

    gcs_block = GcsBucket.load("zoomcamp-gcp-bucket")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )


@flow(name='SubFlow_ETL_Web_to_GCP')
def etl_web_to_gcs(month: int, year: int, color: str) -> None:
    """
    The main ETL function
    """

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow(name='MainFlow_ETL_parent_flow')
def etl_parent_flow(months: list[int], year: int = 2021, color: str = 'yellow'):
    for each_month in months:
        etl_web_to_gcs(each_month, year, color)


if __name__ == '__main__':
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)
