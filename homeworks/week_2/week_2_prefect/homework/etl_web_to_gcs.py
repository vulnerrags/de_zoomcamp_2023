import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


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
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> dict:
    """
    Write DataFrame out locally as parquet file
    """

    local_path = Path(f"../data/{color}/{dataset_file}.parquet")
    df.to_parquet(local_path, compression="gzip")

    gcs_path = Path(f"data/{color}/{dataset_file}.parquet")

    return {'local': local_path, 'gcs': gcs_path}


@task(name='writing data to gcs')
def write_gcs(path_local: Path, path_gcs: Path) -> None:
    """
    Upload local parquet file to GCS
    """

    gcs_block = GcsBucket.load("zoomcamp-gcp-bucket")
    gcs_block.upload_from_path(
        from_path=path_local,
        to_path=path_gcs
    )


@flow(name='ETL Web to GCP')
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """
    The main ETL function
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    paths = write_local(df_clean, color, dataset_file)
    write_gcs(path_local=paths['local'], path_gcs=paths['gcs'])


if __name__ == '__main__':

    color = "green"
    year = 2020
    months = [11]

    for each_month in months:
        etl_web_to_gcs(color, year, each_month)
