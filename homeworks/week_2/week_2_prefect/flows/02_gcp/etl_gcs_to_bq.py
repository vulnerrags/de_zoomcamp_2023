import pandas as pd

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(name='Extracting data', retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """
    Download trip data from Google Cloud Storage (GCS)
    """

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoomcamp-gcp-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../../data/downloaded_data_from_gcs/")

    return Path(f'../../data/downloaded_data_from_gcs/{gcs_path}')


@task(name='Transforming data')
def transform(path: Path) -> pd.DataFrame:
    """
    Data cleaning example
    """

    df = pd.read_parquet(path)
    print(f"pre: missing passenger count = {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count = {df['passenger_count'].isna().sum()}")

    return df


@task(name='Writing local DF -> BQ')
def write_bq(df: pd.DataFrame) -> None:
    """
    Write DataFrame to BigQuery
    """

    df.to_gbq(
        destination_table="ny_taxi.yellow_trip_data_2021_01",
        project_id="zoomcamp-de-2023",
        credentials=GcpCredentials.load("zoomcamo-gcp-creds").get_credentials_from_service_account(),
        chunksize=500000,
        if_exists='append'
    )


@flow(name='ETL GCS to BQ')
def etl_gcs_to_bq():
    """
    Main ETL flow to load data into Big Query
    """

    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    etl_gcs_to_bq()
