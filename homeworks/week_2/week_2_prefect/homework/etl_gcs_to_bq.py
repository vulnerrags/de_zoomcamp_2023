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
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/downloaded_data_from_gcs/")

    return Path(f'../data/downloaded_data_from_gcs/{gcs_path}')

@task(name='Writing local DF -> BQ')
def write_bq(df: pd.DataFrame, color: str, year: int, month: int) -> None:
    """
    Write DataFrame to BigQuery
    """

    df.to_gbq(
        destination_table=f"ny_taxi.{color}_trip_data_{year}_{month:02}",
        project_id="zoomcamp-de-2023",
        credentials=GcpCredentials.load("zoomcamo-gcp-creds").get_credentials_from_service_account(),
        chunksize=500000,
        if_exists='append'
    )


@flow(name='MainFlow_GCS_to_BQ', log_prints=True)
def etl_gcs_to_bq(color: str, year: int, months: list[int]) -> None:
    """
    Main ETL flow to load data into Big Query
    """

    cnt = 0

    for each_month in months:

        path = extract_from_gcs(color, year, each_month)
        df = pd.read_parquet(path)

        cnt += df.shape[0]
        print(f"total columns: {df.shape[0]} for dataset: [{color}-{year}-{each_month:02}]")
        write_bq(df, color, year, each_month)

    print(f"total columns for {len(months)} dataset(s): {cnt}")


if __name__ == '__main__':
    etl_gcs_to_bq('yellow', 2019, [2, 3])
