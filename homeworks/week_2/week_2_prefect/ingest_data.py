#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd

from datetime import timedelta
from time import time

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url_file: str) -> pd.DataFrame:
    if url_file.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"/Users/feelingalive/opt/anaconda3/bin/wget {url_file} -O ./{csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True, retries=3)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print(f"pre:  missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post:  missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def ingest_data(table_name: str, df: pd.DataFrame) -> None:

    connection_block = SqlAlchemyConnector.load("postgres13")
    with connection_block.get_connection(begin=False) as engine:
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

    """
    while True:
        t_start = time()
        try:
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=f'{table_name}', con=engine, if_exists='append')

            t_end = time()

            print(f'Another chunk inserted...took {round(t_end - t_start, 3)} seconds')
        except StopIteration:
            print(f'Ingesting is finished...took {round(time() - t_start, 3)} seconds')
            break
    """


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name: str) -> None:
    print(f"Logging Subflow for {table_name}")


@flow(name='Ingest flow')
def main_flow(table_name: str, url_file: str) -> None:

    log_subflow(table_name)

    raw_data_df = extract_data(url_file)
    transformed_data = transform_data(raw_data_df)
    ingest_data(table_name=table_name, df=transformed_data)


if __name__ == '__main__':
    main_flow(
        table_name='yellow_taxi_data_2021_01',
        url_file='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    )
