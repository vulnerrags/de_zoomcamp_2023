#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(user: str, password: str, host: str, port: int, db_name: str, table_name: str, url_file: str):

    if url_file.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"/Users/feelingalive/opt/anaconda3/bin/wget {url_file} -O ./{csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

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


if __name__ == '__main__':

    user = 'root'
    password = 'root'
    host = 'localhost'
    port = 5432
    db_name = 'ny_taxi'
    table_name = 'yellow_taxi_data_2021_01'
    url_file = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'

    main(user, password, host, port, db_name, table_name, url_file)




