CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_zoomcamp-de-2023/data/fhv_tripdata/fhv_tripdata_2019-*.csv.gz']
);

--
CREATE OR REPLACE TABLE `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_non_partitioned` AS
SELECT * FROM `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_external`;
--

--Q1
SELECT COUNT(1) FROM `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_non_partitioned`) AS t1;


--Q2
SELECT COUNT(1)
FROM (SELECT DISTINCT affiliated_base_number FROM `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_non_partitioned`) AS t1;


--Q3
SELECT COUNT(1) FROM
(SELECT * FROM `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_non_partitioned`
WHERE PUlocationID IS NULL OR DOlocationID IS NULL);


--Q4
CREATE OR REPLACE TABLE `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_external`

--Q5
SELECT DISTINCT affiliated_base_number
FROM `zoomcamp-de-2023.ny_taxi.fhv_tripdata_2019_non_partitioned`
WHERE 1=1
  AND DATE(pickup_datetime) >= '2019-03-01'
  AND DATE(pickup_datetime) <= '2019-03-31';