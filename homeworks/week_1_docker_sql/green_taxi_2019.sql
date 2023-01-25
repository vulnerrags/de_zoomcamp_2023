-- number 3
select 
	count(1)
from green_taxi_data_2019_01_01
where 1=1
	and lpep_pickup_datetime::date = '2019-01-15' 
	and lpep_dropoff_datetime::date ='2019-01-15';

-- number 4
select 
	lpep_pickup_datetime::date
	, MAX(trip_distance) as distance
from green_taxi_data_2019_01_01
group by lpep_pickup_datetime::date
order by distance desc
limit 1;

-- number 5
select 
	lpep_pickup_datetime::date
	, passenger_count
	, count(1)
from green_taxi_data_2019_01_01
where 1=1
	and passenger_count in (2, 3)
	and lpep_pickup_datetime::date = '2019-01-01'
group by lpep_pickup_datetime::date, passenger_count;

-- number 6
select
	zone_DO
	, max(tip_amount) as tip
from
	(
		select 
			t1.*
			, t2."Zone" as zone_PU
			, t3."Zone" as zone_DO
		from green_taxi_data_2019_01_01 as t1
		left join taxi_zone_lookup as t2
			on t1."PULocationID" = t2."LocationID"
		left join taxi_zone_lookup as t3
			on t1."DOLocationID" = t3."LocationID"
	) as t1
where 1=1
	and zone_PU = 'Astoria'
group by zone_DO
order by tip desc
limit 1;