{{
config 
(
	materialized='table',
	tags="tables",
	post_hook=[index(this,'batch_name'),index(this,'timestamp')]
)
}}

-- the query which averages/smooths the data for each batch
SELECT
	  fd.timestamp
	, fd.batch_name
	, fd.beer_temp
	, avg(fd2.beer_temp) as avg_beer
	, avg(fd2.fridge_temp) as avg_fridge
	, avg(fd2.room_temp) as avg_room
FROM brewpi_logs.fermentation_data fd	INNER JOIN brewpi_logs.fermentation_data fd2
			ON  fd2.batch_name = fd.batch_name 
			AND fd2.timestamp <= fd.timestamp
    	AND fd2.timestamp + interval '30 minutes' > fd.timestamp
GROUP BY fd.timestamp, fd.batch_name, fd.beer_temp