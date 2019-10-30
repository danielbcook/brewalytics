{{ 
	config(
	materialized='table',
	tags="tables",
	post_hook=[index(this,'batch_name'),index(this,'row_number'),index(this,'timestamp')]
)

}}

SELECT 
	ROW_NUMBER() OVER ( PARTITION BY batch_name ORDER BY timestamp ) as row_number
	, timestamp
	, batch_name
	, beer_temp
	, room_temp
FROM brewpi_logs.fermentation_data
