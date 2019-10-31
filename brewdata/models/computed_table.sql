{{ config ( materialized='view', tags="views") }}

SELECT
	ROW_NUMBER()
	OVER
	(
		PARTITION BY fd.batch_name
		ORDER BY fd.timestamp
	) as row_number
	, ca.batch_name
	, fd.timestamp
	, fd.beer_temp
	, fd.fridge_temp
	, fd.room_temp
	, fd.beer_setting
	, fd.fridge_setting
	, CASE fd.actuator_code
		WHEN 0 THEN 'idle'
		WHEN 3 THEN 'heating'
		WHEN 4 THEN 'cooling'
		END as actuator_state
	, ca.avg_beer, ca.avg_room, ca.avg_fridge
	, ABS(fd.beer_temp-fd.beer_setting)/fd.beer_setting as temp_error
FROM {{ref('compute_averages')}} ca
	INNER JOIN brewpi_logs.fermentation_data fd ON fd.batch_name = ca.batch_name AND fd.timestamp = ca.timestamp