{{ config ( materialized='view', tags="views") }}

-- query to mimic the Excel table, materialize as a view
SELECT
	ca.batch_name
	, ca.row_number
	, fd.timestamp
	, fd.beer_temp
	, fd.fridge_temp
	, fd.room_temp
	, fd.beer_setting
	, CASE fd.actuator_code
		WHEN 0 THEN 'idle'
		WHEN 3 THEN 'heating'
		WHEN 4 THEN 'cooling'
	END as actuator_state
	, ca.avg_beer
	, ca.avg_room
	, ABS(fd.beer_temp - fd.beer_setting) / fd.beer_setting as temp_error
FROM {{ref('compute_averages')}} ca
	INNER JOIN {{ref('synth_key')}} sk ON sk.batch_name = ca.batch_name AND sk.row_number = ca.row_number
	INNER JOIN brewpi_logs.fermentation_data fd ON fd.batch_name = sk.batch_name AND fd.timestamp = sk.timestamp

