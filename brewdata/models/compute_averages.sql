{{
config 
(
	materialized='table',
	tags="tables",
	post_hook=[index(this,'batch_name'),index(this,'row_number')]
)
}}

SELECT sk.batch_name
, sk.row_number
, avg(sk2.beer_temp) as avg_beer
, avg(sk2.room_temp) as avg_room
FROM {{ref('synth_key')}} sk
	INNER JOIN {{ref('synth_key')}} sk2 ON
	sk2.batch_name = sk.batch_name
	AND sk2.row_number <= sk.row_number
	AND sk2.timestamp + interval '15 minutes' > sk.timestamp
GROUP BY sk.batch_name, sk.row_number
