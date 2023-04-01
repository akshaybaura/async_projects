
{{ config(
    materialized='incremental',
   unique_key='taxi_id'
)}}

select 
{{ dbt_utils.generate_surrogate_key(['taxi_id']) }} as taxi_sk, 
taxi_id, 
taxi_name, 
driver_name, 
trip_start_timestamp as last_trip_ts 
from 
    (select taxi_id, 
    trip_start_timestamp, 
    'dummy_taxi_name' as taxi_name, 
    'dummy_driver_name' as driver_name,
    row_number() over (partition by taxi_id order by trip_start_timestamp desc) as rn
    from {{source('taxi', 'taxi')}}
        {% if is_incremental() %}
        join (select max(last_trip_ts) as max_last_trip_ts from {{this}}) a on 1=1
        where {{source('taxi', 'taxi')}}.trip_start_timestamp > a.max_last_trip_ts
        {% endif %}
    )a
where rn=1