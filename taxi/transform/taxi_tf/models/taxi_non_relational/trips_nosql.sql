
{{ config(
    materialized='incremental',
   unique_key='trip_id'
)}}

select trip_id, 
    trip_start_timestamp, 
    to_jsonb(t) as trip_record
    from {{source('trips', 'trips')}} t
        {% if is_incremental() %}
        join (select max(trip_start_timestamp) as max_last_trip_ts from {{this}}) a on 1=1
        where t.trip_start_timestamp > a.max_last_trip_ts
        {% endif %}