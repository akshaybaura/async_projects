
{{ config(
    materialized='incremental',
   unique_key='community_area_id'
)}}

select 
{{ dbt_utils.generate_surrogate_key(['community_area_id']) }} as community_area_sk, 
community_area_id,
community_name,
trip_start_timestamp as last_trip_ts 
from 
    (select *,
    row_number() over (partition by community_area_id order by trip_start_timestamp desc) as rn
    from
    (select pickup_community_area as community_area_id, trip_start_timestamp, 'dummy community name' community_name
    from {{source('trips', 'trips')}}
    union
    select dropoff_community_area as community_area_id, trip_start_timestamp, 'dummy community name' community_name
    from {{source('trips', 'trips')}}) community_area
        {% if is_incremental() %}
        join (select max(last_trip_ts) as max_last_trip_ts from {{this}}) a on 1=1
        where community_area.trip_start_timestamp > a.max_last_trip_ts
        {% endif %}
    )a
where rn=1