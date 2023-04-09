
{{ config(
    materialized='incremental',
   unique_key='census_tract_id'
)}}

select 
{{ dbt_utils.generate_surrogate_key(['census_tract_id']) }} as census_tract_sk, 
census_tract_id,
tract_name,
county,
state, 
trip_start_timestamp as last_trip_ts 
from 
    (select *,
    row_number() over (partition by census_tract_id order by trip_start_timestamp desc) as rn
    from
    (select pickup_census_tract as census_tract_id, trip_start_timestamp, 'dummy tract name' tract_name,'dummy county' county, 'dummy state' state
    from {{source('trips', 'trips')}}
    union
    select dropoff_census_tract as census_tract_id, trip_start_timestamp, 'dummy tract name' tract_name,'dummy county' county, 'dummy state' state
    from {{source('trips', 'trips')}}) census_tract
        {% if is_incremental() %}
        join (select max(last_trip_ts) as max_last_trip_ts from {{this}}) a on 1=1
        where census_tract.trip_start_timestamp > a.max_last_trip_ts
        {% endif %}
    )a
where rn=1