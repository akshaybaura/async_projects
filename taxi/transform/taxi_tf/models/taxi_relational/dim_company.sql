
{{ config(
    materialized='incremental',
   unique_key='company'
)}}

select 
{{ dbt_utils.generate_surrogate_key(['company']) }} as company_sk, 
company as company, 
description,
trip_start_timestamp as last_trip_ts 
from 
    (select company, 
    'dummy description' as description,
    trip_start_timestamp,
    row_number() over (partition by company order by trip_start_timestamp desc) as rn
    from {{source('trips', 'trips')}}
        {% if is_incremental() %}
        join (select max(last_trip_ts) as max_last_trip_ts from {{this}}) a on 1=1
        where {{source('trips', 'trips')}}.trip_start_timestamp > a.max_last_trip_ts
        {% endif %}
    )a
where rn=1