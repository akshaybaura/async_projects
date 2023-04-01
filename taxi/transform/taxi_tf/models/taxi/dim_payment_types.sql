
{{ config(
    materialized='incremental',
   unique_key='payment_type'
)}}

select 
{{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_sk, 
payment_type as payment_type, 
description,
trip_start_timestamp as last_trip_ts 
from 
    (select payment_type, 
    trip_start_timestamp,
    'dummy description' as description,
    row_number() over (partition by payment_type order by trip_start_timestamp desc) as rn
    from {{source('taxi', 'taxi')}}
        {% if is_incremental() %}
        join (select max(last_trip_ts) as max_last_trip_ts from {{this}}) a on 1=1
        where {{source('taxi', 'taxi')}}.trip_start_timestamp > a.max_last_trip_ts
        {% endif %}
    )a
where rn=1