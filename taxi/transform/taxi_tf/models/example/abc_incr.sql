
{{ config(
    materialized='incremental',
   unique_key='id'
)}}

select id, name, dt from 
(select *, row_number() over (partition by id order by dt desc) as rn
from abc
{% if is_incremental() %}
    where dt > (select max(dt) from {{this}})
{% endif %}
)a
where rn=1