
{{ config(
    materialized='incremental',
)}}

select txn.trip_id, 
    txn.trip_start_timestamp,
    txn.trip_end_timestamp,
    txn.trip_seconds,
    txn.trip_miles,
    txn.fairs,
    txn.tips,
    txn.tolls,
    txn.extras,
    txn.trip_total,
    txn.pickup_centroid_latitude,
    txn.pickup_centroid_longitude,
    txn.pickup_centroid_location,
    txn.dropoff_centroid_latitude,
    txn.dropoff_centroid_longitude,
    txn.dropoff_centroid_location,
    d_taxi.taxi_sk,
    d_pickup_tracts.census_tract_sk as pickup_census_tract_sk,
    d_dropoff_tracts.census_tract_sk as dropoff_census_tract_sk,
    d_pickup_community.community_area_sk as pickup_community_area_sk,
    d_dropoff_community.community_area_sk as dropoff_community_area_sk,
    d_company.company_sk,
    d_paytypes.payment_type_sk,
    current_timestamp as _insert_ts
    from {{source('trips', 'trips')}} txn
    left join {{ ref('dim_taxi')}} d_taxi
    on txn.taxi_id = d_taxi.taxi_id
    left join {{ ref('dim_census_tracts')}} d_pickup_tracts
    on txn.pickup_census_tract = d_pickup_tracts.census_tract_id
    left join {{ ref('dim_census_tracts')}} d_dropoff_tracts
    on txn.dropoff_census_tract = d_dropoff_tracts.census_tract_id
    left join {{ ref('dim_community_area')}} d_pickup_community
    on txn.pickup_community_area = d_pickup_community.community_area_id
    left join {{ ref('dim_community_area')}} d_dropoff_community
    on txn.dropoff_community_area = d_dropoff_community.community_area_id
    left join {{ ref('dim_company')}} d_company
    on txn.company = d_company.company
    left join {{ ref('dim_payment_types')}} d_paytypes
    on txn.payment_type = d_paytypes.payment_type
        {% if is_incremental() %}
        join (select max(trip_start_timestamp) as max_trip_start_timestamp from {{this}}) a on 1=1
        where txn.trip_start_timestamp > a.max_trip_start_timestamp
        {% endif %}
