{{ config(materialized='table') }}

with stg_fhv_tripdata as (
    select *,
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    stg_fhv_tripdata.tripid,
    stg_fhv_tripdata.pickup_locationid,
    stg_fhv_tripdata.dropoff_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  

    stg_fhv_tripdata.pickup_datetime,
    stg_fhv_tripdata.dropoff_datetime,
    stg_fhv_tripdata.sr_flag,
    stg_fhv_tripdata.Affiliated_base_number

from stg_fhv_tripdata
inner join dim_zones as pickup_zone
on pickup_zone.locationid = stg_fhv_tripdata.pickup_locationid
inner join dim_zones as dropoff_zone
on dropoff_zone.locationid = stg_fhv_tripdata.dropoff_locationid 