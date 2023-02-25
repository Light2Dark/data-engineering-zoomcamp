{{ config(materialized='table') }}

select
    -- identifiers
    ROW_NUMBER() OVER (ORDER BY dispatching_base_num, pickup_datetime) as tripid,
    dispatching_base_num,
    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,

    -- timestamps
    -- already timestamp
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag as sr_flag,
    Affiliated_base_number
from {{ source("staging", "fhv_2019") }}
where dispatching_base_num is not null

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}