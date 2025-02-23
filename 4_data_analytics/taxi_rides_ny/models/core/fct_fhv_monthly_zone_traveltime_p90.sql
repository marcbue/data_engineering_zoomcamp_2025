{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('dim_fhv_trips') }}
),
trip_durations as (
    select 
        pickup_year,
        pickup_month,
        pulocationid,
        dolocationid,
        pickup_zone_name,
        dropoff_zone_name,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    from trips_data
    -- where pickup_zone_name = 'Newark Airport'
    --     and pickup_year = 2019
    --     and pickup_month = 11
),
duration_percentiles as (
    select 
        pickup_year,
        pickup_month,
        pickup_zone_name,
        dropoff_zone_name,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] as p90
    from trip_durations
    where pickup_zone_name in ('Newark Airport', 'SoHo', 'Yorkville East')
        and pickup_year = 2019
        and pickup_month = 11
    group by pickup_year, pickup_month, pickup_zone_name, dropoff_zone_name
    ORDER BY p90 DESC
)


select *
from duration_percentiles