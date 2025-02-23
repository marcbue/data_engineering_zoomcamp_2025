{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
filtered_trips as (
    select *
    from trips_data
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit card')
),
monthly_percentiles as (
    select 
        service_type,
        pickup_year,
        pickup_month,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] as p97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] as p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] as p90
    from filtered_trips
    where pickup_year = 2020
        and pickup_month = 4
    group by service_type, pickup_year, pickup_month
)


select *
from monthly_percentiles