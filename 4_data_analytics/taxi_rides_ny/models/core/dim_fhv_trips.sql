{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime,
    EXTRACT(YEAR    FROM fhv_tripdata.pickup_datetime) AS pickup_year,
    EXTRACT(QUARTER FROM fhv_tripdata.pickup_datetime) AS pickup_quarter,
    CONCAT(
      CAST(EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS STRING),
      '/Q',
      CAST(EXTRACT(QUARTER FROM fhv_tripdata.pickup_datetime) AS STRING)
    )                                 AS pickup_year_quarter,
    EXTRACT(MONTH   FROM fhv_tripdata.pickup_datetime) AS pickup_month,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.pulocationid,
    pickup_zone.zone as pickup_zone_name,
    fhv_tripdata.dolocationid,
    dropoff_zone.zone as dropoff_zone_name,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dolocationid = dropoff_zone.locationid