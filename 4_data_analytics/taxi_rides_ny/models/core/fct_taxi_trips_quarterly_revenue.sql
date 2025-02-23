{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),
quarterly_revenue as (
    select 
        service_type,
        pickup_year,
        pickup_quarter,
        pickup_year_quarter,
        sum(total_amount) as quarterly_revenues
    from trips_data
    where pickup_year in (2019,2020)
    group by service_type, pickup_year, pickup_quarter, pickup_year_quarter
)

select
    cur.service_type,
    cur.pickup_year,
    cur.pickup_quarter,
    cur.pickup_year_quarter,
    cur.quarterly_revenues,
    prev.quarterly_revenues as prev_quarterly_revenues,
    round(((cur.quarterly_revenues - prev.quarterly_revenues) / prev.quarterly_revenues) * 100, 2) as quarterly_growth
from quarterly_revenue as cur
left join quarterly_revenue as prev
    on cur.service_type = prev.service_type
    and cur.pickup_quarter = prev.pickup_quarter
    and cur.pickup_year = prev.pickup_year + 1

-- select
--     quarterly_revenues
-- from quarterly_revenue
-- where pickup_year = 2020

    -- select 
    -- -- Revenue grouping 
    -- service_type,
    -- pickup_year,
    -- pickup_quarter,
    -- pickup_year_quarter,


    -- -- Revenue calculation 
    -- sum(total_amount) as quarterly_revenues,

    -- -- quarterly_revenues/2 as half
    -- -- from quarterly_revenue as cur
    -- -- left join quarterly_revenue as prev
    -- -- on cur.service_type = prev.service_type
    -- -- and cur.pickup_quarter = prev.pickup_quarter
    -- -- and cur.pickup_year = prev.pickup_year + 1

    -- from trips_data
    -- where pickup_year = 2020
    -- group by 1,2,3,4