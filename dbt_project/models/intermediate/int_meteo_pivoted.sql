-- int_meteo_pivoted
-- Pivots the long-format meteorological data into wide format.
-- One row per (station_code, reading_date) with one column per variable.

with source as (
    select * from {{ ref('stg_meteo_daily') }}
),

pivoted as (
    select
        station_code,
        reading_date,
        max(case when variable = 'TM'     then value end) as temp_mean_c,
        max(case when variable = 'TX'     then value end) as temp_max_c,
        max(case when variable = 'TN'     then value end) as temp_min_c,
        max(case when variable = 'HRM'    then value end) as humidity_mean_pct,
        max(case when variable = 'PPT24H' then value end) as precipitation_mm,
        max(case when variable = 'VVM10'  then value end) as wind_speed_ms,
        max(case when variable = 'RS24H'  then value end) as solar_radiation_wm2,
        max(case when variable = 'PN'     then value end) as pressure_hpa
    from source
    group by station_code, reading_date
),

enriched as (
    select
        *,
        -- Comfort index: temperature range for the day
        temp_max_c - temp_min_c                     as temp_range_c,
        -- Simple heat index flag (>30°C mean)
        (temp_mean_c > 30)                          as is_heat_day,
        -- Rain flag
        (coalesce(precipitation_mm, 0) > 0)         as has_precipitation
    from pivoted
)

select * from enriched
