-- fct_meteo_daily
-- Grain: one row per (station, date).
-- Wide-format daily weather fact table.

with meteo as (
    select * from {{ ref('int_meteo_pivoted') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['station_code', 'reading_date']) }} as meteo_key,

    station_code,
    reading_date,

    -- Temperature
    temp_mean_c,
    temp_max_c,
    temp_min_c,
    temp_range_c,

    -- Precipitation & humidity
    precipitation_mm,
    humidity_mean_pct,

    -- Wind & radiation
    wind_speed_ms,
    solar_radiation_wm2,

    -- Pressure
    pressure_hpa,

    -- Derived flags
    is_heat_day,
    has_precipitation
from meteo
