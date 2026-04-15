-- fct_bicing_availability
-- Grain: one row per (station, snapshot timestamp).
-- Analytics-ready fact table with FK to dim_bicing_stations.

with avail as (
    select * from {{ ref('int_bicing_availability') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['station_id', 'last_reported_at']) }} as availability_key,

    -- Foreign keys
    station_id,

    -- Timestamps
    last_reported_at,
    date_trunc('hour', last_reported_at)    as reported_hour,
    last_reported_at::date                  as reported_date,

    -- Measures
    num_bikes_available,
    num_bikes_mechanical,
    num_bikes_electric,
    num_docks_available,
    total_slots,
    bike_occupancy_rate,

    -- Status flags
    is_installed,
    is_renting,
    is_returning,
    has_bikes_available,
    has_docks_available
from avail
where last_reported_at is not null
