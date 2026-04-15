-- int_bicing_availability
-- Joins station status snapshots with station metadata.
-- Adds derived metrics: total bikes, occupancy rate, availability flag.

with status as (
    select * from {{ ref('stg_bicing_station_status') }}
),

info as (
    select * from {{ ref('stg_bicing_station_info') }}
),

joined as (
    select
        s.station_id,
        i.station_name,
        i.latitude,
        i.longitude,
        i.postal_code,
        i.capacity,
        s.num_bikes_available,
        s.num_bikes_mechanical,
        s.num_bikes_electric,
        s.num_docks_available,
        s.is_installed,
        s.is_renting,
        s.is_returning,
        s.last_reported_at,

        -- Derived
        s.num_bikes_available + s.num_docks_available           as total_slots,
        case
            when i.capacity > 0
            then round(s.num_bikes_available::numeric / i.capacity, 4)
        end                                                      as bike_occupancy_rate,
        (s.num_bikes_available > 0 and s.is_renting)            as has_bikes_available,
        (s.num_docks_available > 0 and s.is_returning)          as has_docks_available
    from status s
    left join info i using (station_id)
)

select * from joined
