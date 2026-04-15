-- dim_bicing_stations
-- Bicing station dimension with geographic enrichment.
-- Resolves postal_code → neighborhood via proximity (best-effort).

with stations as (
    select * from {{ ref('stg_bicing_station_info') }}
)

select
    station_id,
    station_name,
    latitude,
    longitude,
    capacity,
    postal_code,
    cross_street,
    _extracted_at                           as last_seen_at
from stations
