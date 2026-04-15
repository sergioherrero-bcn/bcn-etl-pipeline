-- dim_geography
-- Conformed geographic dimension: districts and neighbourhoods.
-- Surrogate key: neighborhood_id (unique within Barcelona).

with source as (
    select * from {{ ref('stg_administrative_units') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['district_id', 'neighborhood_id']) }} as geo_key,
    district_id,
    district_name,
    neighborhood_id,
    neighborhood_name,
    latitude,
    longitude
from source
