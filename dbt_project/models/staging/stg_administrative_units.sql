-- stg_administrative_units
-- Geographic reference dimension: 10 Barcelona districts, 73 neighbourhoods.
-- Slowly-changing; loaded once (or on schema refresh).

with source as (
    select * from {{ source('raw', 'administrative_units') }}
),

renamed as (
    select
        district_id::smallint                       as district_id,
        trim(district_name)                         as district_name,
        neighborhood_id::smallint                   as neighborhood_id,
        trim(neighborhood_name)                     as neighborhood_name,
        latitude::numeric(10, 6)                    as latitude,
        longitude::numeric(10, 6)                   as longitude,
        _extracted_at
    from source
    where district_id is not null
)

select * from renamed
