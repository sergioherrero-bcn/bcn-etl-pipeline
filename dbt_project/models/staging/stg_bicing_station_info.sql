-- stg_bicing_station_info
-- 1:1 with raw.bicing_station_info; casts types and trims strings.

with source as (
    select * from {{ source('raw', 'bicing_station_info') }}
),

renamed as (
    select
        station_id::integer                         as station_id,
        trim(station_name)                          as station_name,
        latitude::numeric(10, 6)                    as latitude,
        longitude::numeric(10, 6)                   as longitude,
        capacity::smallint                          as capacity,
        trim(postal_code)                           as postal_code,
        trim(cross_street)                          as cross_street,
        _extracted_at
    from source
    where station_id is not null
)

select * from renamed
