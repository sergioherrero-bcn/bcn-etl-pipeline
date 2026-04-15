-- stg_bicing_station_status
-- Normalises Bicing availability snapshot rows.
-- Boolean flags (is_installed, is_renting, is_returning) are cast to boolean.

with source as (
    select * from {{ source('raw', 'bicing_station_status') }}
),

renamed as (
    select
        station_id::integer                                as station_id,
        num_bikes_available::smallint                      as num_bikes_available,
        coalesce(num_bikes_mechanical::smallint, 0)        as num_bikes_mechanical,
        coalesce(num_bikes_electric::smallint, 0)          as num_bikes_electric,
        num_docks_available::smallint                      as num_docks_available,
        (is_installed::integer = 1)                        as is_installed,
        (is_renting::integer = 1)                          as is_renting,
        (is_returning::integer = 1)                        as is_returning,
        last_reported::timestamptz                         as last_reported_at,
        _loaded_at
    from source
    where station_id is not null
)

select * from renamed
