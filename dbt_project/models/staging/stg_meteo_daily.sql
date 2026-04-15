-- stg_meteo_daily
-- Long-format daily meteorological readings from 4 Barcelona stations.
-- Filters to the 8 recognised variables defined in sources.yml.

with source as (
    select * from {{ source('raw', 'meteo_daily') }}
),

valid_variables as (
    -- Allowed variable codes (matches MeteoExtractor._VARIABLES)
    select unnest(array['TM','TX','TN','HRM','PPT24H','VVM10','RS24H','PN']) as variable
),

filtered as (
    select s.*
    from source s
    inner join valid_variables v using (variable)
    where s.station_code is not null
      and s.reading_date  is not null
      and s.value         is not null
),

renamed as (
    select
        trim(station_code)                          as station_code,
        reading_date::date                          as reading_date,
        trim(variable)                              as variable,
        value::numeric(10, 4)                       as value,
        extreme_time::time                          as extreme_time,
        _loaded_at
    from filtered
)

select * from renamed
