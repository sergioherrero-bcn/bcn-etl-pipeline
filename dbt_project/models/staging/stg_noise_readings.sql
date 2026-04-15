-- stg_noise_readings
-- 1-minute Leq noise readings from the Barcelona monitoring network.
-- Physical bounds (0–140 dB) already enforced by the extractor; re-checked here.

with source as (
    select * from {{ source('raw', 'noise_readings') }}
),

renamed as (
    select
        reading_date::date                          as reading_date,
        reading_time::time                          as reading_time,
        trim(monitor_id)                            as monitor_id,
        leq_db::numeric(6, 2)                       as leq_db,
        trim(_source_file)                          as source_file,
        _loaded_at
    from source
    where reading_date is not null
      and leq_db between 0 and 140
)

select * from renamed
