-- int_noise_daily_agg
-- Aggregates 1-minute Leq noise readings to daily level per monitor.
-- Produces Leq daily average, min, max, and reading count.
-- WHO guideline threshold: 53 dB daytime, 45 dB night.

with source as (
    select * from {{ ref('stg_noise_readings') }}
),

daily as (
    select
        reading_date,
        monitor_id,
        count(*)                                        as reading_count,
        round(avg(leq_db)::numeric, 2)                  as leq_mean_db,
        round(min(leq_db)::numeric, 2)                  as leq_min_db,
        round(max(leq_db)::numeric, 2)                  as leq_max_db,
        round(percentile_cont(0.5)
              within group (order by leq_db)::numeric, 2) as leq_median_db,
        -- WHO daytime guideline exceedance flag (53 dB)
        (avg(leq_db) > 53)                              as exceeds_who_daytime
    from source
    group by reading_date, monitor_id
)

select * from daily
