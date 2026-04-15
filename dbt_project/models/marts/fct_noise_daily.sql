-- fct_noise_daily
-- Grain: one row per (monitor, date).
-- Daily noise level fact table with WHO compliance flag.

with noise as (
    select * from {{ ref('int_noise_daily_agg') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['monitor_id', 'reading_date']) }} as noise_key,

    monitor_id,
    reading_date,

    -- Measures
    reading_count,
    leq_mean_db,
    leq_min_db,
    leq_max_db,
    leq_median_db,

    -- Data quality / completeness
    round(reading_count::numeric / (24 * 60), 4)    as data_completeness_rate,  -- vs 1440 expected readings/day

    -- Compliance
    exceeds_who_daytime
from noise
