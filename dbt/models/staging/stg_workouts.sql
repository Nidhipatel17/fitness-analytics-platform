-- One row per workout. Dedupe on the natural key (workout_id), keeping the
-- most recently synced version -- same QUALIFY/ROW_NUMBER pattern used to
-- dedupe load_raw.py's staging table, kept consistent rather than inventing
-- a second mechanism for the same problem.
--
-- samples stays nested (ARRAY<STRUCT<...>>) rather than being exploded into
-- its own row per sample here: UNNEST-ing now would multiply every workout
-- column across dozens of sample rows just to compute a handful of
-- aggregates. That flattening happens in fact_workout, where it belongs --
-- one UNNEST + GROUP BY, still one row out per workout in.

with base as (
    select * from {{ source('raw', 'raw_workouts') }}
),

deduped as (
    select *
    from base
    qualify row_number() over (partition by workout_id order by synced_at desc) = 1
)

select
    workout_id,
    user_id,
    source as source_system,
    workout_type,
    cast(start_time as timestamp) as start_time,
    cast(end_time as timestamp) as end_time,
    timestamp_diff(cast(end_time as timestamp), cast(start_time as timestamp), minute) as duration_minutes,
    device_id,
    cast(synced_at as timestamp) as synced_at,
    distance_meters,
    calories,
    samples
from deduped
