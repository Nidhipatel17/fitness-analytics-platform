-- Grain: one row per workout.
--
-- samples gets flattened here, not in staging: UNNEST once per workout to
-- compute heart-rate aggregates, still one row out per workout in. Doing
-- this in staging would mean exploding into one row per sample just to
-- immediately re-aggregate -- wasted work and a staging grain that lies
-- about what stg_workouts actually represents.
--
-- The join to dim_user is an as-of join on the SCD2 validity window, not a
-- plain join on user_id: a naive join would attach every workout -- even
-- one from six months ago -- to the user's *current* weight/goal, which
-- would make any "weight at time of workout" analysis silently wrong. Half-
-- open interval (>= valid_from, < valid_to) rather than BETWEEN, so a
-- workout can never double-match two adjacent SCD2 versions at the exact
-- boundary timestamp.

with workouts as (
    select * from {{ ref('stg_workouts') }}
),

sample_agg as (
    select
        workout_id,
        avg(sample.heart_rate_bpm) as avg_heart_rate_bpm,
        max(sample.heart_rate_bpm) as max_heart_rate_bpm,
        min(sample.heart_rate_bpm) as min_heart_rate_bpm,
        count(sample.ts) as sample_count
    from workouts, unnest(samples) as sample
    group by workout_id
)

select
    {{ dbt_utils.generate_surrogate_key(['w.workout_id']) }} as workout_sk,
    w.workout_id,
    w.user_id,
    du.user_sk,
    det.exercise_type_sk,
    dd.date_sk as workout_date_sk,
    w.workout_type,
    w.source_system,
    w.start_time,
    w.end_time,
    w.duration_minutes,
    w.device_id,
    w.distance_meters,
    w.calories,
    sa.avg_heart_rate_bpm,
    sa.max_heart_rate_bpm,
    sa.min_heart_rate_bpm,
    sa.sample_count,
    w.samples
from workouts w
left join sample_agg sa
    on sa.workout_id = w.workout_id
left join {{ ref('dim_user') }} du
    on du.user_id = w.user_id
    and w.start_time >= du.valid_from
    and w.start_time < du.valid_to
left join {{ ref('dim_exercise_type') }} det
    on det.workout_type = w.workout_type
left join {{ ref('dim_date') }} dd
    on dd.date_day = date(w.start_time)
