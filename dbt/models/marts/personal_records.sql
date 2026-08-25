-- Grain: one row per (user_id, workout_type) -- this user's all-time bests
-- for that exercise type. Straight aggregation, no self-join needed here --
-- that's leaderboard.sql's job.

select
    user_id,
    workout_type,
    max(distance_meters) as best_distance_meters,
    max(duration_minutes) as longest_duration_minutes,
    max(calories) as most_calories_burned,
    min(case when distance_meters > 0 then duration_minutes / (distance_meters / 1000.0) end) as best_pace_min_per_km
from {{ ref('fact_workout') }}
group by user_id, workout_type
