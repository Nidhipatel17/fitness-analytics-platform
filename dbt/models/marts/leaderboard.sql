-- Grain: one row per (user_id, member_user_id) -- "from user_id's point of
-- view, how does member_user_id rank in their leaderboard group this week."
--
-- This is the self-join the plan called out: stg_friendships joins back to
-- dim_user through friend_user_id, i.e. dim_user joined to itself via the
-- friendship bridge. leaderboard_groups unions in a self-edge (user_id ->
-- user_id) per current user so everyone's own stats are ranked alongside
-- their friends', not just their friends' against each other.

with weekly_metrics as (
    select
        user_id,
        sum(total_distance_meters) as distance_7d,
        sum(workout_count) as workouts_7d
    from {{ ref('fact_daily_metrics') }}
    where date_day between date_sub(current_date(), interval 6 day) and current_date()
    group by user_id
),

leaderboard_groups as (
    select user_id, friend_user_id as member_user_id
    from {{ ref('stg_friendships') }}
    union all
    select user_id, user_id as member_user_id
    from {{ ref('dim_user') }}
    where is_current
)

select
    lg.user_id,
    lg.member_user_id,
    du.display_name as member_name,
    coalesce(wm.distance_7d, 0) as distance_7d,
    coalesce(wm.workouts_7d, 0) as workouts_7d,
    rank() over (partition by lg.user_id order by coalesce(wm.distance_7d, 0) desc) as rank_by_distance
from leaderboard_groups lg
join {{ ref('dim_user') }} du
    on du.user_id = lg.member_user_id
    and du.is_current
left join weekly_metrics wm
    on wm.user_id = lg.member_user_id
