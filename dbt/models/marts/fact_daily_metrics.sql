-- Grain: one row per user per calendar day -- dense, not sparse. Every user
-- gets a row for every day in range, 0-workout days included.
--
-- This is the grain distinction that matters most in this model: a rolling
-- 7-day average computed with `ROWS BETWEEN 6 PRECEDING` only means "6
-- calendar days back" if there are no gap rows. On a sparse table (rows
-- only for days someone worked out), a user who skips 3 days silently
-- pulls in 9 calendar days' worth of history under a "7-day" label. Dense
-- rows -- built from dim_user x dim_date, left-joined to actual activity --
-- make ROWS and RANGE framing agree, so this doesn't have to be gotten
-- exactly right by convention.
--
-- Joined to each user's *current* dim_user row rather than an as-of match:
-- unlike fact_workout, nothing here depends on point-in-time attributes --
-- these are behavioral counts/durations, not "weight at the time," so the
-- extra join complexity wouldn't buy anything.

with date_spine as (
    select date_day
    from {{ ref('dim_date') }}
    where date_day between (select min(date(start_time)) from {{ ref('stg_workouts') }}) and current_date()
),

current_users as (
    select user_id, user_sk
    from {{ ref('dim_user') }}
    where is_current
),

user_days as (
    select
        u.user_id,
        u.user_sk,
        d.date_day
    from current_users u
    cross join date_spine d
),

daily_workouts as (
    select
        user_id,
        date(start_time) as workout_date,
        count(*) as workout_count,
        sum(duration_minutes) as total_duration_minutes,
        sum(distance_meters) as total_distance_meters,
        sum(calories) as total_calories
    from {{ ref('stg_workouts') }}
    group by user_id, date(start_time)
),

joined as (
    select
        ud.user_id,
        ud.user_sk,
        ud.date_day,
        coalesce(dw.workout_count, 0) as workout_count,
        coalesce(dw.total_duration_minutes, 0) as total_duration_minutes,
        coalesce(dw.total_distance_meters, 0) as total_distance_meters,
        coalesce(dw.total_calories, 0) as total_calories
    from user_days ud
    left join daily_workouts dw
        on dw.user_id = ud.user_id and dw.workout_date = ud.date_day
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'date_day']) }} as daily_metric_sk,
    user_id,
    user_sk,
    date_day,
    workout_count,
    total_duration_minutes,
    total_distance_meters,
    total_calories,

    avg(total_duration_minutes) over (
        partition by user_id order by unix_date(date_day)
        range between 6 preceding and current row
    ) as rolling_7d_avg_duration_minutes,

    safe_divide(
        sum(total_duration_minutes) over (
            partition by user_id order by unix_date(date_day)
            range between 6 preceding and current row
        ) - sum(total_duration_minutes) over (
            partition by user_id order by unix_date(date_day)
            range between 13 preceding and 7 preceding
        ),
        nullif(
            sum(total_duration_minutes) over (
                partition by user_id order by unix_date(date_day)
                range between 13 preceding and 7 preceding
            ),
            0
        )
    ) as week_over_week_duration_pct_change,

    -- gap since this user's previous session, only meaningful on a day that
    -- actually had a workout (null otherwise). Uses a running MAX over prior
    -- rows rather than LAG(... IGNORE NULLS) -- same result, plainer SQL.
    case when workout_count > 0 then
        date_diff(
            date_day,
            max(case when workout_count > 0 then date_day end) over (
                partition by user_id order by unix_date(date_day)
                rows between unbounded preceding and 1 preceding
            ),
            day
        )
    end as days_since_previous_workout

from joined
