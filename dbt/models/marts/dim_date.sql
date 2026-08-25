-- Generated, not seeded: a date spine is fully derivable from a start date
-- and a range, so a static CSV would just be dead weight to keep extending.
-- Range covers everything the synthetic client can produce (it never
-- backdates more than ~14 days) plus a year of runway past today.

with spine as (
    select date_day
    from unnest(generate_date_array('2024-01-01', date_add(current_date(), interval 1 year), interval 1 day)) as date_day
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk,
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    format_date('%B', date_day) as month_name,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    format_date('%A', date_day) as day_name,
    extract(week from date_day) as week_of_year,
    extract(quarter from date_day) as quarter,
    extract(dayofweek from date_day) in (1, 7) as is_weekend,
    date_day = current_date() as is_today
from spine
