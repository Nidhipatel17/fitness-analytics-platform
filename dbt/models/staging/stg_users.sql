-- One row per (user_id, source_system). Dedupe on that natural key, keeping
-- the most recently updated version -- mirrors stg_workouts.sql. This is
-- the feed the users_snapshot.sql SCD2 snapshot reads from, not raw_users
-- directly, so the snapshot inherits the same dedupe/type-casting guarantees
-- as every other consumer of this data.

with base as (
    select * from {{ source('raw', 'raw_users') }}
),

deduped as (
    select *
    from base
    qualify row_number() over (partition by user_id, source order by updated_at desc) = 1
)

select
    user_id,
    source as source_system,
    display_name,
    email,
    age,
    weight_kg,
    height_cm,
    fitness_goal,
    cast(updated_at as timestamp) as updated_at
from deduped
