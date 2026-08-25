-- Thin wrapper over the users_snapshot dbt snapshot: renames dbt's
-- dbt_valid_from/dbt_valid_to to this project's valid_from/valid_to, and
-- turns the open-ended NULL valid_to on the current row into a far-future
-- sentinel. That sentinel is what lets fact_workout join on a plain
-- `start_time >= valid_from AND start_time < valid_to` range without a
-- separate NULL-handling branch for "the current version."

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_valid_from']) }} as user_sk,
    user_id,
    source_system,
    display_name,
    email,
    age,
    weight_kg,
    height_cm,
    fitness_goal,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31')) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('users_snapshot') }}
