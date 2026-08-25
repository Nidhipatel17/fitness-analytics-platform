-- Seeded, not generated: this is genuinely static reference data (a fixed
-- list of workout types), the textbook case for a dbt seed -- the opposite
-- call from dim_date.sql, and worth contrasting: seed for real static
-- lookups, generated SQL for anything mechanically derivable.

select
    {{ dbt_utils.generate_surrogate_key(['workout_type']) }} as exercise_type_sk,
    workout_type,
    category,
    is_cardio,
    met_value
from {{ ref('exercise_types') }}
