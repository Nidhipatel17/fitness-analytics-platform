{% test duration_under_hours(model, column_name, max_hours=24) %}

-- Business-rule sanity check, not a schema check: a workout this long is
-- structurally valid (right types, all fields present) but still wrong.
-- This is exactly the anomaly synthetic_client.py's anomaly_rate injects on
-- demand, so this test has a real, reproducible case to catch.

select *
from {{ model }}
where {{ column_name }} > ({{ max_hours }} * 60)

{% endtest %}
