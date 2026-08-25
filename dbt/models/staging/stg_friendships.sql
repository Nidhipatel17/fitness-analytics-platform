-- One row per (user_id, friend_user_id) edge. The synthetic client
-- generates both directions of every edge already (see
-- SyntheticClient._generate_friendships), so this is a straight dedupe,
-- not a symmetrization step -- there's no missing reverse edge to fabricate.

with base as (
    select * from {{ source('raw', 'raw_friendships') }}
),

deduped as (
    select *
    from base
    qualify row_number() over (partition by user_id, friend_user_id order by created_at desc) = 1
)

select
    user_id,
    friend_user_id,
    source as source_system,
    cast(created_at as timestamp) as created_at
from deduped
