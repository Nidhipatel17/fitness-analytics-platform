{% snapshot users_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

-- user_id is globally unique across sources already (synthetic_client and
-- strava_client both prefix it: "u-0001", "strava-123"), so it's the right
-- unique_key here even though stg_users itself dedupes on (user_id, source) --
-- that dedupe is about the raw MERGE's natural key, this is about one SCD2
-- timeline per physical person.
select * from {{ ref('stg_users') }}

{% endsnapshot %}
