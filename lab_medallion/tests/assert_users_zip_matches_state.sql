{{ config(severity='warn') }}

-- The ZIP code must belong to one of the ZIP ranges of the state (seed state_zip_ranges).
-- Faker draws the state and the ZIP code independently: most users fail, hence a warning.
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('state_zip_ranges') }} r
    where r.state = u.state
      and u.zip_code between r.zip_min and r.zip_max
)
