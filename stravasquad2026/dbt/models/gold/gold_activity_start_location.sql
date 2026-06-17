{{
    config(
        materialized='table'
    )
}}

-- One row per activity: the first GPS point recorded (lowest time_s), used to plot a
-- single map pin per run instead of scanning the full RunStream table in Power BI.
with first_point as (
    select
        activity_id,
        lat,
        lon,
        row_number() over (partition by activity_id order by time_s asc) as rn
    from {{ source('silver', 'RunStream') }}
    where lat is not null and lon is not null
)

select
    a.activity_id,
    a.athlete_id,
    a.athlete_name,
    a.date,
    a.activity_type,
    fp.lat as start_lat,
    fp.lon as start_lon
from {{ source('silver', 'Activities') }} a
inner join first_point fp
    on fp.activity_id = a.activity_id
    and fp.rn = 1
