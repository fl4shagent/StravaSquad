{{
    config(
        materialized='table'
    )
}}

-- Calendar spine covering every date present in silver.Activities, padded a week on
-- each side so week-over-week DAX measures never fall off the edge of the table.
-- Built from a tally table (sys.all_objects cross join) instead of a recursive CTE,
-- since SQL Server doesn't allow `OPTION (MAXRECURSION ...)` inside a subquery.
with date_bounds as (
    select
        dateadd(day, -7, min(cast([date] as date))) as min_date,
        dateadd(day,  7, max(cast([date] as date))) as max_date
    from {{ source('silver', 'Activities') }}
),

tally as (
    select top (100000)
        row_number() over (order by (select null)) - 1 as n
    from sys.all_objects a
    cross join sys.all_objects b
),

date_spine as (
    select dateadd(day, t.n, db.min_date) as [date]
    from tally t
    cross join date_bounds db
    where dateadd(day, t.n, db.min_date) <= db.max_date
)

select
    [date],
    datepart(year, [date])      as iso_year,
    datepart(iso_week, [date])  as iso_week,
    datename(weekday, [date])   as day_name,
    -- Mon=1 .. Sun=7, independent of @@DATEFIRST (1900-01-01 was a Monday)
    ((datediff(day, 0, [date]) % 7) + 1) as day_of_week,
    -- Monday of this date's week (1900-01-01 trick, also DATEFIRST-independent)
    dateadd(day, (datediff(day, 0, [date]) / 7) * 7, 0) as week_start_date,
    -- 0 = this week, 1 = last week, ... relative to today's week
    (datediff(day, 0, cast(getdate() as date)) / 7)
        - (datediff(day, 0, [date]) / 7) as week_relative_to_today,
    datediff(day, [date], cast(getdate() as date)) as days_ago
from date_spine
