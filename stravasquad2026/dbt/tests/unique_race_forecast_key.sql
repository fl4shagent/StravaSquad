-- Singular test: gold.RaceForecast should have one row per
-- (athlete_id, target_distance_label, method, as_of_date). Fails if any duplicates exist.
select athlete_id, target_distance_label, method, as_of_date, count(*) as n
from {{ source('gold_predictions', 'RaceForecast') }}
group by athlete_id, target_distance_label, method, as_of_date
having count(*) > 1
