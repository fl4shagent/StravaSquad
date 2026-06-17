-- Singular test: gold.PBPrediction should have one row per
-- (athlete_id, distance_label, prediction_date). Fails if any duplicates exist.
select athlete_id, distance_label, prediction_date, count(*) as n
from {{ source('gold_predictions', 'PBPrediction') }}
group by athlete_id, distance_label, prediction_date
having count(*) > 1
