# Power BI Handoff — StravaDW gold layer (dim_date, dim_distance_label, gold_activity_start_location)

This doc covers everything needed to wire the new `StravaDW.gold.*` tables into the
existing `.pbit` model and replace the relevant visuals' calculations with DAX measures.
Source: `silver.*` tables already used today; new tables add date/week intelligence,
an ordered distance-label dimension, and one-pin-per-run map points.

---

## 1. New tables to import

Connect to SQL Server `HANG`, database `StravaDW` (same server as the existing `silver.*`
tables), and import:

- `gold.dim_date`
- `gold.dim_distance_label`
- `gold.gold_activity_start_location`

## 2. Relationships to add

| From | To | Cardinality | Cross-filter |
|---|---|---|---|
| `gold.dim_date[date]` | `silver.Activities[date]` | 1 : many | Single (dim_date → Activities) |
| `gold.dim_distance_label[distance_label]` | `silver.RunBest[name]` | 1 : many | Single (dim_distance_label → RunBest) |
| `gold.gold_activity_start_location[activity_id]` | `silver.Activities[activity_id]` | 1 : 1 | **Both** (so dim_date/athlete slicers filter the map pins) |

## 3. Mark `dim_date` as a Date Table

Model view → `dim_date` table → **Mark as date table** → date column = `date`.
This unlocks `TOTALMTD`, `SAMEPERIODLASTYEAR`, etc. if you want them later, and is
required by some time-intelligence functions used below.

## 4. Sort `dim_distance_label` by `sort_order`

Model view → `dim_distance_label[distance_label]` column → **Sort by column** →
`sort_order`. This makes the Personal Best table render in the correct distance order
(400m → Marathon) without manual sorting.

---

## 5. DAX measures

### Squad dashboard

**Athletes training today**
```dax
Athletes Training Today =
CALCULATE(DISTINCTCOUNT(Activities[athlete_id]), Activities[date] = TODAY())
```
(denominator "9" = `DISTINCTCOUNT(Athlete[athlete_id])`)

**Latest Run (card text)**
```dax
Latest Run =
VAR LatestTime = MAX(Activities[start_time_utc])
RETURN
CALCULATE(
    MAX(Activities[athlete_name]) & " - " & FORMAT(LatestTime, "mmm d, yyyy (hh:mm)"),
    Activities[start_time_utc] = LatestTime
)
```

**This week / last week (repeat pattern for distance, run count, runtime)**
```dax
Total Distance This Week =
CALCULATE(SUM(Activities[distance_km]), dim_date[week_relative_to_today] = 0)

Total Distance Last Week =
CALCULATE(SUM(Activities[distance_km]), dim_date[week_relative_to_today] = 1)

Distance WoW % =
DIVIDE([Total Distance This Week] - [Total Distance Last Week], [Total Distance Last Week])

Total Runs This Week =
CALCULATE(DISTINCTCOUNT(Activities[activity_id]), dim_date[week_relative_to_today] = 0)

Total Runs Last Week =
CALCULATE(DISTINCTCOUNT(Activities[activity_id]), dim_date[week_relative_to_today] = 1)

Total Runtime This Week (sec) =
CALCULATE(SUM(Activities[duration_sec]), dim_date[week_relative_to_today] = 0)

Total Runtime Last Week (sec) =
CALCULATE(SUM(Activities[duration_sec]), dim_date[week_relative_to_today] = 1)

Total Runtime This Week (hh:mm) =
VAR S = [Total Runtime This Week (sec)]
RETURN FORMAT(S / 86400, "hh:mm")
```

**This Month**
```dax
Total Distance This Month =
CALCULATE(
    SUM(Activities[distance_km]),
    FILTER(ALL(dim_date), dim_date[date] <= TODAY()
        && dim_date[iso_year] = YEAR(TODAY())
        && MONTH(dim_date[date]) = MONTH(TODAY()))
)
```
(same pattern for run count / runtime "This Month")

**Top Runner / Bottom Runner (this week, by distance)**
```dax
Top Runner This Week =
VAR T =
    ADDCOLUMNS(
        VALUES(Activities[athlete_name]),
        "@Dist", CALCULATE([Total Distance This Week])
    )
VAR Best = TOPN(1, T, [@Dist], DESC)
RETURN
    CONCATENATEX(Best, Activities[athlete_name]) & " (" &
    FORMAT(MAXX(Best, [@Dist]), "0.0") & " km)"

Bottom Runner This Week =
VAR T =
    ADDCOLUMNS(
        VALUES(Activities[athlete_name]),
        "@Dist", CALCULATE([Total Distance This Week])
    )
VAR Worst = TOPN(1, FILTER(T, [@Dist] > 0), [@Dist], ASC)
RETURN
    CONCATENATEX(Worst, Activities[athlete_name]) & " (" &
    FORMAT(MAXX(Worst, [@Dist]), "0.0") & " km)"
```
Apply the same pattern to "Total Run" and "Total Runtime" cards' top/bottom runner.

**Daily Runs bar (today, per athlete)** — filter visual to `dim_date[days_ago] = 0`,
then:
```dax
Pace (sec per km) = DIVIDE(SUM(Activities[duration_sec]), SUM(Activities[distance_km]))

Pace (mm:km) =
VAR P = [Pace (sec per km)]
RETURN FORMAT(P / 86400, "mm:ss") & "/km"
```

**Squad Weekly Running Distance bar** — X axis = `dim_date[week_start_date]`
(via the dim_date → Activities relationship), Y = `SUM(Activities[distance_km])`.

**Scatterplot (weekly volume vs. time, per athlete)** — filter to a selected week
(slicer on `dim_date[week_start_date]` or `dim_date[week_relative_to_today]`):
- X = `SUM(Activities[distance_km])`
- Y = `SUM(Activities[duration_sec])`
- Legend = `Activities[athlete_name]`

**Weekly stats table**
```dax
Run Frequency = DISTINCTCOUNT(Activities[date]) / 7

Avg Cadence = AVERAGE(Activities[cad_avg])
Avg Heart Rate = AVERAGE(Activities[hr_avg])
```
`Total Distance`, `Run Count` (`DISTINCTCOUNT(activity_id)`), `Total Time`
(`SUM(duration_sec)`) reuse the measures above, all filtered by the
`dim_date[week_relative_to_today] = 0` (or whichever week is selected).

**This Week Map** — use `gold.gold_activity_start_location` directly as the map
visual's source (`start_lat`/`start_lon`, one row per run). It already carries
`athlete_id`, `athlete_name`, `date` for tooltips/legends, and is filterable by
`dim_date` via the `Both`-direction relationship to `Activities`.

---

### Individual dashboard

**Athlete details card** — direct fields from `silver.Athlete`, filtered by the
existing athlete slicer. No change.

**This week (km) / This Week (run count) / This Week (hh:mm)** — same measures as
the Squad "This week" cards above; they'll automatically respect the athlete slicer
via the existing `Athlete` ↔ `Activities` relationship.

**Vega bubble chart (week × day grid)**
Build the chart's data table from:
- `Activities[athlete_id]` = selected athlete
- `dim_date[week_relative_to_today]` (0–5, last 6 weeks) → map to "Week N" as
  `Week Label = "Week " & (1 - dim_date[week_relative_to_today]) * -1` i.e.
  `"Week " & (dim_date[week_relative_to_today] + 1)`
- `dim_date[day_name]` / `dim_date[day_of_week]` for the x-axis (Mon–Sun)
- value = `SUM(Activities[distance_km])` per day

Filter `dim_date[week_relative_to_today]` between 0 and 5 for the "last 6 weeks" window.

**Personal Best table**
With the `dim_distance_label → RunBest` relationship and sort-by-`sort_order` applied:
- Rows = `dim_distance_label[distance_label]` (already in correct order)
- Personal Best column:
```dax
Personal Best (sec) = MIN(RunBest[elapsed_time])

Personal Best =
VAR S = [Personal Best (sec)]
RETURN IF(ISBLANK(S), BLANK(), FORMAT(S / 86400, "hh:mm:ss"))
```
This is filtered by the athlete slicer via the existing `Athlete` ↔ `RunBest` relationship
(`athlete_id`).

**Overview of Run / Average Pace by km / Heart Rate & Watts / Route map** — all
unchanged, still driven directly by `silver.RunSplitKilometer` / `silver.RunStream` /
`silver.Activities` filtered to the selected activity.

---

## 6. Notes / known gaps
- `gold_activity_start_location` has 955 rows vs. 1,465 in `silver.Activities` —
  ~510 activities have no GPS stream (e.g., manual/treadmill entries) and so have no
  map pin. This is expected, not a data quality bug.
- `dim_date` currently spans 2025-05-01 to 2026-06-17 (one week padding around the
  min/max dates in `silver.Activities`). It will need re-running (`dbt run`) if the
  underlying data range grows beyond that padding — re-running is idempotent
  (`materialized='table'`, full rebuild each time).
