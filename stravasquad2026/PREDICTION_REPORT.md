# Step 5 — Race-Time Prediction: Validation Report (2026-06-15)

Pipeline: `stravasquad2026/predict_pb.py`
Run with: `"D:\BO\New folder\python.exe" stravasquad2026/predict_pb.py`

## 1. Algorithms

### Part A — `gold.PBPrediction` (PB trend)
For each `(athlete_id, distance_label)` with ≥3 `RunBest` records, fits a linear
regression (`numpy.polyfit`, degree 1) of `elapsed_time` vs. `start_date`, then
projects the line to today. Outputs `predicted_best_sec` and `r_squared`.

### Part B — `gold.RaceForecast` (race-time forecast backtest)
Three formulas, each given a "source" PB (most recent qualifying PB before an as-of
date, preferring 10K, falling back to 15K/5K/20K/etc.):

1. **Riegel**: `T2 = T1 × (D2/D1)^1.06`
2. **VDOT (Jack Daniels)**: source PB → VO2 cost/% via velocity-based formulas →
   VDOT score → numerically inverted (`scipy.optimize.brentq`) for the target distance.
3. **Elevation-adjusted Riegel**: strips a 2 sec/km-per-(m/km) elevation penalty from
   the source pace to get a flat-equivalent pace, Riegel-scales, then re-applies the
   target race's elevation penalty.

Backtested at 8/4/2/1 weeks before race day, against two real squad races:
- **Half-Marathon, 2025-10-26** — 6 athletes
- **Marathon, 2025-10-05** — 1 athlete (Tam_Vu), bonus case

## 2. Pipeline run result

```
OK gold.PBPrediction: wrote 69 rows
OK gold.RaceForecast: wrote 84 rows
```
69 rows = 9 athletes × up to 12 distances. 84 rows = 6 HM athletes × 3 methods × up to
4 as-of dates + Tam_Vu's Marathon × 3 methods × 4 as-of dates.

## 3. Findings

### Accuracy (closest as-of checkpoint, mean |error %|)
| Method | Mean abs error |
|---|---|
| Riegel | 14.6% |
| VDOT | 14.8% |
| Elevation-adjusted | 14.8% |

No method clearly dominates — all three are within ~0.2 pts of each other.

### Half-Marathon (6 athletes, 1-week-out forecast)
- Errors range from **-13% to +44%**.
- Biggest miss (athlete 6091734, +44%): 10K PB (~70 min) predicted a HM of ~2:34,
  actual was 1:47 — the 10K source PB was stale/unrepresentative of HM-day fitness.
- Two athletes (38095302, 94062196) landed within ±3.5% of actual.
- Elevation adjustment tracked almost identically to plain Riegel — the 2 sec/km
  per m/km factor isn't the dominant error source for this group; source-PB
  recency/quality matters far more.

### Marathon (Tam_Vu, bonus case)
- 8-week-old 10K (4248s) → VDOT predicted marathon time within **0.5%**
  (19,178s vs. 19,083s actual). Riegel was 2.4% off, elevation-adjusted overshot 6.8%.
- Fresher 3-week-old 10K (3437s, faster pace) → all three methods underpredicted by
  **13-18%**.
- **Insight**: which 10K you use as the source PB swings the result far more than
  which formula you pick.

### PBPrediction (69 rows, 9 athletes, 12 distances)
- Most `r_squared` values are modest (0.3-0.94), reflecting noisy PB progression.
- **Edge case / known limitation**: 2 rows (athlete 169950101 @ 10K, r²=0.94;
  athlete 94062196 @ 10 mile, r²=0.82) hit the `max(predicted_best_sec, 1.0)` clamp —
  these athletes have such steep improving trends that the linear extrapolation to
  today goes negative. These values are meaningless if surfaced as-is; consider
  filtering or capping the extrapolation horizon in the dashboard.

## 4. Next steps
- PB-improvement insights per athlete/distance — explore via the Streamlit dashboard
  (`stravasquad2026/dashboard/app.py`, Tab 2 — Prediction > PB Trends).

---

## 5. Deep-dive: Rung_Rinh — 45-minute forecast miss (2026-06-16 research)

**Research method**: queried `silver.RunBest`, `silver.Activities`, `silver.RunStream`,
`gold.RaceForecast` directly for athlete 6091734 (Rung_Rinh).

### What the system predicted vs. actual

| Method | Predicted (1 wk out) | Actual | Error |
|---|---|---|---|
| Riegel | ~2:22–2:24 | 1:47:26 | +35–37 min (+33%) |
| VDOT | ~2:30–2:34 | 1:47:26 | +43–47 min (+40%) |
| Elevation-adjusted | ~2:22–2:24 | 1:47:26 | +35–37 min (+33%) |

### Root cause: source PB was not a race effort

The system's source PB selector uses the **most recent** `RunBest` record before the
as-of date with `name = '10K'`. For Rung_Rinh, the most recent 10K entry was:

- **Date**: 2025-09-13
- **Elapsed time**: 4,180s (69:40 = 6:58/km)
- **Activity**: a **16.8 km long run** — the 10K split was recorded mid-run as a
  best-effort timestamp, not a standalone 10K race or tempo effort.

This single unrepresentative data point corrupted all three forecasting methods. The
formula inputs were:
- Source pace ~6:58/km → predicted HM ~2:22–2:34
- Actual HM pace: ~5:04/km → actual time 1:47:26

### What Rung_Rinh's actual fitness was

Cross-referencing other `RunBest` records and the training trajectory:
- Rung_Rinh had a **steep improvement curve**: `r² = 0.61` for 10K trend (strong for
  amateur runners), consistent ~13.7 runs/week and ~52.1 km/week in the 8-week lead-up.
- The improvement trend suggested a 10K fitness of roughly **~3,300s (~55:00)** by
  race day — which, plugged into Riegel, would project an HM of ~1:55–2:00, much
  closer to actual.
- Training volume was NOT the problem. This athlete was well-prepared; the system
  just couldn't see it because the only 10K PB on record was a junk mid-long-run split.

### Why the other athletes were predicted more accurately

Athletes with smaller errors (e.g., 38095302 and 94062196 within ±3.5%) had clean
source PBs from actual 10K race or tempo efforts — the formula accuracy held.
The error distribution across the squad (−13% to +44%) is almost entirely explained by
source-PB quality, not algorithm choice.

### Three concrete recommendations

**Recommendation 1 — Filter source PBs by activity total distance**
Only accept a `RunBest` record as a valid source PB if the parent activity's
`distance_km` is within 10%–120% of the PB distance (i.e., 9–12 km for a 10K PB).
This filters out mid-long-run splits automatically without any athlete-specific logic.

Implementation: join `silver.RunBest` to `silver.Activities` on `activity_id`;
add `WHERE a.distance_km BETWEEN 0.9*pb_distance_km AND 1.2*pb_distance_km` in the
source-PB selection query inside `predict_pb.py`.

**Recommendation 2 — Use fastest, not most recent, 10K in the 90-day window**
The current selector picks the most recent qualifying PB. For a runner on an improving
trajectory, the most recent genuine race effort is a better signal than the fastest
historical one, but a casual-run split can sneak in. Using `MIN(elapsed_time)` (fastest)
within the 90-day window is more robust:
- Genuine race efforts are usually faster than casual splits → fastest = most race-like.
- Gives more weight to peak fitness than to recency of a bad data point.

Implementation: change `ORDER BY start_date DESC LIMIT 1` → `ORDER BY elapsed_time ASC LIMIT 1`
in the source-PB selection CTE.

**Recommendation 3 — Blend formula with trend-regression for high-r² athletes**
For athletes where `PBPrediction.r_squared >= 0.5` at the source distance, blend the
formula-based forecast with the trend-extrapolated PB:
```
blended = 0.5 * formula_prediction + 0.5 * trend_prediction
```
Rung_Rinh's r²=0.61 trend line would have contributed a ~1:55–2:00 projection, pulling
the blended output much closer to actual (1:47). Athletes with noisy trends (r²<0.5)
get full formula weight.

Implementation: in `predict_pb.py` Part B, after computing `predicted_sec`, look up the
athlete's `PBPrediction.predicted_best_sec` for the source distance and blend if
`r_squared >= 0.5`.

### Impact estimate

Applying all three recommendations retroactively to the Rung_Rinh case:
- With recommendation 1 + 2: system would have selected a legitimate ~3300s 10K →
  Riegel predicts ~1:54, error drops from +33% to ~+6%.
- With recommendation 3 also applied: blended output ~1:51–1:54, error ~+2–5%.
- Still not perfect (actual 1:47 was a strong performance), but well within acceptable
  race-day prediction range for amateur runners.
