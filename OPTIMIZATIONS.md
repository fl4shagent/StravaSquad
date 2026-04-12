# Pipeline Optimizations

## 1. datawarehouse.py — Manifest Pattern
Added a `.processed_files.txt` manifest to track already-merged files.
- Only reads **new files** not in the manifest each run
- Appends to existing CSVs instead of overwriting
- Manifest updates only after successful export (crash-safe)

```
Before: reads all 500+ files every run
After:  reads only new files since last run
```

---

## 2. datamart.py — Activity ID Checkpoint + Vectorized Haversine
Uses the output CSV itself as a checkpoint — reads existing `activity_id`s and skips them.
- Filters all inputs (runstream, best, segments) to new activity_ids only
- Replaced `geopy.geodesic` Python loop with **vectorized NumPy haversine**
- Athletes always fully reprocessed (small table, profiles can change)

```
Before: processes 1.3M GPS rows every run, geopy loop per point
After:  processes only new activity rows, haversine on entire array at once
```

---

## 3. datamart_to_sql.py — Incremental Upsert
Replaced full `TRUNCATE + reload` with two strategies per table based on data mutability.

| Strategy | Tables | Logic |
|----------|--------|-------|
| `insert_only` | RunStream, RunBest, RunSegment, RunSplitKilometer | Skip `activity_id`s already in DB |
| `upsert` | Activities, Athlete | Delete touched rows, re-insert from CSV |

```
Before: TRUNCATE all 6 tables, reload 1.3M rows every run
After:  insert only new rows, update only changed rows
```
