# StravaSquad 🏃‍♀️🏃‍♂️

**StravaSquad** is a multi-runner analytics pipeline and dashboard for Strava data.  
It securely onboards runners via OAuth, ingests activities (1-second GPS streams, segments, best efforts), enriches them (cadence, distance splits, type-safe cleaning), stores in SQL, and powers squad-level dashboards (weekly heatmaps, team totals, per-runner volume).

**Scale (today):** 10 runners • ~40 activities/week • ~1M GPS points  
**Built for:** easy growth to 100+ runners with partitioned storage, idempotent upserts, and rate-limit-aware ingestion.

---

## Features
- 🔐 **Multi-user onboarding** (Strava OAuth: access/refresh tokens, athlete ID, expiry)
- ⛓️ **Ingestion** of activities + 1s GPS streams, segments, best efforts (Strava API v3)
- 🧮 **Data Mart**: cadence, distance-based splits, consistent datatypes
- 🧱 **SQL-backed** storage with upserts & incremental syncs
- 📊 **Dashboards**: weekly squad heatmaps, total time/distance, per-runner volume

---

## Architecture (high level)
1. **Helper web app** collects OAuth tokens securely.
2. **Ingestion jobs** pull activities & streams and write to a raw warehouse.
3. **Transforms** build a curated Data Mart (cadence, splits, clean types).
4. **BI layer** (Power BI) connects to SQL for squad analytics.

---

## Quick start
> Minimal local setup to run the code that’s in `src/` and `scripts/` (once added).

1. Create a virtual environment and install deps:
   ```bash
   pip install -r requirements.txt
