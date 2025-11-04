#!/usr/bin/env python
"""
Export Strava activity summaries (last 30 days) → one CSV
--------------------------------------------------------
• Uses hard-coded credentials and token path.
• Fetches /athlete/activities?after=<30 days> for every athlete in D:\BO\friends_tokens.json
• Writes D:\BO\all_activities.csv with:
    activity_id, athlete_id, athlete_name, date, start_time_utc,
    duration_sec, distance_km, elev_gain_m, avg_pace_sec_km, source
"""

import json, time, sys, requests
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# ───────── CONFIG ─────────
CLIENT_ID     = xxx
CLIENT_SECRET = xxx

TOKENS_FILE   = Path(r"D:\BO\friends_tokens.json")
OUTPUT_CSV    = Path(r"D:\BO\all_activities_clean.csv")

DAYS_BACK     = 150
PER_PAGE      = 200
# ──────────────────────────

since_ts = int(time.time() - DAYS_BACK * 86400)


def refresh_token(tok: dict) -> str:
    """Return a valid access_token, refreshing if needed."""
    if time.time() < tok.get("expires_at", 0) - 300:
        return tok["access_token"]

    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": tok["refresh_token"],
        },
        timeout=15,
    ).json()

    tok.update(
        access_token=r["access_token"],
        refresh_token=r["refresh_token"],
        expires_at=r["expires_at"],
    )
    return tok["access_token"]


def fetch_activities(tok: dict) -> list:
    acts, page = [], 1
    headers = {"Authorization": f"Bearer {refresh_token(tok)}"}

    while True:
        resp = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=headers,
            params={"per_page": PER_PAGE, "page": page, "after": since_ts},
            timeout=20,
        )
        data = resp.json()
        if not isinstance(data, list) or not data:
            break
        acts.extend(data)
        page += 1

    return acts


def normalise(acts: list, athlete_id: int, athlete_name: str) -> pd.DataFrame:
    rows = []
    for a in acts:
        dist_m  = a.get("distance", 0) or 0
        elapsed = a.get("elapsed_time", 0) or 0
        rows.append(
            { 
                "activity_id":      a.get("id"),
                "activity_type":   a.get("type"),
                "athlete_id":       athlete_id,
                "athlete_name":     athlete_name,
                "date":             a.get("start_date_local", "")[:10],
                "start_time_utc":   a.get("start_date"),
                "duration_sec":     elapsed,
                "distance_km":      dist_m / 1000,
                "elev_gain_m":      a.get("total_elevation_gain", 0),
                "avg_pace_sec_km":  elapsed / (dist_m / 1000) if dist_m else None,
                "source":           "API",
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    if not TOKENS_FILE.exists():
        sys.exit(f"Token file not found: {TOKENS_FILE}")

    tokens = json.loads(TOKENS_FILE.read_text())
    all_dfs = []

    for aid, meta in tokens.items():
        athlete_id   = int(aid)
        athlete_name = meta.get("name", "")
        print(f"Fetching last {DAYS_BACK} days for {athlete_name} ({athlete_id}) …")

        activities = fetch_activities(meta)
        print(f"  → {len(activities)} activities")

        if activities:
            all_dfs.append(normalise(activities, athlete_id, athlete_name))

    if not all_dfs:
        print("No activities returned — nothing to write.")
        return

    out_df = pd.concat(all_dfs, ignore_index=True)
    out_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n✓ Wrote {len(out_df)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
