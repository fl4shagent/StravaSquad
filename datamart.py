import os
import pandas as pd
import numpy as np
from ast import literal_eval


# === Helper functions ===


def pull_id(val):
    """Extract an 'id' field from a dict-like string or a dict itself."""
    if pd.isna(val):
        return None
    if isinstance(val, str):
        try:
            obj = literal_eval(val)
        except Exception:
            return None
    elif isinstance(val, dict):
        obj = val
    else:
        return None
    return obj.get("id")


def _fix_mojibake(s: str) -> str:
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf8")
    except Exception:
        return s


def haversine_vec(lats, lons):
    """Vectorized haversine distance between consecutive GPS points (meters).
    Replaces geopy.geodesic loop — ~100x faster on large streams."""
    R = 6_371_000
    lat = np.radians(lats)
    lon = np.radians(lons)
    dlat = np.diff(lat)
    dlon = np.diff(lon)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat[:-1]) * np.cos(lat[1:]) * np.sin(dlon / 2) ** 2
    return np.concatenate([[0.0], 2 * R * np.arcsin(np.sqrt(a))])


def existing_ids(path, col="activity_id") -> set:
    """Return set of already-processed IDs from an output CSV (empty set if file missing)."""
    if not os.path.exists(path):
        return set()
    df = pd.read_csv(path, usecols=[col], low_memory=False)
    return set(df[col].dropna().astype(str))


def filter_new(df, done_ids, col="activity_id") -> pd.DataFrame:
    """Keep only rows whose activity_id is not in done_ids.
    Handles both direct activity_id columns and raw 'activity' dict columns."""
    if col in df.columns:
        ids = df[col].astype(str)
    elif "activity" in df.columns:
        ids = df["activity"].apply(pull_id).astype(str)
    else:
        return df.copy()
    return df[~ids.isin(done_ids)].copy()


def append_csv(df, path, label):
    """Append df to an existing CSV, writing header only if the file is new."""
    if df is None or df.empty:
        print(f"  ⏩ {label}: nothing new to append")
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", header=write_header, index=False)
    print(f"  ✅ {label}: appended {len(df)} rows")


# === File paths ===

EXPORTS_DIR = r"D:\BO\strava_api_exports"
BASE_DIR    = r"D:\BO"

runstream_csv        = os.path.join(EXPORTS_DIR, "runstream.csv")
best_csv             = os.path.join(EXPORTS_DIR, "best_profiles.csv")
segments_csv         = os.path.join(EXPORTS_DIR, "segments.csv")
activities_csv       = os.path.join(BASE_DIR,    "all_activities_clean.csv")
athletes_csv         = os.path.join(BASE_DIR,    "athletes_profiles.csv")

out_best_dt          = os.path.join(EXPORTS_DIR, "best_clean_datetime.csv")
out_segments_dt      = os.path.join(EXPORTS_DIR, "segments_clean_datetime.csv")
out_activities_dt    = os.path.join(BASE_DIR,    "all_activities_clean_datetime.csv")
out_segment_stats_dt = os.path.join(EXPORTS_DIR, "runstream_segments_by_kilometers.csv")
out_athletes_dt      = os.path.join(BASE_DIR,    "athletes_profiles_clean.csv")


# === Determine already-processed activity_ids ===
# Use the activities output as the source of truth
done_ids = existing_ids(out_activities_dt)
print(f"📋 {len(done_ids)} activities already processed — loading new ones only...")


# === Load inputs and filter to new activity_ids ===

df_activities   = pd.read_csv(activities_csv, low_memory=False)
df_runstream    = pd.read_csv(runstream_csv,  low_memory=False)
df_best_raw     = pd.read_csv(best_csv,       low_memory=False)
df_segments_raw = pd.read_csv(segments_csv,   low_memory=False)

df_activities_new = filter_new(df_activities,   done_ids)
df_runstream_new  = filter_new(df_runstream,    done_ids)
df_best_new       = filter_new(df_best_raw,     done_ids)
df_segments_new   = filter_new(df_segments_raw, done_ids)

new_count = df_activities_new["activity_id"].nunique() if not df_activities_new.empty else 0

if new_count == 0:
    print("⏩ No new activities — data mart is up to date.")
else:
    print(f"📊 Processing {new_count} new activities...")

    # === 1) Compute metrics per activity_id from runstream ===

    metrics = []
    for aid, grp in df_runstream_new.groupby("activity_id"):
        hr   = grp["hr_bpm"].astype(float).dropna()
        cad  = grp["cadence"].astype(float).dropna()
        elev = grp["alt_m"].astype(float).dropna()
        dist = grp["dist_m"].astype(float)
        tsec = grp["time_s"].astype(float)

        hr_avg,  hr_max  = (hr.mean(),   hr.max())   if not hr.empty   else (np.nan, np.nan)
        cad_avg, cad_max = (cad.mean(),  cad.max())  if not cad.empty  else (np.nan, np.nan)
        elev_min, elev_max = (elev.min(), elev.max()) if not elev.empty else (np.nan, np.nan)

        ascent = descent = np.nan
        if not elev.empty:
            d_alt   = elev.diff().fillna(0)
            ascent  = d_alt[d_alt > 0].sum()
            descent = -d_alt[d_alt < 0].sum()

        total_dist = dist.iat[-1] if len(dist) else 0
        dt         = tsec.diff().fillna(0)
        steps      = (cad * (dt / 60)).sum()
        stride_len = total_dist / (2 * steps) if steps > 0 else np.nan

        metrics.append({
            "activity_id":   aid,
            "hr_avg":        hr_avg,
            "hr_max":        hr_max,
            "cad_avg":       cad_avg,
            "cad_max":       cad_max,
            "elev_min":      elev_min,
            "elev_max":      elev_max,
            "total_ascent":  ascent,
            "total_descent": descent,
            "stride_len_m":  stride_len,
        })

    if metrics:
        df_activities_new = df_activities_new.merge(
            pd.DataFrame(metrics), on="activity_id", how="left"
        )

    # === 2) Clean best and segments ===

    BEST_DROP    = ["resource_state", "start_date_local", "pr_rank", "achievements"]
    SEGMENT_DROP = ["segment", "pr_rank", "achievements", "resource_state", "kom_rank"]

    def clean_best(df):
        df = df.copy()
        df.drop(columns=[c for c in BEST_DROP if c in df], inplace=True, errors="ignore")
        if "activity" in df:
            df["activity_id"] = df["activity"].apply(pull_id).astype("Int64")
            df.drop(columns=["activity"], inplace=True)
        if "athlete" in df:
            df["athlete_id"] = df["athlete"].apply(pull_id).astype("Int64")
            df.drop(columns=["athlete"], inplace=True)
        return df

    def clean_segments(df):
        df = df.copy()
        df.drop(columns=[c for c in SEGMENT_DROP if c in df], inplace=True, errors="ignore")
        if "activity" in df:
            df["activity_id"] = df["activity"].apply(pull_id).astype("Int64")
            df.drop(columns=["activity"], inplace=True)
        if "athlete" in df:
            df["athlete_id"] = df["athlete"].apply(pull_id).astype("Int64")
            df.drop(columns=["athlete"], inplace=True)
        if "name" in df.columns:
            df["name"] = df["name"].apply(_fix_mojibake)
        return df

    df_best_clean     = clean_best(df_best_new)
    df_segments_clean = clean_segments(df_segments_new)

    # === 3) Date parsing ===

    if "start_date" in df_best_clean.columns:
        df_best_clean["start_date"] = (
            pd.to_datetime(df_best_clean["start_date"], utc=True).dt.tz_convert(None)
        )

    for col in ("start_date", "start_date_local"):
        if col in df_segments_clean.columns:
            df_segments_clean[col] = (
                pd.to_datetime(df_segments_clean[col], utc=True).dt.tz_convert(None)
            )

    if "start_time_utc" in df_activities_new.columns:
        df_activities_new["start_time_utc"] = (
            pd.to_datetime(df_activities_new["start_time_utc"], utc=True).dt.tz_convert(None)
        )

    # === 4) Km segmentation — vectorized haversine (replaces geopy loop) ===

    seg_chunks = []
    for aid, grp in df_runstream_new.groupby("activity_id"):
        grp = grp.reset_index(drop=True)
        grp["segment_distance"]    = haversine_vec(grp["lat"].values, grp["lon"].values)
        grp["cumulative_distance"] = grp["segment_distance"].cumsum()
        grp["segment_number"]      = (grp["cumulative_distance"] // 1000 + 1).astype(int)
        seg_chunks.append(grp)

    if seg_chunks:
        df_segs = pd.concat(seg_chunks, ignore_index=True)
        segment_stats = df_segs.groupby(["activity_id", "segment_number"]).agg(
            segment_start_time   =("time_s",              "min"),
            segment_end_time     =("time_s",              "max"),
            start_lat            =("lat",                 "first"),
            start_lon            =("lon",                 "first"),
            cumulative_distance_m=("cumulative_distance", "last"),
            avg_hr_bpm           =("hr_bpm",              "mean"),
            avg_cadence          =("cadence",             "mean"),
            avg_watts            =("watts",               "mean"),
            segment_distance_m   =("segment_distance",    "sum"),
        ).reset_index()

        segment_stats["duration_s"] = (
            segment_stats["segment_end_time"] - segment_stats["segment_start_time"]
        )
        segment_stats["pace_sec_per_km"] = (
            segment_stats["duration_s"] / (segment_stats["segment_distance_m"] / 1000)
        ).replace(np.inf, np.nan)
    else:
        segment_stats = pd.DataFrame()

    # === 5) Append new rows to existing output CSVs ===

    append_csv(df_best_clean,     out_best_dt,          "RunBest")
    append_csv(df_segments_clean, out_segments_dt,      "RunSegment")
    append_csv(df_activities_new, out_activities_dt,    "Activities")
    append_csv(segment_stats,     out_segment_stats_dt, "RunSplitKilometer")

    print(f"\n✅ Data mart updated with {new_count} new activities.")


# === Athletes: always reprocess (small table, profiles can change) ===

ATHLETE_DROP = [
    "badge_type_id", "bio", "created_at", "follower", "friend", "id", "premium",
    "profile", "profile_medium", "resource_state", "state", "summit", "updated_at", "username"
]

df_athletes = pd.read_csv(athletes_csv, low_memory=False)
df_athletes.drop(columns=[c for c in ATHLETE_DROP if c in df_athletes], inplace=True, errors="ignore")
df_athletes.to_csv(out_athletes_dt, index=False)
print("  ✅ Athletes: refreshed (always reprocessed)")
