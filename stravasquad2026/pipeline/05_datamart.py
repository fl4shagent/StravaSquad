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
    return obj.get('id')


def _fix_mojibake(s: str) -> str:
    """Fix mojibake by re-encoding if necessary."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode('latin1').decode('utf8')
    except Exception:
        return s


def haversine_m(lat1, lon1, lat2, lon2):
    """Vectorized haversine distance in meters."""
    R = 6_371_000.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    a = (np.sin(np.radians(lat2 - lat1) / 2) ** 2
         + np.cos(phi1) * np.cos(phi2)
         * np.sin(np.radians(lon2 - lon1) / 2) ** 2)
    return 2 * R * np.arcsin(np.sqrt(a))


# === File paths ===


EXPORTS_DIR = r"D:\BO\stravasquad2026\data\strava_api_exports"
BASE_DIR    = r"D:\BO\stravasquad2026\data"

runstream_csv        = os.path.join(EXPORTS_DIR, "runstream.csv")
best_csv             = os.path.join(EXPORTS_DIR, "best_profiles.csv")
segments_csv         = os.path.join(EXPORTS_DIR, "segments.csv")
activities_csv       = os.path.join(BASE_DIR,    "all_activities_clean.csv")
athletes_csv         = os.path.join(BASE_DIR,    "athletes_profiles.csv")

out_best_dt          = os.path.join(EXPORTS_DIR, "best_clean_datetime.csv")
out_segments_dt      = os.path.join(EXPORTS_DIR, "segments_clean_datetime.csv")
out_activities_dt    = os.path.join(BASE_DIR,     "all_activities_clean_datetime.csv")
out_segment_stats_dt = os.path.join(EXPORTS_DIR,  "runstream_segments_by_kilometers.csv")


# === Activity ID Checkpoint ===


existing_ids = set()
if os.path.exists(out_activities_dt):
    existing_ids = set(
        pd.read_csv(out_activities_dt, usecols=["activity_id"], low_memory=False)
          ["activity_id"].dropna().astype(str)
    )


# === Load all input DataFrames ===


df_runstream    = pd.read_csv(runstream_csv,   low_memory=False)
df_best_raw     = pd.read_csv(best_csv,        low_memory=False)
df_segments_raw = pd.read_csv(segments_csv,    low_memory=False)
df_activities   = pd.read_csv(activities_csv,  low_memory=False)
df_athletes     = pd.read_csv(athletes_csv,    low_memory=False)


# === Filter to new activity_ids only ===


df_activities["activity_id"] = df_activities["activity_id"].astype(str)
new_ids = set(df_activities["activity_id"]) - existing_ids

if not new_ids:
    print("No new activities to process.")
else:
    print(f"Processing {len(new_ids)} new activity_id(s)...")

    df_runstream["activity_id"] = df_runstream["activity_id"].astype(str)
    df_runstream = df_runstream[df_runstream["activity_id"].isin(new_ids)]

    # Extract activity_id for filtering (nested 'activity' dict column in best/segments)
    df_best_raw["_aid"]     = df_best_raw["activity"].apply(pull_id).astype("Int64").astype(str)
    df_segments_raw["_aid"] = df_segments_raw["activity"].apply(pull_id).astype("Int64").astype(str)
    df_best_raw     = df_best_raw[df_best_raw["_aid"].isin(new_ids)].drop(columns=["_aid"])
    df_segments_raw = df_segments_raw[df_segments_raw["_aid"].isin(new_ids)].drop(columns=["_aid"])
    df_activities   = df_activities[df_activities["activity_id"].isin(new_ids)]
    # df_athletes: always fully reprocessed (small table, profiles can change)


    # === 1) Compute metrics per activity_id ===


    metrics = []
    for aid, grp in df_runstream.groupby("activity_id"):
        hr   = grp["hr_bpm"].astype(float).dropna()
        cad  = grp["cadence"].astype(float).dropna()
        elev = grp["alt_m"].astype(float).dropna()
        dist = grp["dist_m"].astype(float)
        tsec = grp["time_s"].astype(float)

        hr_avg, hr_max     = (hr.mean(),   hr.max())   if not hr.empty   else (np.nan, np.nan)
        cad_avg, cad_max   = (cad.mean(),  cad.max())  if not cad.empty  else (np.nan, np.nan)
        elev_min, elev_max = (elev.min(),  elev.max()) if not elev.empty else (np.nan, np.nan)

        ascent = descent = np.nan
        if not elev.empty:
            d_alt   = elev.diff().fillna(0)
            ascent  = d_alt[d_alt > 0].sum()
            descent = -d_alt[d_alt < 0].sum()

        total_dist = dist.iat[-1] if len(dist) else 0
        dt = tsec.diff().fillna(0)
        steps = (cad * (dt / 60)).sum()
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

    df_metrics    = pd.DataFrame(metrics)
    df_activities = df_activities.merge(df_metrics, on="activity_id", how="left")


    # === 2) Clean/reformat best and segments ===


    BEST_DROP    = ["resource_state", "start_date_local", "pr_rank", "achievements"]
    SEGMENT_DROP = ["segment", "pr_rank", "achievements", "resource_state"]

    def clean_best(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.drop(columns=[c for c in BEST_DROP if c in df], inplace=True, errors="ignore")
        if "activity" in df:
            df["activity_id"] = df["activity"].apply(pull_id).astype("Int64")
            df.drop(columns=["activity"], inplace=True)
        if "athlete" in df:
            df["athlete_id"] = df["athlete"].apply(pull_id).astype("Int64")
            df.drop(columns=["athlete"], inplace=True)
        return df

    def clean_segments(df: pd.DataFrame) -> pd.DataFrame:
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

    df_best_clean     = clean_best(df_best_raw)
    df_segments_clean = clean_segments(df_segments_raw)


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

    if "start_time_utc" in df_activities.columns:
        df_activities["start_time_utc"] = (
            pd.to_datetime(df_activities["start_time_utc"], utc=True).dt.tz_convert(None)
        )


    # === 4) Km segmentation - vectorized haversine ===


    df_runstream = df_runstream.sort_values(["activity_id", "time_s"]).reset_index(drop=True)

    lat1 = df_runstream["lat"].shift(1).values
    lon1 = df_runstream["lon"].shift(1).values
    lat2 = df_runstream["lat"].values
    lon2 = df_runstream["lon"].values

    seg_dist = haversine_m(lat1, lon1, lat2, lon2)
    boundary = (df_runstream["activity_id"] != df_runstream["activity_id"].shift(1)).values
    seg_dist[boundary] = 0.0
    df_runstream["segment_distance"] = np.nan_to_num(seg_dist, nan=0.0)

    def calculate_segments(group):
        cum_dist = group["segment_distance"].cumsum()
        segments = (cum_dist // 1000 + 1).astype(int)
        return pd.DataFrame({"cumulative_distance": cum_dist, "segment_number": segments})

    seg_data = df_runstream.groupby("activity_id", group_keys=False).apply(calculate_segments)
    df_runstream[["cumulative_distance", "segment_number"]] = seg_data

    segment_stats = df_runstream.groupby(["activity_id", "segment_number"]).agg({
        "time_s":              ["min", "max"],
        "lat":                 "first",
        "lon":                 "first",
        "cumulative_distance": "last",
        "hr_bpm":              "mean",
        "cadence":             "mean",
        "watts":               "mean",
        "segment_distance":    "sum",
    }).reset_index()

    segment_stats.columns = [
        "activity_id", "segment_number",
        "segment_start_time", "segment_end_time",
        "start_lat", "start_lon",
        "cumulative_distance_m",
        "avg_hr_bpm", "avg_cadence", "avg_watts",
        "segment_distance_m",
    ]
    segment_stats["duration_s"] = (
        segment_stats["segment_end_time"] - segment_stats["segment_start_time"]
    )
    segment_stats["pace_sec_per_km"] = (
        segment_stats["duration_s"] / (segment_stats["segment_distance_m"] / 1000)
    ).replace(np.inf, np.nan)


    # === 5) Append to existing output CSVs ===


    def append_csv(new_df: pd.DataFrame, path: str, label: str) -> None:
        if new_df.empty:
            return
        if os.path.exists(path):
            existing = pd.read_csv(path, low_memory=False)
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df
        combined.to_csv(path, index=False)
        print(f"{label}: +{len(new_df)} rows -> {len(combined)} total")

    append_csv(df_best_clean,     out_best_dt,          "best_clean_datetime.csv")
    append_csv(df_segments_clean, out_segments_dt,       "segments_clean_datetime.csv")
    append_csv(df_activities,     out_activities_dt,     "all_activities_clean_datetime.csv")
    append_csv(segment_stats,     out_segment_stats_dt,  "runstream_segments_by_kilometers.csv")
