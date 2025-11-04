import os
import pandas as pd
import numpy as np
from ast import literal_eval
from geopy.distance import geodesic


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


# === File paths ===


EXPORTS_DIR = r"D:\BO\strava_api_exports"
BASE_DIR    = r"D:\BO"


# Input CSVs
runstream_csv   = os.path.join(EXPORTS_DIR, "runstream.csv")
best_csv        = os.path.join(EXPORTS_DIR, "best_profiles.csv")
segments_csv    = os.path.join(EXPORTS_DIR, "segments.csv")
activities_csv  = os.path.join(BASE_DIR,    "all_activities_clean.csv")
athletes_csv    = os.path.join(BASE_DIR,    "athletes_profiles.csv")


# === Load DataFrames ===


df_runstream    = pd.read_csv(runstream_csv,   low_memory=False)
df_best_raw     = pd.read_csv(best_csv,        low_memory=False)
df_segments_raw = pd.read_csv(segments_csv,    low_memory=False)
df_activities   = pd.read_csv(activities_csv,  low_memory=False)
df_athletes     = pd.read_csv(athletes_csv,    low_memory=False)


# === 1) Compute metrics per activity_id ===


metrics = []
for aid, grp in df_runstream.groupby("activity_id"):
    hr   = grp["hr_bpm"].astype(float).dropna()
    cad  = grp["cadence"].astype(float).dropna()
    elev = grp["alt_m"].astype(float).dropna()
    dist = grp["dist_m"].astype(float)
    tsec = grp["time_s"].astype(float)


    hr_avg, hr_max = (hr.mean(), hr.max()) if not hr.empty else (np.nan, np.nan)
    cad_avg, cad_max = (cad.mean(), cad.max()) if not cad.empty else (np.nan, np.nan)
    elev_min, elev_max = (elev.min(), elev.max()) if not elev.empty else (np.nan, np.nan)


    ascent = descent = np.nan
    if not elev.empty:
        d_alt = elev.diff().fillna(0)
        ascent  = d_alt[d_alt>0].sum()
        descent = -d_alt[d_alt<0].sum()


    total_dist = dist.iat[-1] if len(dist) else 0
    dt = tsec.diff().fillna(0)
    steps = (cad * (dt/60)).sum()
    stride_len = total_dist / (2*steps) if steps > 0 else np.nan


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
        "stride_len_m":  stride_len
    })


df_metrics = pd.DataFrame(metrics)


# merge metrics into activities
df_activities = df_activities.merge(df_metrics, on="activity_id", how="left")


# === 2) Clean/Reformat best_profiles and segments ===


BEST_DROP    = ["resource_state", "start_date_local", "pr_rank", "achievements"]
SEGMENT_DROP = ["segment", "pr_rank", "achievements", "resource_state"]
ATHLETE_DROP = ["badge_type_id", "bio", "created_at", "follower", "friend", "id", "premium", "profile", "profile_medium",
                "resource_state", "state", "summit", "updated_at", "username"]

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

def clean_athletes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.drop(columns=[c for c in ATHLETE_DROP if c in df], inplace=True, errors="ignore")
    return df 


df_best_clean     = clean_best(df_best_raw)
df_segments_clean = clean_segments(df_segments_raw)
df_athletes_clean = clean_athletes(df_athletes)


# === 3) Date parsing ===


# best_clean: parse start_date
if "start_date" in df_best_clean.columns:
    df_best_clean["start_date"] = (
        pd.to_datetime(df_best_clean["start_date"], utc=True)
          .dt.tz_convert(None)
    )


# segments_clean: parse start_date and start_date_local
for col in ("start_date", "start_date_local"):
    if col in df_segments_clean.columns:
        df_segments_clean[col] = (
            pd.to_datetime(df_segments_clean[col], utc=True)
              .dt.tz_convert(None)
        )


# activities: parse start_time_utc
if "start_time_utc" in df_activities.columns:
    df_activities["start_time_utc"] = (
        pd.to_datetime(df_activities["start_time_utc"], utc=True)
          .dt.tz_convert(None)
    )


# === 4.Split by kilometers === 

# === Kilometer Segmentation with Aggregation ===

# --- Step 1: Calculate point-to-point distances ---
df_runstream['segment_distance'] = 0.0  # Initialize with 0

# Vectorized distance calculation (faster than loop)
def calculate_distances(group):
    lats = group['lat'].values
    lons = group['lon'].values
    distances = [0.0] + [
        geodesic((lats[i-1], lons[i-1]), (lats[i], lons[i])).meters 
        for i in range(1, len(lats))
    ]
    return distances

distances = df_runstream.groupby('activity_id', group_keys=False).apply(calculate_distances)
df_runstream['segment_distance'] = np.concatenate(distances.values)

# --- Step 2: Compute cumulative distance & segment numbers ---
def calculate_segments(group):
    cum_dist = group['segment_distance'].cumsum()
    segments = (cum_dist // 1000 + 1).astype(int)
    return pd.DataFrame({
        'cumulative_distance': cum_dist,
        'segment_number': segments
    })

seg_data = df_runstream.groupby('activity_id', group_keys=False).apply(calculate_segments)
df_runstream[['cumulative_distance', 'segment_number']] = seg_data

# --- Step 3: Aggregate by segment ---
segment_stats = df_runstream.groupby(['activity_id', 'segment_number']).agg({
    'time_s': ['min', 'max'],
    'lat': 'first',
    'lon': 'first',
    'cumulative_distance': 'last',
    'hr_bpm': 'mean',
    'cadence': 'mean',
    'watts': 'mean',
    'segment_distance': 'sum'
}).reset_index()

# Flatten multi-index columns
segment_stats.columns = [
    'activity_id', 'segment_number',
    'segment_start_time', 'segment_end_time',
    'start_lat', 'start_lon',
    'cumulative_distance_m',
    'avg_hr_bpm', 'avg_cadence', 'avg_watts',
    'segment_distance_m'
]

# --- Step 4: Calculate segment pace ---
segment_stats['duration_s'] = (
    segment_stats['segment_end_time'] - segment_stats['segment_start_time']
)
segment_stats['pace_sec_per_km'] = (
    segment_stats['duration_s'] / 
    (segment_stats['segment_distance_m'] / 1000)
).replace(np.inf, np.nan)

# === 4) Export to new files ===


out_best_dt     = os.path.join(EXPORTS_DIR, "best_clean_datetime.csv")
out_segments_dt = os.path.join(EXPORTS_DIR, "segments_clean_datetime.csv")
out_activities_dt = os.path.join(BASE_DIR,  "all_activities_clean_datetime.csv")
out_segment_stats_dt = os.path.join(EXPORTS_DIR, "runstream_segments_by_kilometers.csv")
out_athletes_dt = os.path.join(BASE_DIR, "athletes_profiles_clean.csv")

df_best_clean.to_csv(out_best_dt,       index=False)
df_segments_clean.to_csv(out_segments_dt, index=False)
df_activities.to_csv(out_activities_dt,  index=False)
segment_stats.to_csv(out_segment_stats_dt, index=False)
df_athletes_clean.to_csv(out_athletes_dt, index = False)



print("✅ Date-converted files exported:")
print(" •", out_best_dt)
print(" •", out_segments_dt)
print(" •", out_activities_dt)
print(" •", out_segment_stats_dt)
print(" •", out_athletes_dt)


