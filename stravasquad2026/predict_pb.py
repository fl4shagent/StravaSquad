"""
Step 5 — PB trend prediction + race-time forecast backtest.

Part A: per athlete/distance_label linear trend on RunBest.elapsed_time -> gold.PBPrediction
Part B: backtest 3 race-time forecast formulas (Riegel, VDOT, elevation-adjusted Riegel)
        against two real squad races, computed at several "as-of" checkpoints before
        race day -> gold.RaceForecast

Run with: "D:\\BO\\New folder\\python.exe" stravasquad2026\\predict_pb.py
"""
import datetime as dt

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from sqlalchemy import create_engine, text

# ───────── CONFIG ─────────
DB_NAME = "StravaDW"
SERVER = "HANG"
DRIVER = "ODBC Driver 17 for SQL Server"

engine = create_engine(
    f"mssql+pyodbc://@{SERVER}/{DB_NAME}"
    f"?driver={DRIVER.replace(' ', '+')}"
    "&Trusted_Connection=yes",
    fast_executemany=True,
    future=True,
    connect_args={"timeout": 30},
)

DISTANCE_LABEL_FILE = r"D:\BO\stravasquad2026\dbt\seeds\dim_distance_label.csv"

# Penalty applied to flat-pace (sec/km) per 1 m/km of net elevation gain.
# Approximate, documented constant — not a calibrated grade-adjusted-pace model.
ELEVATION_FACTOR_SEC_PER_M_PER_KM = 2.0

# Distances preferred as the "source" PB for forecasting, in priority order.
# Excludes Half-Marathon and Marathon (the usual targets) to avoid circularity.
SOURCE_DISTANCE_PRIORITY = ["10K", "15K", "5K", "20K", "10 mile", "1 mile", "30K"]

AS_OF_WEEKS_BEFORE = [8, 4, 2, 1]

# Real squad races to backtest against.
TARGET_RACES = [
    {
        "target_distance_label": "Half-Marathon",
        "race_date": dt.date(2025, 10, 26),
        "athletes": {
            6091734: {"activity_id": 16255637938, "actual_sec": 6421, "elev_gain_m": 21.0, "distance_km": 21.3808},
            36978778: {"activity_id": 16255837334, "actual_sec": 8486, "elev_gain_m": 27.0, "distance_km": 21.6179},
            38095302: {"activity_id": 16256651852, "actual_sec": 11795, "elev_gain_m": 74.0, "distance_km": 21.3942},
            94062156: {"activity_id": 16256197542, "actual_sec": 12343, "elev_gain_m": 32.9, "distance_km": 22.4243},
            94062196: {"activity_id": 16255807860, "actual_sec": 8232, "elev_gain_m": 16.0, "distance_km": 21.4139},
            169950101: {"activity_id": 16255845381, "actual_sec": 8378, "elev_gain_m": 16.0, "distance_km": 21.3830},
        },
    },
    {
        "target_distance_label": "Marathon",
        "race_date": dt.date(2025, 10, 5),
        "athletes": {
            36978778: {"activity_id": 16036389587, "actual_sec": 19083, "elev_gain_m": 446.0, "distance_km": 42.7205},
        },
    },
]
# ──────────────────────────


def load_distance_labels():
    df = pd.read_csv(DISTANCE_LABEL_FILE)
    return dict(zip(df["distance_label"], df["standard_distance_m"]))


# ───────── Part A: PB trend regression ─────────

def compute_pb_predictions(run_best):
    today = dt.date.today()
    today_ordinal = today.toordinal()
    rows = []

    for (athlete_id, distance_label), grp in run_best.groupby(["athlete_id", "name"]):
        grp = grp.dropna(subset=["start_date", "elapsed_time"]).sort_values("start_date")
        if len(grp) < 3:
            continue

        x = grp["start_date"].apply(lambda d: d.toordinal()).to_numpy(dtype=float)
        y = grp["elapsed_time"].to_numpy(dtype=float)

        slope, intercept = np.polyfit(x, y, 1)
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y.mean()) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        predicted_best_sec = slope * today_ordinal + intercept
        predicted_best_sec = max(predicted_best_sec, 1.0)

        rows.append({
            "athlete_id": int(athlete_id),
            "distance_label": distance_label,
            "predicted_best_sec": float(predicted_best_sec),
            "prediction_date": today,
            "r_squared": float(r_squared),
        })

    return pd.DataFrame(rows)


# ───────── Part B: race-time forecast formulas ─────────

def riegel(t1_sec, d1_m, d2_m, exponent=1.06):
    return t1_sec * (d2_m / d1_m) ** exponent


def _vo2_cost(velocity_m_per_min):
    return -4.60 + 0.182258 * velocity_m_per_min + 0.000104 * velocity_m_per_min ** 2


def _vo2_pct(time_min):
    return 0.8 + 0.1894393 * np.exp(-0.012778 * time_min) + 0.2989558 * np.exp(-0.1932605 * time_min)


def compute_vdot(distance_m, time_sec):
    time_min = time_sec / 60.0
    velocity = distance_m / time_min
    return _vo2_cost(velocity) / _vo2_pct(time_min)


def vdot_time(vdot, distance_m):
    """Invert compute_vdot for a target distance: find time_sec such that
    compute_vdot(distance_m, time_sec) == vdot."""
    def f(time_min):
        velocity = distance_m / time_min
        return _vo2_cost(velocity) / _vo2_pct(time_min) - vdot

    time_min = brentq(f, 1.0, 600.0)
    return time_min * 60.0


def elevation_adjusted_riegel(t1_sec, d1_m, source_elev_per_km, d2_m, target_elev_per_km):
    source_pace_sec_km = t1_sec / (d1_m / 1000.0)
    flat_pace = source_pace_sec_km - ELEVATION_FACTOR_SEC_PER_M_PER_KM * source_elev_per_km
    flat_t1 = flat_pace * (d1_m / 1000.0)

    flat_t2 = riegel(flat_t1, d1_m, d2_m)
    flat_pace2 = flat_t2 / (d2_m / 1000.0)

    target_pace2 = flat_pace2 + ELEVATION_FACTOR_SEC_PER_M_PER_KM * target_elev_per_km
    return target_pace2 * (d2_m / 1000.0)


def find_source_pb(run_best_with_elev, athlete_id, as_of_date, distance_labels):
    """Fastest qualifying PB for this athlete before as_of_date.

    Improvement 1 — activity-distance filter: only accept a RunBest record if the
    parent activity's total distance is within 90%-120% of the PB standard distance.
    This filters out mid-long-run splits (e.g. a 4180s 10K recorded inside a 16.8 km run).

    Improvement 2 — fastest, not most recent: within each candidate pool use
    MIN(elapsed_time) instead of most recent start_date, to select the best genuine
    race effort rather than the latest (possibly casual) one.

    Pool cascade (tries best option first, falls back gracefully):
      1. 90-day window + activity-distance filter
      2. All-time       + activity-distance filter
      3. 90-day window  (no filter — athlete hasn't run standalone races)
      4. All-time       (no filter — legacy behaviour, last resort)
    """
    candidates = run_best_with_elev[
        (run_best_with_elev["athlete_id"] == athlete_id)
        & (run_best_with_elev["start_date"].dt.date < as_of_date)
        & (run_best_with_elev["name"].isin(SOURCE_DISTANCE_PRIORITY))
    ]
    if candidates.empty:
        return None

    cutoff_90d = as_of_date - dt.timedelta(days=90)
    recent = candidates[candidates["start_date"].dt.date >= cutoff_90d]

    def apply_dist_filter(df, label):
        std_m = distance_labels.get(label)
        if not std_m or df.empty:
            return df
        return df[
            df["activity_distance_km"].notna()
            & (df["activity_distance_km"] * 1000 >= 0.9 * std_m)
            & (df["activity_distance_km"] * 1000 <= 1.2 * std_m)
        ]

    for label in SOURCE_DISTANCE_PRIORITY:
        for pool, use_filter in [
            (recent,      True),
            (candidates,  True),
            (recent,      False),
            (candidates,  False),
        ]:
            rows = pool[pool["name"] == label]
            if use_filter:
                rows = apply_dist_filter(rows, label)
            if not rows.empty:
                return rows.nsmallest(1, "elapsed_time").iloc[0]

    return None


def compute_race_forecasts(run_best_with_elev, distance_labels, pb_predictions):
    """Improvement 3 — trend blending: for athletes with r² >= 0.5 on the source
    distance, blend the formula prediction 50/50 with a Riegel-scaled version of
    their trend-projected source PB. This partially corrects for athletes on steep
    improvement curves whose most-recent-PB understates actual race-day fitness.
    (pb_predictions is computed over all history projected to today — a valid
    approximation for real-time use; for backtesting it slightly overstates the
    trend knowledge available at as_of_date.)
    """
    rows = []

    for race in TARGET_RACES:
        target_label = race["target_distance_label"]
        target_distance_m = distance_labels[target_label]
        race_date = race["race_date"]

        for athlete_id, race_info in race["athletes"].items():
            target_elev_per_km = race_info["elev_gain_m"] / race_info["distance_km"]

            for weeks_before in AS_OF_WEEKS_BEFORE:
                as_of_date = race_date - dt.timedelta(weeks=weeks_before)
                source = find_source_pb(run_best_with_elev, athlete_id, as_of_date, distance_labels)
                if source is None:
                    continue

                source_label = source["name"]
                source_distance_m = distance_labels[source_label]
                source_pb_sec = float(source["elapsed_time"])
                source_elev_per_km = source["elev_per_km"]
                if pd.isna(source_elev_per_km):
                    source_elev_per_km = 0.0

                predictions = {
                    "riegel": riegel(source_pb_sec, source_distance_m, target_distance_m),
                    "vdot": vdot_time(compute_vdot(source_distance_m, source_pb_sec), target_distance_m),
                    "elevation_adjusted": elevation_adjusted_riegel(
                        source_pb_sec, source_distance_m, source_elev_per_km,
                        target_distance_m, target_elev_per_km,
                    ),
                }

                # Improvement 3: blend with trend projection for high-r² athletes.
                # Guard: skip if trend_source_sec <= 120 — catches rows where the
                # linear extrapolation went negative and was clamped to 1.0 sec in
                # compute_pb_predictions(). A plausible PB for any source distance
                # is always well above 2 minutes.
                trend_match = pb_predictions[
                    (pb_predictions["athlete_id"] == athlete_id)
                    & (pb_predictions["distance_label"] == source_label)
                ]
                if not trend_match.empty and trend_match.iloc[0]["r_squared"] >= 0.5:
                    trend_source_sec = float(trend_match.iloc[0]["predicted_best_sec"])
                    if trend_source_sec > 120:
                        trend_target_sec = riegel(trend_source_sec, source_distance_m, target_distance_m)
                        predictions = {k: 0.5 * v + 0.5 * trend_target_sec for k, v in predictions.items()}

                actual_sec = float(race_info["actual_sec"])
                for method, predicted_sec in predictions.items():
                    error_sec = predicted_sec - actual_sec
                    rows.append({
                        "athlete_id": int(athlete_id),
                        "race_activity_id": int(race_info["activity_id"]),
                        "target_distance_label": target_label,
                        "method": method,
                        "as_of_date": as_of_date,
                        "source_distance_label": source_label,
                        "source_pb_sec": source_pb_sec,
                        "predicted_sec": float(predicted_sec),
                        "actual_sec": actual_sec,
                        "error_sec": float(error_sec),
                        "error_pct": float(error_sec / actual_sec * 100.0),
                    })

    return pd.DataFrame(rows)


# ───────── Load + write ─────────

def load_run_best_with_elev(conn):
    run_best = pd.read_sql(
        text("SELECT id, name, elapsed_time, moving_time, start_date, distance, activity_id, athlete_id "
             "FROM silver.RunBest"),
        conn,
        parse_dates=["start_date"],
    )
    activities = pd.read_sql(
        text("SELECT activity_id, distance_km, elev_gain_m FROM silver.Activities"),
        conn,
    )
    activities["elev_per_km"] = activities["elev_gain_m"] / activities["distance_km"]

    merged = run_best.merge(
        activities[["activity_id", "elev_per_km", "distance_km"]].rename(
            columns={"distance_km": "activity_distance_km"}
        ),
        on="activity_id", how="left"
    )
    return merged


def write_table(conn, df, schema, table, key_cols):
    if df.empty:
        print(f"-- {schema}.{table}: nothing to write")
        return

    for _, group_keys in df[key_cols].drop_duplicates().iterrows():
        conditions = " AND ".join(
            f"[{col}] = '{val}'" if isinstance(val, (str, dt.date)) else f"[{col}] = {val}"
            for col, val in group_keys.items()
        )
        conn.execute(text(f"DELETE FROM [{schema}].[{table}] WHERE {conditions}"))

    df.to_sql(name=table, schema=schema, con=conn, if_exists="append", index=False, chunksize=1000)
    print(f"OK {schema}.{table}: wrote {len(df)} rows")


def main():
    distance_labels = load_distance_labels()

    with engine.begin() as conn:
        run_best_with_elev = load_run_best_with_elev(conn)

        pb_predictions = compute_pb_predictions(run_best_with_elev)
        write_table(conn, pb_predictions, "gold", "PBPrediction", ["athlete_id", "distance_label"])

        race_forecasts = compute_race_forecasts(run_best_with_elev, distance_labels, pb_predictions)
        write_table(conn, race_forecasts, "gold", "RaceForecast", ["athlete_id", "target_distance_label", "method"])

    print("\npredict_pb.py complete!")


if __name__ == "__main__":
    main()
