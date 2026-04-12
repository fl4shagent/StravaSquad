import pandas as pd
from sqlalchemy import create_engine, text

# ───────── CONFIG ─────────
DB_NAME = "StravaProject"
SERVER  = "localhost"
DRIVER  = "ODBC Driver 17 for SQL Server"

engine = create_engine(
    f"mssql+pyodbc://@{SERVER}/{DB_NAME}"
    f"?driver={DRIVER.replace(' ', '+')}"
    "&Trusted_Connection=yes",
    fast_executemany=True,
    future=True,
    connect_args={"timeout": 30},
)
# ──────────────────────────

# (csv_path, table, mode, key_col, utf8)
#
# mode = "insert_only" → skip rows whose key_col already exists in the table
#                        (safe for immutable data: GPS streams, best efforts, splits)
# mode = "upsert"      → delete rows for all key_col values in the CSV, then re-insert
#                        (needed for mutable data: activity metadata, athlete profiles)
FILES = [
    (r"D:\BO\strava_api_exports\best_clean_datetime.csv",              "RunBest",           "insert_only", "activity_id", False),
    (r"D:\BO\strava_api_exports\runstream.csv",                        "RunStream",         "insert_only", "activity_id", False),
    (r"D:\BO\strava_api_exports\segments_clean_datetime.csv",          "RunSegment",        "insert_only", "activity_id", True),
    (r"D:\BO\all_activities_clean_datetime.csv",                       "Activities",        "upsert",      "activity_id", False),
    (r"D:\BO\athletes_profiles_clean.csv",                             "Athlete",           "upsert",      "athlete_id",  True),
    (r"D:\BO\strava_api_exports\runstream_segments_by_kilometers.csv", "RunSplitKilometer", "insert_only", "activity_id", False),
]


def _delete_keys(conn, table, key_col, keys, batch=500):
    """Delete rows in batches where key_col is in keys (numeric IDs only)."""
    safe = [str(int(float(k))) for k in keys]
    for i in range(0, len(safe), batch):
        placeholders = ",".join(safe[i : i + batch])
        conn.execute(text(f"DELETE FROM [{table}] WHERE [{key_col}] IN ({placeholders})"))


def load_insert_only(conn, df, table, key_col):
    """Insert only rows whose key_col is not already present in the table."""
    existing = pd.read_sql(f"SELECT DISTINCT [{key_col}] FROM [{table}]", conn)
    existing_ids = set(existing[key_col].astype(str))

    new_df = df[~df[key_col].astype(str).isin(existing_ids)]
    skipped = len(df) - len(new_df)

    if new_df.empty:
        print(f"  ⏩ `{table}`: {skipped} rows already loaded, nothing to insert")
        return 0

    new_df.to_sql(table, conn, if_exists="append", index=False, chunksize=1000)
    print(f"  ✅ `{table}`: inserted {len(new_df)} new rows (skipped {skipped} existing)")
    return len(new_df)


def load_upsert(conn, df, table, key_col):
    """Delete all rows for keys present in df, then re-insert."""
    keys = df[key_col].dropna().unique().tolist()
    _delete_keys(conn, table, key_col, keys)
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=1000)
    print(f"  ✅ `{table}`: upserted {len(df)} rows across {len(keys)} {key_col}s")
    return len(df)


with engine.begin() as conn:
    for path, table, mode, key_col, use_utf8 in FILES:
        print(f"\n📂 {table}")
        df = pd.read_csv(path, encoding="utf-8" if use_utf8 else None, low_memory=False)

        if mode == "insert_only":
            load_insert_only(conn, df, table, key_col)
        elif mode == "upsert":
            load_upsert(conn, df, table, key_col)

print("\n🏁 Incremental load complete!")
