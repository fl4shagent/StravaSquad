import pandas as pd
from sqlalchemy import create_engine, text

# ───────── CONFIG ─────────
DB_NAME = "StravaDW"
SERVER  = "HANG"
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

DATA_DIR    = r"D:\BO\stravasquad2026\data"
EXPORTS_DIR = r"D:\BO\stravasquad2026\data\strava_api_exports"

# (csv_path, table, use_utf8, strategy, key_col, drop_cols_for_silver, silver_pk_cols)
# strategy "insert_only": skip rows whose key_col already exists in the table
# strategy "upsert":      delete touched rows, re-insert from CSV (handles updates)
# silver_pk_cols: dedupe on these columns before loading silver (silver has a PK constraint)
files = [
    (rf"{EXPORTS_DIR}\best_clean_datetime.csv",              "RunBest",           False, "insert_only", "activity_id", ["start_index", "end_index"], ["id"]),
    (rf"{EXPORTS_DIR}\runstream.csv",                        "RunStream",         False, "insert_only", "activity_id", [], ["activity_id", "time_s"]),
    (rf"{EXPORTS_DIR}\segments_clean_datetime.csv",          "RunSegment",        True,  "insert_only", "activity_id", ["device_watts", "hidden", "visibility", "kom_rank"], ["id"]),
    (rf"{DATA_DIR}\athletes_profiles_clean.csv",             "Athlete",           True,  "upsert",      "athlete_id",  ["id"], ["athlete_id"]),
    (rf"{DATA_DIR}\all_activities_clean_datetime.csv",       "Activities",        False, "upsert",      "activity_id", [], ["activity_id"]),
    (rf"{EXPORTS_DIR}\runstream_segments_by_kilometers.csv", "RunSplitKilometer", False, "insert_only", "activity_id", [], ["activity_id", "segment_number"]),
]


def load(conn, df, schema, table, strategy, key_col):
    full_name = f"[{schema}].[{table}]"

    if strategy == "insert_only":
        try:
            result = conn.execute(text(f"SELECT DISTINCT [{key_col}] FROM {full_name}"))
            existing = {str(r[0]) for r in result.fetchall()}
        except Exception:
            existing = set()

        new_df = df[~df[key_col].astype(str).isin(existing)]
        if new_df.empty:
            print(f"-- `{full_name}`: nothing new")
            return

        new_df.to_sql(name=table, schema=schema, con=conn, if_exists="append", index=False, chunksize=1000)
        print(f"OK `{full_name}`: inserted {len(new_df)} new rows")

    elif strategy == "upsert":
        ids = [int(x) for x in df[key_col].dropna().unique()]
        for i in range(0, len(ids), 500):
            chunk = ", ".join(str(x) for x in ids[i:i + 500])
            conn.execute(text(f"DELETE FROM {full_name} WHERE [{key_col}] IN ({chunk})"))

        df.to_sql(name=table, schema=schema, con=conn, if_exists="append", index=False, chunksize=1000)
        print(f"OK `{full_name}`: upserted {len(df)} rows")


with engine.begin() as conn:
    for path, table, use_utf8, strategy, key_col, silver_drop_cols, silver_pk_cols in files:
        df = pd.read_csv(path, encoding="utf-8" if use_utf8 else None, low_memory=False)

        # bronze: mirror of the CSV as-is, append-only (duplicates included, if any)
        load(conn, df, "bronze", table, "insert_only", key_col)

        # silver: normalized (drop duplicate/raw columns, dedupe on PK), same strategy as datamart_to_sql.py
        silver_df = df.drop(columns=[c for c in silver_drop_cols if c in df.columns])
        before = len(silver_df)
        silver_df = silver_df.drop_duplicates(subset=silver_pk_cols)
        if len(silver_df) < before:
            print(f"   (dropped {before - len(silver_df)} duplicate rows on {silver_pk_cols} for silver.{table})")
        load(conn, silver_df, "silver", table, strategy, key_col)

print("\nStravaDW load complete!")
