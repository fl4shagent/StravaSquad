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

# list of (csv_path, destination_table, utf8_flag)
files = [
    (r"D:\BO\strava_api_exports\best_clean_datetime.csv",          "RunBest",     False),
    (r"D:\BO\strava_api_exports\runstream.csv",           "RunStream",   False),
    (r"D:\BO\strava_api_exports\segments_clean_datetime.csv",      "RunSegment",  True),
    (r"D:\BO\athletes_profiles_clean.csv",                "Athlete",     True),
    (r"D:\BO\all_activities_clean_datetime.csv",                "Activities",  False),
    (r"D:\BO\strava_api_exports\runstream_segments_by_kilometers.csv", "RunSplitKilometer", False)
]

with engine.begin() as conn:
    for path, table, use_utf8 in files:
        # 1) truncate the target table
        conn.execute(text(f"TRUNCATE TABLE {table}"))
        print(f"🔄 Truncated table `{table}`")

        # 2) read CSV with appropriate encoding
        df = pd.read_csv(path, encoding="utf-8" if use_utf8 else None, low_memory=False)

        # 3) bulk-insert into SQL Server
        df.to_sql(
            name=table,
            con=conn,
            if_exists="append",
            index=False,
            chunksize=1000
        )
        print(f"✅ Inserted {len(df)} rows into `{table}`")

print("\n🏁 All tables refreshed and reloaded successfully!")