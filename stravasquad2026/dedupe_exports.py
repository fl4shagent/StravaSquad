import shutil
from pathlib import Path
import pandas as pd

OUTDIR = Path(r"D:\BO\strava_api_exports")

# (filename, dedupe subset)
targets = [
    ("runstream.csv", ["activity_id", "time_s"]),
    ("segments.csv", ["id"]),
    ("best_profiles.csv", ["id"]),
]

for fname, subset in targets:
    path = OUTDIR / fname
    backup = OUTDIR / (fname + ".bak")
    shutil.copy2(path, backup)

    df = pd.read_csv(path, low_memory=False)
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    after = len(df)
    df.to_csv(path, index=False)
    print(f"{fname}: {before} -> {after} rows (removed {before - after}); backup at {backup.name}")
