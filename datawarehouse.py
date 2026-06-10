import os
import re
import sys
import pandas as pd
from pathlib import Path

# ───────── CONFIG ─────────
FOLDER     = r"D:\BO\strava_api"
OUTDIR     = r"D:\BO\strava_api_exports"
MANIFEST   = Path(OUTDIR) / ".processed_files.txt"
LOCK_FILE  = Path(OUTDIR) / ".datawarehouse.lock"
SKIP_FILES = {"best_profiles.csv", "segments.csv", "runstream.csv"}
DROP_RUNSTREAM = [
    "id", "resource_state", "name", "activity", "athlete", "elapsed_time", "moving_time",
    "start_date", "start_date_local", "distance", "pr_rank", "achievements", "start_index",
    "end_index", "run_id", "average_cadence", "device_watts", "average_watts",
    "average_heartrate", "max_heartrate", "segment", "visibility", "hidden"
]
# ──────────────────────────


def load_manifest() -> set:
    if MANIFEST.exists():
        return set(MANIFEST.read_text(encoding="utf-8").splitlines())
    return set()


def save_manifest(names: set):
    MANIFEST.write_text("\n".join(sorted(names)), encoding="utf-8")


def append_csv(new_dfs, outpath, drop_cols=None):
    """Concat new_dfs and append to outpath (writes header only if file doesn't exist yet)."""
    if not new_dfs:
        return 0
    df = pd.concat(new_dfs, ignore_index=True)
    if drop_cols:
        df.drop(columns=drop_cols, errors="ignore", inplace=True)
    write_header = not os.path.exists(outpath)
    df.to_csv(outpath, mode="a", header=write_header, index=False)
    print(f"  OK Appended {len(df)} rows -> {os.path.basename(outpath)}")
    return len(df)


# ── Prevent overlapping runs (an interrupted run can leave the manifest stale,
# causing a later run to re-append the same batch and duplicate rows) ──
if LOCK_FILE.exists():
    print(f"Another datawarehouse.py run appears to be in progress (lock file exists: {LOCK_FILE}).")
    print("If no other run is active, delete this file and re-run.")
    sys.exit(1)
LOCK_FILE.write_text(str(os.getpid()))

try:
    # ── Load manifest of already-processed filenames ──
    already_done = load_manifest()

    best_list      = []
    segments_list  = []
    runstream_list = []
    new_files      = set()

    for fname in sorted(os.listdir(FOLDER)):
        if fname in SKIP_FILES or fname in already_done:
            continue

        path = os.path.join(FOLDER, fname)
        if not os.path.isfile(path):
            continue

        base, ext = os.path.splitext(fname)
        ext = ext.lower()
        if ext == ".gpx" or ext not in {".csv", ".xlsx"}:
            continue

        df = pd.read_csv(path, low_memory=False) if ext == ".csv" else pd.read_excel(path)

        if base.endswith("_best"):
            best_list.append(df)
        elif base.endswith("_segments"):
            segments_list.append(df)
        else:
            m = re.search(r"_(\d+)$", base)
            df["activity_id"] = m.group(1) if m else None
            runstream_list.append(df)

        new_files.add(fname)

    if not new_files:
        print("No new files to process - warehouse is up to date.")
    else:
        print(f"{len(new_files)} new files found, processing...")

        append_csv(runstream_list, os.path.join(OUTDIR, "runstream.csv"),     DROP_RUNSTREAM)
        append_csv(best_list,      os.path.join(OUTDIR, "best_profiles.csv"))
        append_csv(segments_list,  os.path.join(OUTDIR, "segments.csv"))

        # Save manifest only after successful export
        save_manifest(already_done | new_files)
        print(f"\nWarehouse updated. Manifest now tracks {len(already_done | new_files)} files.")
finally:
    LOCK_FILE.unlink(missing_ok=True)
