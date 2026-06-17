import os
import re
import sys
import pandas as pd

FOLDER     = r"D:\BO\stravasquad2026\data\strava_api"
OUTDIR     = r"D:\BO\stravasquad2026\data\strava_api_exports"
SKIP_FILES = {'best_profiles.csv', 'segments.csv', 'runstream.csv'}
MANIFEST   = os.path.join(OUTDIR, ".processed_files.txt")
LOCK_FILE  = os.path.join(OUTDIR, ".datawarehouse.lock")

os.makedirs(OUTDIR, exist_ok=True)

if os.path.exists(LOCK_FILE):
    print(f"Another datawarehouse.py run appears to be in progress (lock file exists: {LOCK_FILE}).")
    print("If no other run is active, delete this file and re-run.")
    sys.exit(1)

with open(LOCK_FILE, 'w') as f:
    f.write(str(os.getpid()))

drop_cols = [
    'id','resource_state','name','activity','athlete','elapsed_time','moving_time',
    'start_date','start_date_local','distance','pr_rank','achievements','start_index',
    'end_index','run_id','average_cadence','device_watts','average_watts',
    'average_heartrate','max_heartrate','segment','visibility','hidden'
]


def load_manifest():
    if os.path.exists(MANIFEST):
        with open(MANIFEST) as f:
            return set(line.strip() for line in f if line.strip())
    return set()


try:
    processed = load_manifest()

    best_list      = []
    segments_list  = []
    runstream_list = []
    new_files      = []

    for fname in os.listdir(FOLDER):
        if fname in SKIP_FILES or fname in processed:
            continue

        path = os.path.join(FOLDER, fname)
        if not os.path.isfile(path):
            continue

        base, ext = os.path.splitext(fname)
        ext = ext.lower()
        if ext == '.gpx' or ext not in {'.csv', '.xlsx'}:
            continue

        df = pd.read_csv(path, low_memory=False) if ext == '.csv' else pd.read_excel(path)

        if base.endswith('_best'):
            best_list.append(df)
        elif base.endswith('_segments'):
            segments_list.append(df)
        else:
            m = re.search(r'_(\d+)$', base)
            df['activity_id'] = m.group(1) if m else None
            runstream_list.append(df)

        new_files.append(fname)

    if not new_files:
        print("Nothing new to process.")
    else:
        def concat_new(lst):
            return pd.concat(lst, ignore_index=True) if lst else pd.DataFrame()

        df_best      = concat_new(best_list)
        df_segments  = concat_new(segments_list)
        df_runstream = concat_new(runstream_list)
        df_runstream.drop(columns=drop_cols, errors='ignore', inplace=True)

        exports = [
            ('best_profiles.csv', df_best),
            ('segments.csv',      df_segments),
            ('runstream.csv',     df_runstream),
        ]

        export_success = True
        for fname_out, new_df in exports:
            if new_df.empty:
                continue
            outpath = os.path.join(OUTDIR, fname_out)
            if os.path.exists(outpath):
                existing = pd.read_csv(outpath, low_memory=False)
                combined = pd.concat([existing, new_df], ignore_index=True)
            else:
                combined = new_df
            try:
                combined.to_csv(outpath, index=False)
                print(f"OK {fname_out}: +{len(new_df)} rows -> {combined.shape[0]} total")
            except Exception as e:
                print(f"FAILED exporting {fname_out}: {e}")
                export_success = False

        if export_success:
            updated = processed | set(new_files)
            with open(MANIFEST, 'w') as f:
                f.writelines(fn + '\n' for fn in sorted(updated))
            print(f"\nDone. Manifest updated (+{len(new_files)} files).")
        else:
            print("\nSome exports failed - manifest not updated.")
finally:
    os.remove(LOCK_FILE)
