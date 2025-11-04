import os
import re
import pandas as pd

FOLDER = r"D:\BO\strava_api"
OUTDIR = r"D:\BO\strava_api_exports"
SKIP_FILES = {'best_profiles.csv', 'segments.csv', 'runstream.csv'}

best_list      = []
segments_list  = []
runstream_list = []

for fname in os.listdir(FOLDER):
    if fname in SKIP_FILES:
        continue
    
    path = os.path.join(FOLDER, fname)
    if not os.path.isfile(path):
        continue

    base, ext = os.path.splitext(fname)
    ext = ext.lower()
    # skip GPX and non-CSV/XLSX
    if ext == '.gpx' or ext not in {'.csv', '.xlsx'}:
        continue

    # read file
    df = pd.read_csv(path, low_memory=False) if ext == '.csv' else pd.read_excel(path)

    if base.endswith('_best'):
        best_list.append(df)
    elif base.endswith('_segments'):
        segments_list.append(df)
    else:
        # extract activity_id from filename (digits after last underscore)
        m = re.search(r'_(\d+)$', base)
        df['activity_id'] = m.group(1) if m else None
        runstream_list.append(df)

# concatenate lists
df_best      = pd.concat(best_list,      ignore_index=True) if best_list      else pd.DataFrame()
df_segments  = pd.concat(segments_list,  ignore_index=True) if segments_list  else pd.DataFrame()
df_runstream = pd.concat(runstream_list, ignore_index=True) if runstream_list else pd.DataFrame()

# drop unwanted columns from runstream
drop_cols = [
    'id','resource_state','name','activity','athlete','elapsed_time','moving_time',
    'start_date','start_date_local','distance','pr_rank','achievements','start_index',
    'end_index','run_id','average_cadence','device_watts','average_watts',
    'average_heartrate','max_heartrate','segment','visibility','hidden'
]
df_runstream.drop(columns=drop_cols, errors='ignore', inplace=True)

# export to CSV with status flag
exports = {
    'best_profiles.csv': df_best,
    'segments.csv':      df_segments,
    'runstream.csv':     df_runstream,
}

export_success = True
for fname, df in exports.items():
    outpath = os.path.join(OUTDIR, fname)
    try:
        df.to_csv(outpath, index=False)
        print(f"✅ Exported {fname} ({df.shape[0]} rows × {df.shape[1]} cols)")
    except Exception as e:
        print(f"❌ Failed exporting {fname}: {e}")
        export_success = False

if export_success:
    print("\n🎉 All files exported successfully!")
else:
    print("\n⚠️ Some exports failed. See messages above.")