import json, time, requests, numpy as np, pandas as pd
import gpxpy, gpxpy.gpx
from pathlib import Path
from datetime import datetime, timedelta

# ───────── CONFIG ─────────
CLIENT_ID     = 168168
CLIENT_SECRET = "5f5dd0d2be43e4ec386cf81d5aa7de5e8d62fc0b"
TOKENS_FILE   = Path(r"D:\BO\friends_tokens.json")  # shared OAuth tokens, same as the StravaProject pipeline
BASE_DIR      = Path(r"D:\BO\stravasquad2026\data\strava_api")  # flat export directory
DAYS_BACK     = 15
WRITE_GPX     = True
WRITE_PARQUET = False  # False → CSV
PER_PAGE      = 200
# ──────────────────────────

BASE_DIR.mkdir(parents=True, exist_ok=True)

def refresh(tok):
    if time.time() < tok.get("expires_at", 0) - 300:
        return tok["access_token"]
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": tok["refresh_token"]
        },
        timeout=15
    ).json()
    tok.update(
        access_token=resp["access_token"],
        refresh_token=resp["refresh_token"],
        expires_at=resp["expires_at"]
    )
    return tok["access_token"]

def write_stream(df, fname):
    path = BASE_DIR / (fname + (".parquet" if WRITE_PARQUET else ".csv"))
    if WRITE_PARQUET:
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)

def write_gpx(latlng, times, act_name, fname):
    if not WRITE_GPX:
        return
    g = gpxpy.gpx.GPX()
    trk = gpxpy.gpx.GPXTrack(name=act_name)
    g.tracks.append(trk)
    seg = gpxpy.gpx.GPXTrackSegment()
    trk.segments.append(seg)
    start = datetime.utcnow()
    for (lat, lon), sec in zip(latlng, times):
        seg.points.append(
            gpxpy.gpx.GPXTrackPoint(lat, lon, time=start + timedelta(seconds=sec))
        )
    out_path = BASE_DIR / (fname + ".gpx")
    out_path.write_text(g.to_xml(), encoding="utf-8")

if not TOKENS_FILE.exists():
    raise FileNotFoundError(f"{TOKENS_FILE} not found.")
tokens = json.loads(TOKENS_FILE.read_text())
since_ts = None if DAYS_BACK is None else int(time.time() - DAYS_BACK * 86400)

for aid, meta in tokens.items():
    print(f"\n=== {meta['name']} ({aid}) ===")
    access = refresh(meta)
    TOKENS_FILE.write_text(json.dumps(tokens, indent=2))
    hdrs = {"Authorization": f"Bearer {access}"}

    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page}
        if since_ts:
            params["after"] = since_ts

        res = requests.get(
            "https://www.strava.com/api/v3/athlete/activities",
            headers=hdrs, params=params, timeout=20
        )
        if res.status_code != 200:
            print(f"HTTP {res.status_code}: {res.text[:120]}")
            break
        try:
            acts = res.json()
        except Exception as e:
            print("JSON parse error:", e)
            break
        if not isinstance(acts, list) or not acts:
            print("No more activities.")
            break

        for a in acts:
            act_id   = a["id"]
            sport    = a["sport_type"].lower()
            date_str = a["start_date_local"][:10]
            base     = f"{meta['name']}_{date_str}_{sport}_{act_id}"

            # skip if already exists
            main_file = BASE_DIR / (base + (".parquet" if WRITE_PARQUET else ".csv"))
            if main_file.exists():
                print("skip", base)
                continue

            # Streams
            st_res = requests.get(
                f"https://www.strava.com/api/v3/activities/{act_id}/streams",
                headers=hdrs,
                params={"keys":"latlng,time,distance,altitude,heartrate,cadence,watts","key_by_type":"true"},
                timeout=25
            )
            if st_res.status_code != 200:
                print("stream failed:", st_res.status_code)
                continue
            st = st_res.json()
            if "latlng" not in st:
                print("no latlng for", act_id)
                continue

            df = pd.DataFrame({
                "time_s":    st["time"]["data"],
                "lat":       [pt[0] for pt in st["latlng"]["data"]],
                "lon":       [pt[1] for pt in st["latlng"]["data"]],
                "dist_m":    st.get("distance",{}).get("data"),
                "alt_m":     st.get("altitude",{}).get("data"),
                "hr_bpm":    st.get("heartrate",{}).get("data"),
                "cadence":   st.get("cadence",{}).get("data"),
                "watts":     st.get("watts",{}).get("data"),
            })

            # detailed for elevation_gain, best efforts, segments
            det = requests.get(f"https://www.strava.com/api/v3/activities/{act_id}", headers=hdrs, timeout=25).json()

            # write main CSV
            write_stream(df, base)
            write_gpx(st["latlng"]["data"], st["time"]["data"], a["name"], base)

            # segment efforts
            segs = det.get("segment_efforts", [])
            if segs:
                pd.DataFrame(segs).to_csv(BASE_DIR / f"{base}_segments.csv", index=False)

            # run best efforts
            if sport == "run":
                be = det.get("best_efforts", [])
                if be:
                    pd.DataFrame(be).to_csv(BASE_DIR / f"{base}_best.csv", index=False)

            print("done", base)
        page += 1

print("\nAll done!")
