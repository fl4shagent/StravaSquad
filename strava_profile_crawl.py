import os
import json
import requests
import csv
from datetime import datetime
from pathlib import Path

# Configuration: Set these as environment variables or replace with your own values
CLIENT_ID = xxx
CLIENT_SECRET = xxx
TOKENS_FILE = Path(r"D:\BO\friends_tokens.json")
OUTPUT_CSV = "athletes_profiles.csv"
STRAVA_API_URL = "https://www.strava.com/api/v3"

def refresh_token(token_info):
    """Refresh an expired Strava access token."""
    response = requests.post(
        f"{STRAVA_API_URL}/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": token_info["refresh_token"],
        },
    )
    response.raise_for_status()
    new_data = response.json()
    token_info.update({
        "access_token": new_data["access_token"],
        "refresh_token": new_data["refresh_token"],
        "expires_at": new_data["expires_at"],
    })
    return token_info

def get_athlete_profile(access_token):
    """Fetch the athlete's profile data."""
    resp = requests.get(
        f"{STRAVA_API_URL}/athlete",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    resp.raise_for_status()
    return resp.json()

def main():
    # Load token info
    with open(TOKENS_FILE, "r+") as tf:
        tokens = json.load(tf)
        profiles = []
        for athlete_id, info in tokens.items():
            # Refresh token if expired
            if info["expires_at"] < int(datetime.now().timestamp()):
                print(f"Refreshing token for athlete {athlete_id}...")
                info = refresh_token(info)

            # Fetch profile and tag with athlete_id
            profile = get_athlete_profile(info["access_token"])
            profile["athlete_id"] = athlete_id
            profiles.append(profile)

        # Write all profiles to CSV
        if profiles:
            fieldnames = sorted(profiles[0].keys())
            with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as cf:
                writer = csv.DictWriter(cf, fieldnames=fieldnames)
                writer.writeheader()
                for row in profiles:
                    writer.writerow(row)
            print(f"Wrote {len(profiles)} athlete profiles to {OUTPUT_CSV}")

        # Update tokens file with any refreshed tokens
        tf.seek(0)
        json.dump(tokens, tf, indent=4)
        tf.truncate()

if __name__ == "__main__":
    main()