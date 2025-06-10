# CM360 Overall via Python # 
import json
import csv
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import requests


def load_and_refresh_credentials(credentials_path="~/.cm360_token.json"):
    path = Path(credentials_path).expanduser()
    with open(path, "r") as f:
        data = json.load(f)

    credentials = Credentials(
        token=data["token"],
        refresh_token=data["refresh_token"],
        token_uri=data["token_uri"],
        client_id=data["client_id"],
        client_secret=data["client_secret"],
        scopes=data["scopes"]
    )

    if credentials.expired:
        print("Access token expired, attempting refresh...")
        credentials.refresh(Request())
        data["token"] = credentials.token
        data["expiry"] = credentials.expiry.isoformat()
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        print("Token refreshed")
    else:
        print("Access token still valid")

    return credentials, credentials.token  # return creds and raw token string


def get_cm360_files(profile_id, report_id, credentials):
    cm360 = build("dfareporting", "v4", credentials=credentials)

    response = cm360.reports().files().list(
        profileId=profile_id,
        reportId=report_id
    ).execute()

    files = response.get("items", [])
    if not files:
        return []

    parsed = []
    for f in files:
        ts = int(f["lastModifiedTime"]) / 1000
        dt = datetime.fromtimestamp(ts, tz=timezone(timedelta(hours=-8)))
        parsed.append({
            "file_id": f.get("id"),
            "report_id": f.get("reportId"),
            "etag": f.get("etag"),
            "status": f.get("status"),
            "file_name": f.get("fileName"),
            "format": f.get("format"),
            "last_modified": dt.isoformat(),
            "start_date": f.get("dateRange", {}).get("startDate"),
            "end_date": f.get("dateRange", {}).get("endDate"),
            "browser_url": f.get("urls", {}).get("browserUrl"),
            "api_url": f.get("urls", {}).get("apiUrl")
        })

    return parsed


def download_latest_cm360_report(file_list, access_token):
    if not file_list:
        return []

    # Sort by last_modified descending
    sorted_files = sorted(file_list, key=lambda x: x["last_modified"], reverse=True)
    latest = sorted_files[0]
    download_url = latest["api_url"]

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    response = requests.get(download_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Download failed with status code {response.status_code}")

    # Try to find where the actual CSV starts
    content = response.content.decode("utf-8")
    lines = content.splitlines()
    header_line_index = next((i for i, line in enumerate(lines) if line.startswith("Date,")), None)

    if header_line_index is None:
        raise Exception("Could not find header row starting with 'Date'.")

    csv_data = "\n".join(lines[header_line_index:])
    reader = csv.DictReader(StringIO(csv_data))
    rows = [row for row in reader if row.get("Date") != "Grand Total:"]

    return rows


# SINGLE ENTRYPOINT FUNCTION
def get_latest_cm360_report(profile_id, report_id, credentials_path="~/.cm360_token.json"):
    credentials, access_token = load_and_refresh_credentials(credentials_path)
    file_list = get_cm360_files(profile_id, report_id, credentials)

    if not file_list:
        return []

    report_rows = download_latest_cm360_report(file_list, access_token)
    return report_rows
