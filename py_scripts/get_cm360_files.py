# Getting CM360 Files # 
from googleapiclient.discovery import build
from datetime import datetime, timezone, timedelta

def get_cm360_files(profile_id, report_id, credentials):
    # Build the CM360 service
    cm360 = build("dfareporting", "v4", credentials=credentials)

    # Fetch the files
    response = cm360.reports().files().list(
        profileId=profile_id,
        reportId=report_id
    ).execute()

    files = response.get("items", [])

    # If no files found, return empty list
    if not files:
        print("No files found for this report.")
        return []

    # Convert the file metadata into a list of dicts
    parsed_files = []
    for f in files:
        last_modified_ts = int(f["lastModifiedTime"]) / 1000
        last_modified_dt = datetime.fromtimestamp(last_modified_ts, tz=timezone(timedelta(hours=-8)))  # PST/PDT

        parsed_files.append({
            "file_id": f.get("id"),
            "report_id": f.get("reportId"),
            "etag": f.get("etag"),
            "status": f.get("status"),
            "file_name": f.get("fileName"),
            "format": f.get("format"),
            "last_modified": last_modified_dt,
            "start_date": f.get("dateRange", {}).get("startDate"),
            "end_date": f.get("dateRange", {}).get("endDate"),
            "browser_url": f.get("urls", {}).get("browserUrl"),
            "api_url": f.get("urls", {}).get("apiUrl")
        })

    return parsed_files
  
  # R-safe wrapper
def get_cm360_files_from_r(kwargs):
  return get_cm360_files(
      profile_id=kwargs["profile_id"],
      report_id=kwargs["report_id"],
      credentials=kwargs["credentials"]
    )
