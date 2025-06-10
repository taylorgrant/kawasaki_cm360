# GCP Reauthorization # 
import json
from pathlib import Path
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

def gcp_reauth(credentials_path=Path.home() / ".cm360_token.json", expiry_buffer_minutes=2):
    credentials_path = Path(credentials_path).expanduser()

    if not credentials_path.exists():
        raise FileNotFoundError("Credentials file not found. Please authenticate first using `gcp_auth()`.")

    # Load the saved credentials
    with open(credentials_path, "r") as f:
        credentials = Credentials.from_authorized_user_info(json.load(f))

    # Check expiry with a buffer
    expiry_buffer = timedelta(minutes=expiry_buffer_minutes)
    should_refresh = not credentials.valid or (
        credentials.expiry and credentials.expiry - expiry_buffer <= datetime.utcnow()
    )

    if should_refresh:
        print("Token expired or near expiry. Attempting refresh...")
        try:
            credentials.refresh(Request())
            print("Token refreshed successfully at", datetime.now())

            # Save the full updated credentials
            with open(credentials_path, "w") as f:
                f.write(credentials.to_json())
        except Exception as e:
            raise RuntimeError("Token refresh failed! Please re-authenticate using `gcp_auth()`.") from e
    else:
        print("Token is still valid. Expiry:", credentials.expiry)

    return credentials
