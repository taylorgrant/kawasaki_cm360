# GCP Authorization # 
import os
import json
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

def gcp_auth(creds_loc="/home/rstudio/R/kawasaki/credentials/gcp-credentials.json"):
    token_path = Path.home() / ".cm360_token.json"

    # Prevent unnecessary re-authentication
    # if token_path.exists():
    #     raise Exception("Authentication already completed. Use `reauthorize_cm360()` to refresh tokens.")

    # Define OAuth scopes for CM360
    OAUTH_SCOPES = [
        "https://www.googleapis.com/auth/dfareporting",
        "https://www.googleapis.com/auth/dfatrafficking"
    ]

    # Start OAuth flow
    flow = InstalledAppFlow.from_client_secrets_file(creds_loc, OAUTH_SCOPES)
    credentials = flow.run_local_server(port=8080)

    # Convert credentials to a dict
    credentials_dict = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes,
        "expiry": credentials.expiry.isoformat() if credentials.expiry else None
    }

    # Save as JSON
    with open(token_path, "w") as f:
        json.dump(credentials_dict, f, indent=2)

    return credentials
