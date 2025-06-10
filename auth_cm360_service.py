from google.oauth2 import service_account
from googleapiclient.discovery import build
import json

# Path to the service account key file
SERVICE_ACCOUNT_FILE = "/home/rstudio/R/kawasaki/credentials/gsp-kawi-1-6269a073b7c4.json"

# Define CM360 API scope
SCOPES = [
    "https://www.googleapis.com/auth/dfareporting",
    "https://www.googleapis.com/auth/dfatrafficking"  # Required for advertiser access
]

def get_cm360_service():
    """Authenticates and returns the CM360 API client."""
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("dfareporting", "v4", credentials=credentials)

def test_cm360():
    """Test API call to verify authentication."""
    try:
        service = get_cm360_service()
        user_profiles = service.userProfiles().list().execute()
        return json.dumps(user_profiles, indent=4)  # Pretty-print response
    except Exception as e:
        return f" authentication failed: {str(e)}"

if __name__ == "__main__":
    print(test_cm360())
