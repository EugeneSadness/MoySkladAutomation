import gspread
from oauth2client.service_account import ServiceAccountCredentials

def authenticate_google_sheets(credentials_json_path: str) -> gspread.Client:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_json_path, scope)
    return gspread.authorize(credentials)
