import base64
import requests
from typing import Dict
from requests import Response
from datetime import datetime, timedelta

def get_access_token(username: str, password: str) -> str:
    url = "https://api.moysklad.ru/api/remap/1.2/security/token"
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Accept-Encoding": "gzip"
    }
    
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json().get("access_token") 