import os
from dotenv import load_dotenv

load_dotenv()

import requests

URL = "https://jsearch.p.rapidapi.com/search-v2"
API_KEY = os.getenv('API_KEY')
def fetch_jobs():
    querystring = {"query": "data engineer jobs"}

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    response = requests.get(URL, headers=headers, params=querystring)

    return response