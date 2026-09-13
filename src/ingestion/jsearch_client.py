import os
from dotenv import load_dotenv

load_dotenv()

import requests

URL = "https://jsearch.p.rapidapi.com/search-v2"
def fetch_jobs():
    querystring = {"query": "data engineer jobs"}

    headers = {
        "x-rapidapi-key": os.getenv('API_KEY'),
        "x-rapidapi-host": "jsearch.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    response = requests.get(URL, headers=headers, params=querystring)

    return response

if __name__ == "__main__":
    fetch_jobs()