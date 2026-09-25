import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
dbutils = w.dbutils

URL = "https://jsearch.p.rapidapi.com/search-v2"
API_KEY = dbutils.secrets.get(scope="rapidapi", key="jsearch_key")
if not API_KEY:
    raise RuntimeError("Missing API_KEY for JSearch client.")

def fetch_jobs():
    querystring = {"country": "us", "date_posted": "today", "query": "data engineer jobs"}

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    response = requests.get(URL, headers=headers, params=querystring)

    return response