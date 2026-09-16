import time
from jsearch_client import fetch_jobs
from databricks.sdk import WorkspaceClient

wsc = WorkspaceClient()

volume_path = f"/Volumes/workspace/bronze/raw_data/postings_{int(time.time())}.json"
def postings_to_volume():
    try:
        print("\nFetching job postings...\n")
        data = fetch_jobs()
        data.raise_for_status()
        print("Successfully fetched new job postings")
        
        print(f"\nUploading data to Databricks Volume {volume_path}...\n")
        wsc.files.upload(file_path=volume_path, contents=data.json(), overwrite=True)
        print(f"Successfully wrote API JSON to {volume_path}")
    except Exception as e:
        print(f"Error ingesting raw postings: {e}")