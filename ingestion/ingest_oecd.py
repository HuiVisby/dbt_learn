import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone


PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"


def fetch_oecd_norway_confidence():
    print("Fetching OECD Norway consumer confidence...")
    url = "https://stats.oecd.org/sdmx-json/data/MEI/NOR.CSCICP03.IXOBSA.M/all"
    params = {"startTime": "2014-01", "endTime": "2025-12"}
    r = requests.get(url, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()
    structures = data["data"]["structures"][0]
    print(f"Series dimension keys: {[d['id'] for d in structures['dimensions'].get('series', [])]}")
    print(f"Observation dimension keys: {[d['id'] for d in structures['dimensions'].get('observation', [])]}")
    time_periods = structures["dimensions"]["observation"][0]["values"]
    print(f"Time periods count: {len(time_periods)}")
    print(f"First 3 periods: {time_periods[:3]}")
    series_keys = list(data["data"]["dataSets"][0]["series"].keys())
    print(f"Series keys (first 3): {series_keys[:3]}")
    observations = data["data"]["dataSets"][0]["series"]["0:0:0:0"]["observations"]
    print(f"Observation count: {len(observations)}")
    print(f"First 3 obs: {list(observations.items())[:3]}")

def load_to_bigquery(df, table_name):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")


def main():
    try:
        df = fetch_oecd_norway_confidence()
        if not df.empty:
            load_to_bigquery(df, "oecd_consumer_confidence_no")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
