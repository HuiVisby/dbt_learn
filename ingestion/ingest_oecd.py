import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone


PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"


def fetch_oecd_norway_confidence():
    """Fetch Norway consumer confidence from OECD MEI dataset (CSCICP03, monthly)."""
    print("Fetching OECD Norway consumer confidence...")
    url = "https://sdmx.oecd.org/public/rest/data/OECD,DF_MEI,/NOR.CSCICP03.IXOBSA.M"
    headers = {"Accept": "application/vnd.sdmx.data+json;version=2"}
    params = {"startPeriod": "2014-01", "endPeriod": "2025-12"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    data = r.json()
    time_periods = data["data"]["structures"][0]["dimensions"]["observation"][0]["values"]
    observations = data["data"]["dataSets"][0]["series"]["0:0:0:0"]["observations"]
    rows = []
    for idx, period_info in enumerate(time_periods):
        obs = observations.get(str(idx))
        if obs and obs[0] is not None:
            rows.append({
                "country_code": "NO",
                "period": period_info["id"],
                "confidence_value": float(obs[0]),
                "ingested_at": datetime.now(timezone.utc).isoformat()
            })
    print(f"OECD Norway confidence: {len(rows)} rows")
    return pd.DataFrame(rows)


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
