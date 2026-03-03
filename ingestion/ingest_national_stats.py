import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"
CET = timezone(timedelta(hours=1))

def fetch_scb_sweden():
  print("Fetching SCB Sweden retail data (1991-present)...")
  url = "https://api.scb.se/OV0104/v1/doris/en/ssd/HA/HA0101/HA0101B/DetOms07N"
  payload = {"query": [{"code": "SNI2007", "selection": {"filter": "item", "values": ["47"]}}, {"code": "Tid", "selection": {"filter": "all", "values": ["*"]}}], "response": {"format": "json"}}
  r = requests.post(url, json=payload, timeout=30)
  data = r.json()
  rows = []
  for item in data["data"]:
      period = item["key"][1].replace("M", "-")
      try:
          rows.append({"country_code": "SE", "period": period, "index_value": float(item["values"][0]), "source": "SCB"})
      except (ValueError, TypeError):
          continue
  print(f"SCB: {len(rows)} rows")
  return pd.DataFrame(rows)

def load_to_bigquery(df, table_name):
  client = bigquery.Client(project=PROJECT_ID)
  table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
  job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
  job.result()
  print(f"Loaded {len(df)} rows into {table_id}")

def main():
  try:
      df = fetch_scb_sweden()
      if not df.empty:
          df["ingested_at"] = datetime.now(CET).isoformat()
          load_to_bigquery(df, "scb_retail_trade")
  except Exception as e:
      print(f"Error: {e}")

if __name__ == "__main__":
  main()
