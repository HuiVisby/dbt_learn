from google.cloud import bigquery
import pandas as pd
from datetime import datetime

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"

# Retail-relevant search terms for Swedish Match / PMI context
RETAIL_TERMS_QUERY = """
SELECT
    week,
    country_code,
    term,
    rank,
    score,
    refresh_date
FROM `bigquery-public-data.google_trends.international_top_terms`
WHERE country_code IN ('SE', 'NO', 'DK', 'FI')
  AND week >= '2014-01-01'
ORDER BY week DESC, country_code, rank
"""

RISING_TERMS_QUERY = """
SELECT
    week,
    country_code,
    term,
    rank,
    score,
    refresh_date
FROM `bigquery-public-data.google_trends.international_top_rising_terms`
WHERE country_code IN ('SE', 'NO', 'DK', 'FI')
  AND week >= '2014-01-01'
ORDER BY week DESC, country_code, rank
"""


def fetch_google_trends(query, label):
    """Fetch Google Trends data from BigQuery public dataset."""
    print(f"Fetching Google Trends: {label}...")
    client = bigquery.Client(project=PROJECT_ID)
    df = client.query(query).to_dataframe()
    df["ingested_at"] = datetime.utcnow().isoformat()
    print(f"  {label}: {len(df)} rows")
    return df


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
    datasets = [
        (RETAIL_TERMS_QUERY, "google_trends_top_terms", "top terms"),
        (RISING_TERMS_QUERY, "google_trends_rising_terms", "rising terms"),
    ]

    for query, table_name, label in datasets:
        try:
            df = fetch_google_trends(query, label)
            if not df.empty:
                load_to_bigquery(df, table_name)
        except Exception as e:
            print(f"Error fetching {label}: {e}")


if __name__ == "__main__":
    main()
