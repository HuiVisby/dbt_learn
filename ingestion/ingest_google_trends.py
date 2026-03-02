  from google.cloud import bigquery
  import pandas as pd
  from datetime import datetime

  PROJECT_ID = "nordic-retail-intel-2025"
  DATASET = "raw_ingest"

  TOP_TERMS_QUERY = """
  SELECT
      week,
      country_code,
      term,
      rank,
      score,
      refresh_date
  FROM `bigquery-public-data.google_trends.international_top_terms`
  WHERE country_code IN ('SE', 'NO', 'DK', 'FI')
    AND week >= '2019-01-01'
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
    AND week >= '2019-01-01'
  ORDER BY week DESC, country_code, rank
  """

  def fetch_and_load(query, table_name, label):
      print(f"Fetching Google Trends: {label}...")
      client = bigquery.Client(project=PROJECT_ID)
      df = client.query(query).to_dataframe()
      df["ingested_at"] = datetime.utcnow().isoformat()
      print(f"  {label}: {len(df)} rows")
      if df.empty:
          return
      table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
      job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
      job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
      job.result()
      print(f"Loaded {len(df)} rows into {table_id}")

  def main():
      fetch_and_load(TOP_TERMS_QUERY, "google_trends_top_terms", "top terms")
      fetch_and_load(RISING_TERMS_QUERY, "google_trends_rising_terms", "rising terms")

  if __name__ == "__main__":
      main()
