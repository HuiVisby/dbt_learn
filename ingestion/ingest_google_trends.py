
from pytrends.request import TrendReq
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import time

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"

# Localized keywords per country
KEYWORDS_BY_COUNTRY = {
    "SE": ["snus", "nikotinpåsar", "ZYN"],
    "NO": ["snus", "nikotinposer", "ZYN"],
    "DK": ["snus", "nikotinposer", "ZYN"],
    "FI": ["nuuska", "nikotiinipussit", "ZYN"],
}


def fetch_trends_over_time():
    """Weekly search interest per keyword per country, 2019-2025."""
    pytrends = TrendReq(hl="en-US", tz=60)
    all_rows = []

    for country, keywords in KEYWORDS_BY_COUNTRY.items():
        print(f"Fetching trends for {country}: {keywords}")
        try:
            pytrends.build_payload(
                keywords,
                timeframe="2019-01-01 2025-12-31",
                geo=country
            )
            df = pytrends.interest_over_time()
            if df.empty:
                print(f"  {country}: no data")
                continue
            df = df.drop(columns=["isPartial"], errors="ignore")
            df = df.reset_index()
            df_melted = df.melt(
                id_vars=["date"],
                var_name="keyword",
                value_name="search_interest"
            )
            df_melted["country_code"] = country
            all_rows.append(df_melted)
            print(f"  {country}: {len(df_melted)} rows")
        except Exception as e:
            print(f"  {country} error: {e}")
        time.sleep(3)  # avoid rate limiting

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


def fetch_trends_by_region():
    """Search interest by sub-region within each country for 'snus' — geo diff."""
    pytrends = TrendReq(hl="en-US", tz=60)
    all_rows = []

    terms = {"SE": "snus", "NO": "snus", "DK": "snus", "FI": "nuuska"}

    for country, keyword in terms.items():
        print(f"Fetching regional trends for {country}: '{keyword}'")
        try:
            pytrends.build_payload(
                [keyword],
                timeframe="2019-01-01 2025-12-31",
                geo=country
            )
            df = pytrends.interest_by_region(resolution="REGION", inc_low_vol=True)
            if df.empty:
                print(f"  {country}: no regional data")
                continue
            df = df.reset_index()
            df.columns = ["region", "search_interest"]
            df["country_code"] = country
            df["keyword"] = keyword
            all_rows.append(df)
            print(f"  {country}: {len(df)} regions")
        except Exception as e:
            print(f"  {country} error: {e}")
        time.sleep(3)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result


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
    df_time = fetch_trends_over_time()
    if not df_time.empty:
        load_to_bigquery(df_time, "google_trends_nicotine_weekly")

    df_region = fetch_trends_by_region()
    if not df_region.empty:
        load_to_bigquery(df_region, "google_trends_nicotine_by_region")


if __name__ == "__main__":
    main()
