import eurostat
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"
COUNTRIES = ["SE", "NO", "DK", "FI"]


def fetch_individuals_buying_online():
    """
    Eurostat isoc_ec_ibuy - individuals buying online.
    Covers: product categories, frequency, device used.
    """
    print("Fetching individual online buying behavior (isoc_ec_ibuy)...")
    df = eurostat.get_data_df("isoc_ec_ibuy")
    df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
    df = df[df["country_code"].isin(COUNTRIES)]
    return df


def fetch_tobacco_health():
    """
    Eurostat hlth_ehis_sk3 - tobacco smoking by sex, age, country.
    Shows consumer base trends for nicotine products.
    """
    print("Fetching tobacco/health survey data (hlth_ehis_sk3)...")
    df = eurostat.get_data_df("hlth_ehis_sk3")
    df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
    df = df[df["country_code"].isin(COUNTRIES)]
    return df


def fetch_retail_digital_behavior():
    """
    Eurostat isoc_ec_ebuyn2 - frequency of online purchases by individuals.
    Shows how often Nordic consumers shop online.
    """
    print("Fetching online purchase frequency (isoc_ec_ebuyn2)...")
    df = eurostat.get_data_df("isoc_ec_ebuyn2")
    df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
    df = df[df["country_code"].isin(COUNTRIES)]
    return df


def melt_and_clean(df, value_name):
    id_cols = [c for c in df.columns if not str(c)[:4].isdigit()]
    time_cols = [c for c in df.columns if str(c)[:4].isdigit()]
    df_melted = df.melt(
        id_vars=id_cols,
        value_vars=time_cols,
        var_name="period",
        value_name=value_name
    )
    df_melted["ingested_at"] = datetime.utcnow().isoformat()
    df_melted = df_melted.dropna(subset=[value_name])
    return df_melted


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
        (fetch_individuals_buying_online, "pct_value", "eurostat_individuals_buying_online"),
        (fetch_tobacco_health, "pct_value", "eurostat_tobacco_health"),
        (fetch_retail_digital_behavior, "pct_value", "eurostat_online_purchase_frequency"),
    ]

    for fetch_fn, value_name, table_name in datasets:
        try:
            df = fetch_fn()
            df = melt_and_clean(df, value_name)
            load_to_bigquery(df, table_name)
        except Exception as e:
            print(f"Error fetching {table_name}: {e}")


if __name__ == "__main__":
    main()
