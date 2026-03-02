import eurostat
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"
COUNTRIES = ["SE", "NO", "DK", "FI"]

def fetch_retail_trade():
    print("Fetching retail trade data (sts_trtu_m)...")
    df = eurostat.get_data_df("sts_trtu_m")
    df = df[df["geo\\TIME_PERIOD"].isin(COUNTRIES)]
    df = df[df["nace_r2"] == "G47"]
    df = df[df["indic_bt"] == "VOL_SLS"]
    df = df[df["s_adj"] == "NSA"]
    df = df[df["unit"] == "I15"]
    return df

def fetch_ecommerce():
    print("Fetching e-commerce data (isoc_ec_eseln2)...")
    df = eurostat.get_data_df("isoc_ec_eseln2")
    df = df[df["geo\\TIME_PERIOD"].isin(COUNTRIES)]
    return df

def fetch_consumer_confidence():
    print("Fetching consumer confidence (ei_bsco_m)...")
    df = eurostat.get_data_df("ei_bsco_m")
    df = df[df["geo\\TIME_PERIOD"].isin(COUNTRIES)]
    df = df[df["indic"] == "BS-CSMCI"]
    return df
def melt_and_clean(df, value_name):
    df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
    id_cols = [c for c in df.columns if not c[:4].isdigit()]
    time_cols = [c for c in df.columns if c[:4].isdigit()]
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
    df_retail = fetch_retail_trade()
    df_retail = melt_and_clean(df_retail, "index_value")
    load_to_bigquery(df_retail, "eurostat_retail_trade")

    df_ecom = fetch_ecommerce()
    df_ecom = melt_and_clean(df_ecom, "pct_value")
    load_to_bigquery(df_ecom, "eurostat_ecommerce")

    df_conf = fetch_consumer_confidence()
    df_conf = melt_and_clean(df_conf, "confidence_value")
    load_to_bigquery(df_conf, "eurostat_consumer_confidence")

if __name__ == "__main__":
    main()


