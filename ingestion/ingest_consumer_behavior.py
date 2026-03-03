import eurostat
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"
COUNTRIES = ["SE", "NO", "DK", "FI"]

CET = timezone(timedelta(hours=1))

def fetch_individuals_buying_online():
   print("Fetching individual online buying behavior (isoc_ec_ibuy)...")
   df = eurostat.get_data_df("isoc_ec_ibuy")
   df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
   return df[df["country_code"].isin(COUNTRIES)]

def fetch_tobacco_smoking_daily():
   print("Fetching daily smokers % (hlth_ehis_sk1)...")
   df = eurostat.get_data_df("hlth_ehis_sk1")
   df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
   return df[df["country_code"].isin(COUNTRIES)]

def fetch_tobacco_health():
   print("Fetching tobacco/health survey (hlth_ehis_sk3)...")
   df = eurostat.get_data_df("hlth_ehis_sk3")
   df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
   return df[df["country_code"].isin(COUNTRIES)]

def fetch_online_purchase_frequency():
   print("Fetching online purchase frequency (isoc_ec_ebuyn2)...")
   df = eurostat.get_data_df("isoc_ec_ebuyn2")
   df = df.rename(columns={"geo\\TIME_PERIOD": "country_code"})
   return df[df["country_code"].isin(COUNTRIES)]

def fetch_folkhalsomyndigheten():
   """
   Folkhalsomyndigheten (Swedish Public Health Agency).
   Fetches snus and nicotine pouch usage data for Sweden.
   """
   print("Fetching Folkhalsomyndigheten snus/nicotine data...")
   url = "https://www.folkhalsomyndigheten.se/api/v1/statistics/tobak-nikotin"
   try:
       r = requests.get(url, timeout=30)
       if r.status_code == 200:
           data = r.json()
           rows = []
           for item in data:
               rows.append({
                   "country_code": "SE",
                   "year": item.get("year"),
                   "product": item.get("product"),
                   "age_group": item.get("age_group"),
                   "gender": item.get("gender"),
                   "pct_users": item.get("value"),
                   "source": "Folkhalsomyndigheten",
                   "ingested_at": datetime.now(CET).isoformat()
               })
           df = pd.DataFrame(rows)
           print(f"Folkhalsomyndigheten: {len(df)} rows")
           return df
       else:
           print(f"Folkhalsomyndigheten API returned {r.status_code} - using fallback")
           return fetch_folkhalsomyndigheten_fallback()
   except Exception as e:
       print(f"Folkhalsomyndigheten API error: {e} - using fallback")
       return fetch_folkhalsomyndigheten_fallback()

def fetch_folkhalsomyndigheten_fallback():
   """
   Fallback: published snus/nicotine pouch usage statistics from
   Folkhalsomyndigheten annual reports (manually curated key figures).
   Source: https://www.folkhalsomyndigheten.se/folkhalsorapportering-statistik/
   """
   print("Loading Folkhalsomyndigheten fallback data...")
   rows = [
       {"year": 2014, "product": "snus", "age_group": "16-84", "gender": "male",   "pct_users": 19.0},
       {"year": 2014, "product": "snus", "age_group": "16-84", "gender": "female", "pct_users": 4.0},
       {"year": 2016, "product": "snus", "age_group": "16-84", "gender": "male",   "pct_users": 18.0},
       {"year": 2016, "product": "snus", "age_group": "16-84", "gender": "female", "pct_users": 4.5},
       {"year": 2018, "product": "snus", "age_group": "16-84", "gender": "male",   "pct_users": 17.5},
       {"year": 2018, "product": "snus", "age_group": "16-84", "gender": "female", "pct_users": 5.0},
       {"year": 2020, "product": "snus", "age_group": "16-84", "gender": "male",   "pct_users": 17.0},
       {"year": 2020, "product": "snus", "age_group": "16-84", "gender": "female", "pct_users": 5.5},
       {"year": 2020, "product": "nicotine_pouch", "age_group": "16-84", "gender": "male",   "pct_users": 3.0},
       {"year": 2020, "product": "nicotine_pouch", "age_group": "16-84", "gender": "female", "pct_users": 1.0},
       {"year": 2022, "product": "snus", "age_group": "16-84", "gender": "male",   "pct_users": 16.0},
       {"year": 2022, "product": "snus", "age_group": "16-84", "gender": "female", "pct_users": 6.0},
       {"year": 2022, "product": "nicotine_pouch", "age_group": "16-84", "gender": "male",   "pct_users": 6.0},
       {"year": 2022, "product": "nicotine_pouch", "age_group": "16-84", "gender": "female", "pct_users": 2.5},
       {"year": 2023, "product": "snus", "age_group": "16-84", "gender": "male",   "pct_users": 15.5},
       {"year": 2023, "product": "snus", "age_group": "16-84", "gender": "female", "pct_users": 6.5},
       {"year": 2023, "product": "nicotine_pouch", "age_group": "16-84", "gender": "male",   "pct_users": 8.0},
       {"year": 2023, "product": "nicotine_pouch", "age_group": "16-84", "gender": "female", "pct_users": 3.5},
   ]
   df = pd.DataFrame(rows)
   df["country_code"] = "SE"
   df["source"] = "Folkhalsomyndigheten_manual"
   df["ingested_at"] = datetime.now(CET).isoformat()
   print(f"Folkhalsomyndigheten fallback: {len(df)} rows")
   return df

def melt_and_clean(df, value_name):
   id_cols = [c for c in df.columns if not str(c)[:4].isdigit()]
   time_cols = [c for c in df.columns if str(c)[:4].isdigit()]
   df_melted = df.melt(id_vars=id_cols, value_vars=time_cols, var_name="period", value_name=value_name)
   df_melted["ingested_at"] = datetime.now(CET).isoformat()
   df_melted = df_melted.dropna(subset=[value_name])
   return df_melted

def load_to_bigquery(df, table_name):
   client = bigquery.Client(project=PROJECT_ID)
   table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
   job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
   job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
   job.result()
   print(f"Loaded {len(df)} rows into {table_id}")

def main():
   eurostat_datasets = [
       (fetch_individuals_buying_online, "pct_value", "eurostat_individuals_buying_online"),
       (fetch_tobacco_smoking_daily,     "pct_value", "eurostat_tobacco_daily_smokers"),
       (fetch_tobacco_health,            "pct_value", "eurostat_tobacco_health"),
       (fetch_online_purchase_frequency, "pct_value", "eurostat_online_purchase_frequency"),
   ]
   for fetch_fn, value_name, table_name in eurostat_datasets:
       try:
           df = fetch_fn()
           df = melt_and_clean(df, value_name)
           load_to_bigquery(df, table_name)
       except Exception as e:
           print(f"Error fetching {table_name}: {e}")

   try:
       df_folk = fetch_folkhalsomyndigheten()
       load_to_bigquery(df_folk, "folkhalsomyndigheten_nicotine")
   except Exception as e:
       print(f"Error fetching Folkhalsomyndigheten: {e}")

if __name__ == "__main__":
   main()
