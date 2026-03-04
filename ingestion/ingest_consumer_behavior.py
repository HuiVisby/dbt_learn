import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"
COUNTRIES = ["SE", "NO", "DK", "FI"]


  def fetch_individuals_buying_online():
      """
      Combines isoc_ec_ibuy (2004-2019) + isoc_ec_ib20 (2020-2025)
      Indicator I_BLT12: % individuals who ordered online in last 12 months.
      """
      print("Fetching individual online buying behavior...")
      params = {
          "format": "JSON",
          "lang": "EN",
          "geo": COUNTRIES,
          "indic_is": "I_BLT12",
          "ind_type": "IND_TOTAL",
          "unit": "PC_IND"
      }

      all_rows = []
      for dataset in ["isoc_ec_ibuy", "isoc_ec_ib20"]:
          url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}"
          r = requests.get(url, params=params, timeout=60)
          r.raise_for_status()
          data = r.json()

          time_periods = list(data["dimension"]["time"]["category"]["index"].keys())
          geo_index    = data["dimension"]["geo"]["category"]["index"]
          values       = data["value"]
          n_times      = len(time_periods)

          for geo_code, geo_idx in geo_index.items():
              if geo_code not in COUNTRIES:
                  continue
              for t_idx, period in enumerate(time_periods):
                  val = values.get(str(geo_idx * n_times + t_idx))
                  if val is not None:
                      all_rows.append({
                          "country_code": geo_code,
                          "period":       int(period),
                          "indic_is":     "I_BLT12",
                          "ind_type":     "IND_TOTAL",
                          "unit":         "PC_IND",
                          "pct_value":    float(val),
                          "ingested_at":  datetime.utcnow().isoformat()
                      })
          print(f"  {dataset}: {len(all_rows)} rows so far")

      print(f"Individuals buying online: {len(all_rows)} rows total")
      return pd.DataFrame(all_rows)


def fetch_consumer_confidence():
   """
   Eurostat ei_bsco_m - consumer confidence indicator, monthly.
   """
   print("Fetching consumer confidence (ei_bsco_m)...")
   url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bsco_m"
   params = {
       "format": "JSON",
       "lang": "EN",
       "geo": COUNTRIES,
       "indic": "BS-CSMCI",
       "s_adj": "SA"
   }
   r = requests.get(url, params=params, timeout=60)
   r.raise_for_status()
   data = r.json()

   time_periods = list(data["dimension"]["time"]["category"]["index"].keys())
   geo_index    = data["dimension"]["geo"]["category"]["index"]
   values       = data["value"]
   n_times      = len(time_periods)

   rows = []
   for geo_code, geo_idx in geo_index.items():
       if geo_code not in COUNTRIES:
           continue
       for t_idx, period in enumerate(time_periods):
           val = values.get(str(geo_idx * n_times + t_idx))
           if val is not None:
               rows.append({
                   "country_code": geo_code,
                   "period":       period,
                   "indic":        "BS-CSMCI",
                   "pct_value":    float(val),
                   "ingested_at":  datetime.utcnow().isoformat()
               })

   print(f"Consumer confidence: {len(rows)} rows")
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
   datasets = [
       (fetch_individuals_buying_online, "eurostat_individuals_buying_online"),
       (fetch_consumer_confidence,       "eurostat_consumer_confidence"),
   ]
   for fetch_fn, table_name in datasets:
       try:
           df = fetch_fn()
           if not df.empty:
               load_to_bigquery(df, table_name)
       except Exception as e:
           print(f"Error fetching {table_name}: {e}")


if __name__ == "__main__":
   main()
