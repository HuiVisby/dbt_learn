import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"


def fetch_ssb_norway_snus_demographics():
    """Fetch daily snus users by sex and age from SSB Norway. Table 07692: 2004-2025."""
    print("Fetching SSB Norway snus demographics...")
    url = "https://data.ssb.no/api/v0/en/table/07692"
    payload = {
        "query": [
            {"code": "Kjonn",        "selection": {"filter": "item", "values": ["1", "2"]}},
            {"code": "Alder",        "selection": {"filter": "item", "values": ["16-24", "25-34", "35-44", "45-54", "55-64",
"65-79"]}},
            {"code": "ContentsCode", "selection": {"filter": "item", "values": ["DagSnus"]}},
            {"code": "Tid",          "selection": {"filter": "all", "values": ["*"]}}
        ],
        "response": {"format": "json-stat2"}
    }
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    dims = data["dimension"]
    genders = list(dims["Kjonn"]["category"]["label"].items())    # [("1","Males"),("2","Females")]
    ages    = list(dims["Alder"]["category"]["label"].items())
    years   = list(dims["Tid"]["category"]["index"].keys())
    values  = data["value"]

    rows = []
    idx = 0
    for g_code, g_label in genders:
        for a_code, _ in ages:
            for year in years:
                val = values[idx]
                if val is not None:
                    rows.append({
                        "country_code": "NO",
                        "year":         int(year),
                        "age_group":    a_code,
                        "gender":       "male" if g_code == "1" else "female",
                        "product":      "snus",
                        "pct_users":    float(val),
                        "source":       "SSB",
                        "ingested_at":  datetime.now(timezone.utc).isoformat()
                    })
                idx += 1

    print(f"SSB Norway snus demographics: {len(rows)} rows")
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
        df = fetch_ssb_norway_snus_demographics()
        if not df.empty:
            load_to_bigquery(df, "ssb_nicotine_demographics_no")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
