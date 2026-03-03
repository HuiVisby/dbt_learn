import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"

FHM_URL = "https://fohm-app.folkhalsomyndigheten.se/Folkhalsodata/api/v1/sv/A_Folkhalsodata/A_Folkhalsodata__B_HLV__aLevvanor__aagL
evvanortobak/"


def fetch_fhm_sweden_demographics():
    """Fetch snus + nicotine pouch daily users by age and gender from FHM Sweden. 2004-2024."""
    print("Fetching FHM Sweden nicotine demographics...")
    payload = {
        "query": [
            {"code": "Användning av tobaks- och nikotinprodukter",
             "selection": {"filter": "item", "values": ["09", "22"]}},
            {"code": "Andel och konfidensintervall",
             "selection": {"filter": "item", "values": ["01"]}},
            {"code": "Ålder",
             "selection": {"filter": "item", "values": ["31", "32", "33", "34"]}},
            {"code": "Kön",
             "selection": {"filter": "item", "values": ["01", "02"]}},
            {"code": "År",
             "selection": {"filter": "all", "values": ["*"]}}
        ],
        "response": {"format": "json-stat2"}
    }
    r = requests.post(FHM_URL, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()

    dims  = data["dimension"]
    products = list(dims["Användning av tobaks- och nikotinprodukter"]["category"]["label"].items())
    ages     = list(dims["Ålder"]["category"]["label"].items())
    genders  = list(dims["Kön"]["category"]["label"].items())
    years    = list(dims["År"]["category"]["index"].keys())
    values   = data["value"]

    product_map = {"09": "snus", "22": "nicotine_pouch"}
    gender_map  = {"01": "female", "02": "male"}
    age_map     = {"31": "16-29", "32": "30-44", "33": "45-64", "34": "65-84"}

    rows = []
    idx  = 0
    for p_code, _ in products:
        for a_code, _ in ages:
            for g_code, _ in genders:
                for year in years:
                    val = values[idx]
                    if val is not None:
                        rows.append({
                            "country_code": "SE",
                            "year":         int(year),
                            "age_group":    age_map[a_code],
                            "gender":       gender_map[g_code],
                            "product":      product_map[p_code],
                            "pct_users":    float(val),
                            "source":       "FHM",
                            "ingested_at":  datetime.now(timezone.utc).isoformat()
                        })
                    idx += 1

    print(f"FHM Sweden demographics: {len(rows)} rows")
    return pd.DataFrame(rows)


def fetch_ssb_norway_snus_demographics():
    """Fetch daily snus users by sex and age group from SSB Norway. Table 07692: 2004-2025."""
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

    dims    = data["dimension"]
    genders = list(dims["Kjonn"]["category"]["label"].items())
    ages    = list(dims["Alder"]["category"]["label"].items())
    years   = list(dims["Tid"]["category"]["index"].keys())
    values  = data["value"]

    rows = []
    idx  = 0
    for g_code, _ in genders:
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
    # Sweden FHM — replaces manual data with full age-group breakdown
    try:
        df_se = fetch_fhm_sweden_demographics()
        if not df_se.empty:
            load_to_bigquery(df_se, "folkhalsomyndigheten_nicotine")
    except Exception as e:
        print(f"Error FHM Sweden: {e}")

    # Norway SSB
    try:
        df_no = fetch_ssb_norway_snus_demographics()
        if not df_no.empty:
            load_to_bigquery(df_no, "ssb_nicotine_demographics_no")
    except Exception as e:
        print(f"Error SSB Norway: {e}")


if __name__ == "__main__":
    main()
