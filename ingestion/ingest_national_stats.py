import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime


PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"

def fetch_scb_sweden():
    """Fetch Sweden retail trade index from Eurostat sts_trtu_m (SCB API deprecated)."""
    print("Fetching Sweden retail data via Eurostat sts_trtu_m...")
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_trtu_m"
    params = {
        "geo": "SE",
        "s_adj": "NSA",
        "unit": "I21",
        "nace_r2": "G47",
        "format": "JSON",
        "lang": "en"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    time_index = data["dimension"]["time"]["category"]["index"]
    values = data["value"]
    rows = []
    for period, pos in time_index.items():
        value = values.get(str(pos))
        if value is not None:
            rows.append({
                "country_code": "SE",
                "period": period.replace("M", "-"),
                "index_value": float(value),
                "source": "SCB"
            })
    print(f"SCB (via Eurostat): {len(rows)} rows")
    return pd.DataFrame(rows)

def fetch_statfin_finland():
    """Fetch retail trade index from Statistics Finland (StatFin)."""
    print("Fetching StatFin Finland retail data...")
    base = "https://statfin.stat.fi/PXWeb/api/v1/en/StatFin/kau/vkm/"
    r = requests.get(base, timeout=30)
    tables = r.json()
    table_id = tables[0]["id"] if tables else None
    if not table_id:
        print("StatFin: no table found")
        return pd.DataFrame()
    url = base + table_id
    meta = requests.get(url, timeout=30).json()
    variables = {v["code"]: v["values"] for v in meta.get("variables", [])}

    # Auto-detect retail trade code (G47 preferred, else first available)
    toimiala_vals = variables.get("Toimiala", [])
    retail_code = "G47" if "G47" in toimiala_vals else toimiala_vals[0]
    print(f"StatFin: using Toimiala='{retail_code}' (available: {toimiala_vals[:6]})")

    muuttuja_vals = variables.get("Muuttuja", [])
    tiedot_vals = variables.get("Tiedot", [])

    payload = {
        "query": [
            {"code": "Toimiala", "selection": {"filter": "item", "values": [retail_code]}},
            {"code": "Muuttuja", "selection": {"filter": "item", "values": [muuttuja_vals[0]]}},
            {"code": "Tiedot", "selection": {"filter": "item", "values": [tiedot_vals[0]]}},
            {"code": "Kuukausi", "selection": {"filter": "all", "values": ["*"]}}
        ],
        "response": {"format": "json-stat2"}
    }
    r = requests.post(url, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    periods = list(data["dimension"]["Kuukausi"]["category"]["index"].keys())
    values = data.get("value", [])
    rows = []
    for period, value in zip(periods, values):
        if value is not None:
            rows.append({
                "country_code": "FI",
                "period": period,
                "index_value": float(value),
                "source": "StatFin"
            })
    print(f"StatFin: {len(rows)} rows")
    return pd.DataFrame(rows)


def fetch_ssb_norway():
    """Fetch retail trade index from Statistics Norway (SSB). Table 07129: 2000-present."""
    print("Fetching SSB Norway retail data...")
    url = "https://data.ssb.no/api/v0/en/table/07129"
    payload = {
        "query": [
            {"code": "NACE", "selection": {"filter": "item", "values": ["47"]}},
            {"code": "ContentsCode", "selection": {"filter": "item", "values": ["VolumUjustert"]}},
            {"code": "Tid", "selection": {"filter": "all", "values": ["*"]}}
        ],
        "response": {"format": "json-stat2"}
    }
    r = requests.post(url, json=payload, timeout=30)
    data = r.json()
    periods = list(data["dimension"]["Tid"]["category"]["index"].keys())
    values = data["value"]
    rows = []
    for period, value in zip(periods, values):
        if value is not None:
            rows.append({
                "country_code": "NO",
                "period": period,
                "index_value": float(value),
                "source": "SSB"
            })
    print(f"SSB: {len(rows)} rows")
    return pd.DataFrame(rows)


def fetch_dst_denmark():
    """Fetch retail trade index from Statistics Denmark (DST). Table DETA212A: 2015-present."""
    print("Fetching DST Denmark retail data...")
    url = "https://api.statbank.dk/v1/data"
    payload = {
        "table": "DETA212A",
        "format": "JSONSTAT",
        "variables": [
            {"code": "BRANCHEDB25UDVALG", "values": ["G47"]},
            {"code": "INDEKSTYPE", "values": ["MAENGDE"]},
            {"code": "Tid", "values": ["*"]}
        ]
    }
    r = requests.post(url, json=payload, timeout=30)
    data = r.json()
    dataset = data.get("dataset", data)
    periods = list(dataset["dimension"]["Tid"]["category"]["index"].keys())
    values = dataset["value"]
    rows = []
    for period, value in zip(periods, values):
        if value is not None:
            rows.append({
                "country_code": "DK",
                "period": period,
                "index_value": float(value),
                "source": "DST"
            })
    print(f"DST: {len(rows)} rows")
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
    dfs = []
    for fetch_fn in [fetch_scb_sweden, fetch_ssb_norway, fetch_dst_denmark, fetch_statfin_finland]:
        try:
            df = fetch_fn()
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"Error in {fetch_fn.__name__}: {e}")
    
    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        df_all["ingested_at"] = datetime.utcnow().isoformat()
        print(f"Total rows: {len(df_all)}")
        load_to_bigquery(df_all, "national_stats_retail_trade")


    if __name__ == "__main__":
    main()
