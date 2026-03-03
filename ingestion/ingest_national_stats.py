import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime


PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"


def fetch_scb_sweden():
    """Fetch retail trade index from Statistics Sweden (SCB)."""
    print("Fetching SCB Sweden retail data...")
    url = "https://api.scb.se/OV0104/v1/doris/en/ssd/HA/HA0101/HA0101A/HA0101AKvTab"
    payload = {
        "query": [
            {"code": "SNI2007", "selection": {"filter": "item", "values": ["47"]}},
            {"code": "Tid", "selection": {"filter": "all", "values": ["*"]}}
        ],
        "response": {"format": "json"}
    }
    r = requests.post(url, json=payload, timeout=30)
    data = r.json()
    rows = []
    for item in data["data"]:
        period = item["key"][1].replace("M", "-")
        try:
            rows.append({
                "country_code": "SE",
                "period": period,
                "index_value": float(item["values"][0]),
                "source": "SCB"
            })
        except (ValueError, TypeError):
            continue
    print(f"SCB: {len(rows)} rows")
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


def fetch_statfin_finland():
    """Fetch retail trade index from Statistics Finland (StatFin). Table 14kr: 2010-present."""
    print("Fetching StatFin Finland retail data...")
    url = "https://pxdata.stat.fi/PXWeb/api/v1/en/StatFin/klv/statfin_klv_pxt_14kr.px"
    meta = requests.get(url, timeout=30).json()
    variables = meta.get("variables", [])
    query = []
    for var in variables:
        if var["code"] == "Toimiala":
            query.append({"code": "Toimiala", "selection": {"filter": "item", "values": ["G47"]}})
        elif var["code"] == "Muuttuja":
            query.append({"code": "Muuttuja", "selection": {"filter": "item", "values": ["mi"]}})
        elif var["code"] == "Tiedot":
            query.append({"code": "Tiedot", "selection": {"filter": "item", "values": ["alkuperainen"]}})
        else:
            query.append({"code": var["code"], "selection": {"filter": "all", "values": ["*"]}})
    payload = {"query": query, "response": {"format": "json-stat2"}}
    r = requests.post(url, json=payload, timeout=60)
    data = r.json()
    periods = list(data["dimension"].get(
        list(data["dimension"].keys())[-1], {}
    ).get("category", {}).get("index", {}).keys())
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
