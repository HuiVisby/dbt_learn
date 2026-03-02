import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "nordic-retail-intel-2025"
DATASET = "raw_ingest"

NORDIC_CITIES = [
    {"city": "Stockholm", "country_code": "SE", "latitude": 59.3293, "longitude": 18.0686},
    {"city": "Oslo",      "country_code": "NO", "latitude": 59.9139, "longitude": 10.7522},
    {"city": "Copenhagen","country_code": "DK", "latitude": 55.6761, "longitude": 12.5683},
    {"city": "Helsinki",  "country_code": "FI", "latitude": 60.1699, "longitude": 24.9384},
]


def fetch_weather(city_config, start_date="2014-01-01", end_date="2024-12-31"):
    """Fetch daily weather from Open-Meteo historical archive API."""
    print(f"Fetching weather for {city_config['city']}...")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city_config["latitude"],
        "longitude": city_config["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "snowfall_sum",
            "wind_speed_10m_max",
            "weathercode"
        ],
        "timezone": "Europe/Stockholm"
    }
    r = requests.get(url, params=params, timeout=60)
    data = r.json()
    df = pd.DataFrame(data["daily"])
    df["city"] = city_config["city"]
    df["country_code"] = city_config["country_code"]
    df["ingested_at"] = datetime.utcnow().isoformat()
    df = df.rename(columns={"time": "date"})
    print(f"  {city_config['city']}: {len(df)} days")
    return df


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
    for city in NORDIC_CITIES:
        try:
            df = fetch_weather(city)
            dfs.append(df)
        except Exception as e:
            print(f"Error fetching weather for {city['city']}: {e}")

    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
        print(f"Total weather rows: {len(df_all)}")
        load_to_bigquery(df_all, "weather_daily")


if __name__ == "__main__":
    main()
