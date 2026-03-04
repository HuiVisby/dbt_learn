def fetch_trends_by_region():
    """Search interest by sub-region within each country for snus, nicotine pouches and ZYN."""
    pytrends = TrendReq(hl="en-US", tz=60)
    all_rows = []

    terms_by_country = {
        "SE": ["snus", "nikotinpåsar", "ZYN"],
        "NO": ["snus", "nikotinposer", "ZYN"],
        "DK": ["snus", "nikotinposer", "ZYN"],
        "FI": ["nuuska", "nikotiinipussit", "ZYN"],
    }

    for country, keywords in terms_by_country.items():
        for keyword in keywords:
            print(f"Fetching regional trends for {country}: '{keyword}'")
            try:
                pytrends.build_payload(
                    [keyword],
                    timeframe="2019-01-01 2025-12-31",
                    geo=country
                )
                df = pytrends.interest_by_region(resolution="REGION", inc_low_vol=True)
                if df.empty:
                    print(f"  {country} '{keyword}': no regional data")
                    continue
                df = df.reset_index()
                df.columns = ["region", "search_interest"]
                df["country_code"] = country
                df["keyword"] = keyword
                all_rows.append(df)
                print(f"  {country} '{keyword}': {len(df)} regions")
            except Exception as e:
                print(f"  {country} '{keyword}' error: {e}")
            time.sleep(3)

    if not all_rows:
        return pd.DataFrame()

    result = pd.concat(all_rows, ignore_index=True)
    result["ingested_at"] = datetime.utcnow().isoformat()
    return result
