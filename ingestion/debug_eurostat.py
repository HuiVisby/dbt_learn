import eurostat
df = eurostat.get_data_df("sts_trtu_m")
print("Columns:", df.columns.tolist())
print("Shape:", df.shape)
if "nace_r2" in df.columns:
    print("nace_r2:", df["nace_r2"].unique().tolist())
if "indic_bt" in df.columns:
    print("indic_bt:", df["indic_bt"].unique().tolist())
if "unit" in df.columns:
    print("unit:", df["unit"].unique().tolist())
if "s_adj" in df.columns:
    print("s_adj:", df["s_adj"].unique().tolist())
