import pandas as pd, pathlib

files = sorted(pathlib.Path("data/raw/m1").glob("mes_m1_*.parquet"))
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
df.sort_values("timestamp", inplace=True)

out = pathlib.Path("data/processed/mes_all.parquet")
out.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(out, index=False)

print(f"Saved {len(df)} rows to {out}")

