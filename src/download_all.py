# download_all.py (run from project root)
from src.fetch_mes import fetch_mes_1m, save_by_day_parquet
import pathlib

START = "2023-01-01"   # <-- set the span you want
END   = "2025-08-16"   # end is exclusive; use next day to include 2025-08-16

df = fetch_mes_1m(START, END)        # pulls the whole span at once
save_by_day_parquet(df)              # writes one parquet per day to data/raw/m1/
count = len(list(pathlib.Path("data/raw/m1").glob("mes_m1_*.parquet")))
print(f"Done. Daily files in data/raw/m1: {count}")

