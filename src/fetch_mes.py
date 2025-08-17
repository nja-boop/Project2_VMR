# src/fetch_mes.py
import os, pathlib
import pandas as pd
from dotenv import load_dotenv
import databento as db

DATA_RAW = pathlib.Path("data/raw/m1")
TZ = "America/New_York"

def _extract_timestamp(df: pd.DataFrame) -> pd.Series:
    # Databento OHLCV-1m usually has ts_event; sometimes it's the index.
    if "ts_event" in df.columns:
        return pd.to_datetime(df["ts_event"], utc=True)
    if df.index.name == "ts_event":
        return pd.to_datetime(df.index.to_series(), utc=True)
    # Fallbacks (rare)
    for cand in ("timestamp", "time", "ts_recv"):
        if cand in df.columns:
            return pd.to_datetime(df[cand], utc=True)
    raise KeyError(f"No time column found. Columns: {list(df.columns)}; index: {df.index.name}")

def fetch_mes_1m(start: str, end: str, symbol: str = "MES.c.0") -> pd.DataFrame:
    load_dotenv()
    client = db.Historical(os.getenv("DATABENTO_API_KEY"))
    recs = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        symbols=symbol,
        stype_in="continuous",
        start=pd.Timestamp(start),
        end=pd.Timestamp(end),
    )
    df = recs.to_df()

    # Ensure timestamp column exists
    ts = _extract_timestamp(df).dt.tz_convert(TZ)
    df = df.reset_index(drop=False)  # harmless even if index wasn’t time
    df.insert(0, "timestamp", ts.values)  # put timestamp as first column

    # Standardize cols we care about (some providers add extra fields)
    keep = ["timestamp", "open", "high", "low", "close", "volume"]
    for col in keep:
        if col not in df.columns:
            # Some schemas use 'vol' instead of 'volume' (unlikely here, but safe)
            if col == "volume" and "vol" in df.columns:
                df["volume"] = df["vol"]
            else:
                df[col] = pd.NA
    df["symbol"] = "MES"
    return df[["timestamp", "symbol", "open", "high", "low", "close", "volume"]]

def save_by_day_parquet(df: pd.DataFrame):
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    for d, g in df.groupby(df["timestamp"].dt.date):
        out = DATA_RAW / f"mes_m1_{d}.parquet"
        g.to_parquet(out, index=False)

if __name__ == "__main__":
    # Start with a 1-day smoke test so you can confirm everything works
    df = fetch_mes_1m("2025-08-12", "2025-08-13")
    save_by_day_parquet(df)
    print("Saved:", *[p.name for p in DATA_RAW.glob("mes_m1_*.parquet")])
