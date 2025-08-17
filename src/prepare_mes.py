# src/prepare_mes.py
import pandas as pd
import numpy as np
from pathlib import Path

IN_PATH  = Path("data/processed/mes_all.parquet")           # input
OUT_PATH = Path("data/processed/mes_all_prepared.parquet")  # output
TZ = "America/New_York"
RTH_START, RTH_END = "09:30", "16:00"   # Regular Trading Hours
MAINT_HOUR = 17                         # 17:00–17:59 ET maintenance break
ROLL_WIN = 15                           # 15-minute rolling volatility (in 1-min bars)

def add_session_tags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    ts = df["timestamp"].dt.tz_convert(TZ)
    # drop maintenance break
    df = df[ts.dt.hour != MAINT_HOUR]
    # session tag
    tstr = ts.dt.strftime("%H:%M")
    df["session"] = np.where((tstr >= RTH_START) & (tstr < RTH_END), "RTH", "ETH")
    # convenience date column for grouping
    df["date"] = ts.dt.date
    return df

def add_vwap(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pvt"] = df["close"] * df["volume"]
    grp = df.groupby(["date", "session"], sort=False)
    cum_pvt = grp["pvt"].cumsum()
    cum_vol = grp["volume"].cumsum()
    df["vwap"] = (cum_pvt / cum_vol.replace(0, np.nan))
    return df.drop(columns=["pvt"])

def add_volatility(df: pd.DataFrame, win: int = ROLL_WIN) -> pd.DataFrame:
    df = df.copy()
    # 1-minute close-to-close returns
    df["ret_1m"] = df.groupby(["date", "session"], sort=False)["close"].pct_change()
    # rolling std of returns within each date/session
    df["vol_15m"] = (
        df.groupby(["date", "session"], sort=False)["ret_1m"]
          .rolling(win, min_periods=max(5, win//3))
          .std()
          .reset_index(level=[0,1], drop=True)
    )
    return df

def main():
    IN_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(IN_PATH)
    # Ensure timestamp is tz-aware (your fetch made it NY already, but this is safe)
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(TZ)

    df = add_session_tags(df)
    df = add_vwap(df)
    df = add_volatility(df)

    # Keep a tidy column order
    cols = ["timestamp","symbol","open","high","low","close","volume",
            "session","date","vwap","ret_1m","vol_15m"]
    df = df[cols]

    df.to_parquet(OUT_PATH, index=False)
    print(f"Saved: {OUT_PATH}  rows={len(df):,}")

if __name__ == "__main__":
    main()

    