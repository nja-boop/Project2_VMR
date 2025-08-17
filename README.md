# Project 2: Volume Weighted Average Price Mean Reversion

This project is an intraday VWAP mean reversion strategy for MES futures. The idea simply put is that when the price varries too far from the VWAP, we expect it to come back.

## Project Structure

├── data
│   └── processed
│       ├── mes_all_prepared.parquet
│       └── mes_all.parquet
├── Notebooks
│   └── phase1_data_preparation.ipynb
├── README.md
└── src
    ├── download_all.py
    ├── fetch_mes.py
    ├── merge_all.py
    └── prepare_mes.py

## Progress

### In Progress

**Phase 1 - Data Collection & Preprocessing (8/14/25)**
- Get data from databento for MES (Micro E-mini SP500) 1m OHLCV for last ~2 years
- Merge each one day file into a single dataset
- Add columns for session (RTH/ETH), VWAP (Volume Weighted Average Price), 1m returns, and 15m volatility (measured as std dev of last 15 1m returns)

### Planned

**Phase 2 - Backtest Framework Setup ()**

**Phase 3 - Strategy Implementation & Optimization ()**

**Phase 4 - Paper Trading on IBKR ()**

**Phase 5 - Live Trading ()**

**Phase 6 - Post-Trade Analysis & Refinement ()**

## Tools and Libraries

bla

## Results

bla

## Author Notes

After completing my first project, I now feel confident enough to tackle something trickier. In a way this is also somewhat of a test project. Project one aimed to ensure I had the capabilities of simply completing a project that deals with market data. Now, Project 2 aims to test if I am able to produce real results. It will start out similar to Project 1 in making the idea, optimizing, and backtesting. Then, I aim to implement it into a papertrading portfolio for further proof of concept, and eventually put my own money at stake. Upon succesful completion of this project, my next goal would be to explore more complex strategies or use more complex derrivatives like options in a new strategy.