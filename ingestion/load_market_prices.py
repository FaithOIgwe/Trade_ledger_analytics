"""
Pull daily bars from Alpaca's market data API into raw.market_prices.

Needs a (free) paper trading account. Put the keys in .env:
  ALPACA_API_KEY=...
  ALPACA_SECRET_KEY=...

Real bars overwrite any synthetic rows the generator made for the same day.
If the keys aren't set the script exits 0 and says so, so the pipeline
still runs on synthetic prices (that's what CI does).

Usage:
  python ingestion/load_market_prices.py --days 30
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg

SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V", "KO", "SPY", "QQQ"]

UPSERT = """
insert into raw.market_prices (symbol, bar_date, open, high, low, close, volume, source, loaded_at)
values (%s, %s, %s, %s, %s, %s, %s, 'alpaca', now())
on conflict (symbol, bar_date) do update set
    open = excluded.open, high = excluded.high, low = excluded.low,
    close = excluded.close, volume = excluded.volume,
    source = excluded.source, loaded_at = excluded.loaded_at
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=10)
    args = p.parse_args()

    key, secret = os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_SECRET_KEY")
    if not key or not secret:
        print("ALPACA_API_KEY / ALPACA_SECRET_KEY not set, skipping (synthetic prices stay).")
        return

    # imported here so CI doesn't need the package
    from alpaca.data.enums import DataFeed
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame

    client = StockHistoricalDataClient(key, secret)
    end = datetime.now(timezone.utc) - timedelta(minutes=20)  # free feed wants a small delay
    req = StockBarsRequest(
        symbol_or_symbols=SYMBOLS,
        timeframe=TimeFrame.Day,
        start=end - timedelta(days=args.days),
        end=end,
        feed=DataFeed.IEX,
    )
    bars = client.get_stock_bars(req).data  # {symbol: [Bar, ...]}

    rows = [
        (sym, b.timestamp.date(), b.open, b.high, b.low, b.close, int(b.volume))
        for sym, sym_bars in bars.items()
        for b in sym_bars
    ]
    if not rows:
        print("Alpaca returned no bars.", file=sys.stderr)
        return

    dsn = os.getenv("SOURCE_DATABASE_URL", "postgresql://brokerage:brokerage@localhost:5432/brokerage")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.executemany(UPSERT, rows)
    print(f"loaded {len(rows)} bars for {len(bars)} symbols")


if __name__ == "__main__":
    main()
