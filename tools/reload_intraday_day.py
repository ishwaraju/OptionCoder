from argparse import ArgumentParser
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from shared.db.pool import DBPool
from shared.db.writer import DBWriter
from shared.market.historical_backfill import HistoricalBackfill
from shared.utils.instrument_profile import get_instrument_profile


def floor_to_5m(ts):
    return ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)


def aggregate_5m(rows):
    buckets = defaultdict(list)
    for row in rows:
        buckets[floor_to_5m(row["ts"])].append(row)

    candles = []
    for bucket_ts in sorted(buckets):
        bucket = buckets[bucket_ts]
        candles.append(
            {
                "ts": bucket_ts,
                "open": bucket[0]["open"],
                "high": max(item["high"] for item in bucket),
                "low": min(item["low"] for item in bucket),
                "close": bucket[-1]["close"],
                "volume": sum(item["volume"] for item in bucket),
            }
        )
    return candles


def main():
    parser = ArgumentParser(description="Reload one intraday day from Dhan historical API into candle tables.")
    parser.add_argument("--date", required=True, help="Trading date in YYYY-MM-DD")
    parser.add_argument("--symbol", default=Config.SYMBOL)
    parser.add_argument("--security-id", type=int, default=None)
    parser.add_argument("--exchange-segment", default="IDX_I")
    parser.add_argument("--instrument-type", default="INDEX")
    parser.add_argument("--replace-day", action="store_true", help="Delete existing 1m/5m candle rows for the day before insert")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    profile = get_instrument_profile(args.symbol)
    symbol = profile["instrument"]
    security_id = args.security_id or profile["security_id"] or Config.SECURITY_IDS.get(symbol)
    if not security_id:
        raise SystemExit(f"No security id found for symbol {symbol}")

    backfill = HistoricalBackfill()
    rows = backfill.fetch_intraday_candles(
        security_id=security_id,
        exchange_segment=args.exchange_segment,
        instrument_type=args.instrument_type,
        from_date=f"{args.date} 09:15:00",
        to_date=f"{args.date} 15:30:00",
        interval=1,
        oi=False,
    )

    normalized = []
    for row in rows:
        ts = backfill._parse_api_timestamp(row["timestamp"])
        if ts.date() != target_date:
            continue
        normalized.append(
            {
                "ts": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"]),
            }
        )

    if not normalized:
        print(f"No intraday candles returned for {args.date} {symbol} ({args.exchange_segment}/{args.instrument_type}).")
        return

    candles_5m = aggregate_5m(normalized)
    db = DBWriter()
    if not db.enabled:
        raise SystemExit("DB is not enabled or connection failed")

    if args.replace_day:
        with DBPool.connection() as conn:
            if conn is None:
                raise SystemExit("DB pool connection unavailable")
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM candles_1m WHERE instrument = %s AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s",
                    (symbol, args.date),
                )
                cur.execute(
                    "DELETE FROM candles_5m WHERE instrument = %s AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = %s",
                    (symbol, args.date),
                )

    for row in normalized:
        db.insert_candle_1m((row["ts"], symbol, row["open"], row["high"], row["low"], row["close"], row["volume"]))
    for row in candles_5m:
        db.insert_candle_5m((row["ts"], symbol, row["open"], row["high"], row["low"], row["close"], row["volume"]))

    print(
        f"Reloaded {len(normalized)} 1m candles and {len(candles_5m)} 5m candles "
        f"for {args.date} {symbol} using {args.exchange_segment}/{args.instrument_type}."
    )


if __name__ == "__main__":
    main()
