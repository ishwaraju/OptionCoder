import argparse
import csv
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config import Config
from shared.indicators.atr import ATRCalculator
from shared.indicators.orb import ORB
from shared.indicators.volume_analyzer import VolumeAnalyzer
from shared.indicators.vwap import VWAPCalculator
from strategies.shared.breakout_strategy import BreakoutStrategy


def parse_args():
    parser = argparse.ArgumentParser(description="Replay strategy using candles + oi_snapshots fallback only.")
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--instrument", default=Config.SYMBOL, help="Instrument")
    parser.add_argument("--aggressive", action="store_true", help="Enable AGGRESSIVE_MODE")
    parser.add_argument("--show-signals", action="store_true", help="Print signal rows")
    return parser.parse_args()


def psql(query):
    result = subprocess.run(
        ["psql", Config.get_db_dsn(), "-At", "-F", "\t", "-c", query],
        check=True,
        capture_output=True,
        text=True,
    )
    lines = result.stdout.strip().splitlines()
    if not lines or lines == [""]:
        return []
    return [next(csv.reader([line], delimiter="\t")) for line in lines]


def load_candles(instrument, from_date, to_date):
    query = f"""
    SELECT ts AT TIME ZONE 'Asia/Kolkata', open, high, low, close, volume
    FROM candles_5m
    WHERE instrument='{instrument}'
      AND DATE(ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY ts;
    """
    candles = []
    for r in psql(query):
        candles.append(
            {
                "time": datetime.fromisoformat(r[0]),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": int(r[5]),
            }
        )
    return candles


def load_snapshots(instrument, from_date, to_date):
    query = f"""
    SELECT
      ts AT TIME ZONE 'Asia/Kolkata',
      underlying_price, ce_oi, pe_oi, pcr,
      ce_oi_change, pe_oi_change, oi_sentiment, oi_trend,
      support_level, resistance_level, volume_pcr
    FROM oi_snapshots_1m
    WHERE instrument='{instrument}'
      AND DATE(ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY ts;
    """
    snapshots = []
    for r in psql(query):
        snapshots.append(
            {
                "ts": datetime.fromisoformat(r[0]),
                "underlying_price": float(r[1]) if r[1] else None,
                "ce_oi": int(r[2]) if r[2] else 0,
                "pe_oi": int(r[3]) if r[3] else 0,
                "pcr": float(r[4]) if r[4] else 0.0,
                "ce_oi_change": int(r[5]) if r[5] else 0,
                "pe_oi_change": int(r[6]) if r[6] else 0,
                "oi_sentiment": r[7],
                "oi_trend": r[8],
                "support_level": float(r[9]) if r[9] else None,
                "resistance_level": float(r[10]) if r[10] else None,
                "volume_pcr": float(r[11]) if r[11] else 0.0,
            }
        )
    return snapshots


def latest_snapshot_before(snapshots, ts):
    latest = None
    for snapshot in snapshots:
        if snapshot["ts"] <= ts:
            latest = snapshot
        else:
            break
    return latest


def can_trade_at(ts):
    trade_start = datetime.strptime(Config.TRADE_START_TIME, "%H:%M").time()
    trade_end = datetime.strptime(Config.TRADE_END_TIME, "%H:%M").time()
    no_trade_start = datetime.strptime(Config.NO_TRADE_START, "%H:%M").time()
    no_trade_end = datetime.strptime(Config.NO_TRADE_END, "%H:%M").time()
    now = ts.time()
    return trade_start <= now <= trade_end and not (no_trade_start <= now <= no_trade_end)


def derive_context(snapshot, price):
    if not snapshot:
        return ("NEUTRAL", None, None, None, None)

    oi_bias = "NEUTRAL"
    if snapshot["oi_sentiment"] == "BULLISH":
        oi_bias = "BULLISH"
    elif snapshot["oi_sentiment"] == "BEARISH":
        oi_bias = "BEARISH"

    if oi_bias == "BULLISH" or (snapshot["pcr"] > 1 and snapshot["volume_pcr"] >= 1):
        oi_trend = "BULLISH"
    elif oi_bias == "BEARISH" or (snapshot["pcr"] < 1 and snapshot["volume_pcr"] < 1):
        oi_trend = "BEARISH"
    else:
        oi_trend = "NEUTRAL"

    build_up = None
    reference_price = snapshot["underlying_price"]
    price_change = 0 if reference_price is None else price - reference_price
    if price_change > 0:
        if snapshot["pe_oi_change"] > 0:
            build_up = "LONG_BUILDUP"
        elif snapshot["ce_oi_change"] < 0:
            build_up = "SHORT_COVERING"
    elif price_change < 0:
        if snapshot["ce_oi_change"] > 0:
            build_up = "SHORT_BUILDUP"
        elif snapshot["pe_oi_change"] < 0:
            build_up = "LONG_UNWINDING"

    support = snapshot["support_level"] if snapshot["support_level"] and snapshot["support_level"] > 0 else None
    resistance = snapshot["resistance_level"] if snapshot["resistance_level"] and snapshot["resistance_level"] > 0 else None
    return (oi_bias, oi_trend, build_up, support, resistance)


def main():
    args = parse_args()
    Config.TEST_MODE = False
    Config.AGGRESSIVE_MODE = bool(args.aggressive)

    candles = load_candles(args.instrument, args.from_date, args.to_date)
    snapshots = load_snapshots(args.instrument, args.from_date, args.to_date)

    strategy = BreakoutStrategy()
    vwap = VWAPCalculator()
    atr = ATRCalculator()
    orb = ORB()
    volume = VolumeAnalyzer()
    blockers = Counter()
    signals = []
    by_day = defaultdict(int)

    for candle in candles:
        ts = candle["time"]
        strategy.time_utils.now_ist = lambda ts=ts: ts
        strategy.time_utils.current_time = lambda ts=ts: ts.time()
        strategy.expiry_rules.time_utils.now_ist = lambda ts=ts: ts
        strategy.expiry_rules.time_utils.current_time = lambda ts=ts: ts.time()

        vwap_value = vwap.update(candle)
        atr_value = atr.update(candle)
        volume.update(candle)
        volume_signal = volume.get_volume_signal(candle["volume"])
        orb.add_candle(candle)

        if orb.is_orb_ready():
            orb_high, orb_low = orb.get_orb_levels()
        else:
            orb_high, orb_low = orb.calculate_orb()

        snapshot = latest_snapshot_before(snapshots, ts)
        oi_bias, oi_trend, build_up, support, resistance = derive_context(snapshot, candle["close"])

        signal, reason = strategy.generate_signal(
            price=candle["close"],
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap_value,
            atr=atr_value,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_trend,
            build_up=build_up,
            support=support,
            resistance=resistance,
            can_trade=can_trade_at(ts),
            buffer=atr.get_buffer(),
            pressure_metrics=None,
            candle_high=candle["high"],
            candle_low=candle["low"],
            candle_close=candle["close"],
            candle_open=candle["open"],
            candle_tick_count=5,
            candle_time=ts,
            candle_volume=candle["volume"],
            expiry=None,
        )

        blockers.update(strategy.last_blockers)
        if signal:
            signals.append((ts, signal, strategy.last_signal_type, strategy.last_score, reason))
            by_day[ts.date().isoformat()] += 1

    print("OI-Only Replay Summary")
    print(f"Instrument: {args.instrument}")
    print(f"Date Range: {args.from_date} to {args.to_date}")
    print(f"Decisions: {len(candles)}")
    print(f"Signals: {len(signals)}")
    print()
    print("Signals By Day")
    for day in sorted({c['time'].date().isoformat() for c in candles}):
        print(f"{day}: {by_day.get(day, 0)}")
    print()
    print("Top Blockers")
    for blocker, count in blockers.most_common(10):
        print(f"{blocker}: {count}")

    if args.show_signals:
        print()
        print("Signals")
        for signal in signals:
            print(f"{signal[0]} | {signal[1]} | {signal[2]} | score={signal[3]} | {signal[4]}")


if __name__ == "__main__":
    main()
