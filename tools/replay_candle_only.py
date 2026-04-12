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
    parser = argparse.ArgumentParser(description="Replay candle-only strategy view using 5m candles only.")
    parser.add_argument("--date", required=True, help="Trading date in YYYY-MM-DD")
    parser.add_argument(
        "--instrument",
        default="ALL",
        help="Instrument name (NIFTY, BANKNIFTY, SENSEX) or ALL",
    )
    parser.add_argument("--show-signals", action="store_true", help="Print generated signal rows")
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


def load_candles(instrument, day):
    query = f"""
    SELECT ts AT TIME ZONE 'Asia/Kolkata', open, high, low, close, volume
    FROM candles_5m
    WHERE instrument='{instrument}'
      AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = DATE '{day}'
    ORDER BY ts;
    """
    candles = []
    for r in psql(query):
        candles.append(
            {
                "time": datetime.fromisoformat(r[0]),
                "close_time": datetime.fromisoformat(r[0]),
                "open": float(r[1]),
                "high": float(r[2]),
                "low": float(r[3]),
                "close": float(r[4]),
                "volume": int(r[5]),
            }
        )
    return candles


def can_trade_at(ts):
    trade_start = datetime.strptime(Config.TRADE_START_TIME, "%H:%M").time()
    trade_end = datetime.strptime(Config.TRADE_END_TIME, "%H:%M").time()
    no_trade_start = datetime.strptime(Config.NO_TRADE_START, "%H:%M").time()
    no_trade_end = datetime.strptime(Config.NO_TRADE_END, "%H:%M").time()
    now = ts.time()
    return trade_start <= now <= trade_end and not (no_trade_start <= now <= no_trade_end)


def derive_bias_from_price(price, vwap_value):
    if vwap_value is None:
        return "NEUTRAL", "NEUTRAL"
    if price > vwap_value:
        return "BULLISH", "BULLISH"
    if price < vwap_value:
        return "BEARISH", "BEARISH"
    return "NEUTRAL", "NEUTRAL"


def replay_instrument(instrument, day):
    candles = load_candles(instrument, day)
    strategy = BreakoutStrategy()
    vwap = VWAPCalculator()
    atr = ATRCalculator()
    orb = ORB()
    volume = VolumeAnalyzer()
    blockers = Counter()
    cautions = Counter()
    signals = []
    last_row = None

    for candle in candles:
        ts = candle["time"]
        strategy.time_utils.now_ist = lambda ts=ts: ts
        strategy.time_utils.current_time = lambda ts=ts: ts.time()
        strategy.expiry_rules.time_utils.now_ist = lambda ts=ts: ts
        strategy.expiry_rules.time_utils.current_time = lambda ts=ts: ts.time()

        orb.add_candle(candle)
        if orb.is_orb_ready():
            orb_high, orb_low = orb.get_orb_levels()
        else:
            orb_high, orb_low = orb.calculate_orb()

        vwap_value = vwap.update(candle)
        atr_value = atr.update(candle)
        volume.update(candle)
        volume_signal = volume.get_volume_signal(candle["volume"])
        oi_bias, oi_trend = derive_bias_from_price(candle["close"], vwap_value)

        signal, reason = strategy.generate_signal(
            price=candle["close"],
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap_value,
            atr=atr_value,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_trend,
            build_up=None,
            support=None,
            resistance=None,
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
        cautions.update(strategy.last_cautions)

        row = {
            "ts": ts,
            "signal": signal,
            "reason": reason,
            "score": strategy.last_score,
            "signal_type": strategy.last_signal_type,
            "grade": strategy.last_signal_grade,
            "blockers": list(strategy.last_blockers),
            "cautions": list(strategy.last_cautions),
            "price": candle["close"],
            "vwap": vwap_value,
            "orb_high": orb_high,
            "orb_low": orb_low,
        }
        last_row = row

        if signal:
            signals.append(row)

    return {
        "instrument": instrument,
        "day": day,
        "candles": candles,
        "signals": signals,
        "blockers": blockers,
        "cautions": cautions,
        "last_row": last_row,
    }


def print_result(result, show_signals=False):
    instrument = result["instrument"]
    candles = result["candles"]
    signals = result["signals"]
    blockers = result["blockers"]
    cautions = result["cautions"]
    last_row = result["last_row"]

    print(f"\n=== {instrument} ===")
    print(f"5m candles: {len(candles)}")
    print(f"Signals: {len(signals)}")

    if last_row:
        print(
            f"Latest: {last_row['ts']} | close={last_row['price']} | "
            f"signal={last_row['signal'] or 'NO_TRADE'} | type={last_row['signal_type']} | score={last_row['score']}"
        )
        print(f"Reason: {last_row['reason']}")

    if blockers:
        print("Top blockers:")
        for blocker, count in blockers.most_common(5):
            print(f"- {blocker}: {count}")

    if cautions:
        print("Top cautions:")
        for caution, count in cautions.most_common(5):
            print(f"- {caution}: {count}")

    if show_signals and signals:
        print("Signals:")
        for row in signals:
            print(
                f"- {row['ts']} | {row['signal']} | {row['signal_type']} "
                f"| score={row['score']} | {row['reason']}"
            )


def main():
    args = parse_args()
    instruments = (
        ["NIFTY", "BANKNIFTY", "SENSEX"]
        if args.instrument.upper() == "ALL"
        else [args.instrument.upper()]
    )

    print("Candle-Only Replay")
    print(f"Date: {args.date}")

    totals = defaultdict(int)
    for instrument in instruments:
        result = replay_instrument(instrument, args.date)
        print_result(result, show_signals=args.show_signals)
        totals["candles"] += len(result["candles"])
        totals["signals"] += len(result["signals"])

    if len(instruments) > 1:
        print("\nOverall")
        print(f"Total 5m candles: {totals['candles']}")
        print(f"Total signals: {totals['signals']}")


if __name__ == "__main__":
    main()
