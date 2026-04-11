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
from shared.market.oi_analyzer import OIAnalyzer
from shared.market.oi_ladder import OILadder
from shared.market.pressure_analyzer import PressureAnalyzer
from strategies.shared.breakout_strategy import BreakoutStrategy


def parse_args():
    parser = argparse.ArgumentParser(description="Replay current strategy on historical DB data.")
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--instrument", default=Config.SYMBOL, help="Instrument")
    parser.add_argument("--test-mode", action="store_true", help="Replay using TEST_MODE logic")
    parser.add_argument("--aggressive", action="store_true", help="Enable AGGRESSIVE_MODE for replay")
    parser.add_argument("--disable-continuation", action="store_true", help="Ignore continuation signals")
    parser.add_argument("--show-signals", action="store_true", help="Print each generated signal row")
    return parser.parse_args()


def psql(query):
    command = ["psql", Config.get_db_dsn(), "-At", "-F", "\t", "-c", query]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    lines = result.stdout.strip().splitlines()
    if not lines or lines == [""]:
        return []
    return [next(csv.reader([line], delimiter="\t")) for line in lines]


def parse_float(value):
    return None if value in (None, "") else float(value)


def parse_int(value):
    return None if value in (None, "") else int(value)


def load_5m_candles(instrument, from_date, to_date):
    query = f"""
    SELECT
      ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
      open, high, low, close, volume
    FROM candles_5m
    WHERE instrument = '{instrument}'
      AND DATE(ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY ts ASC;
    """
    candles = []
    for r in psql(query):
        r = r + [None] * (6 - len(r))
        candles.append(
            {
                "time": datetime.fromisoformat(r[0]),
                "close_time": datetime.fromisoformat(r[0]),
                "open": parse_float(r[1]),
                "high": parse_float(r[2]),
                "low": parse_float(r[3]),
                "close": parse_float(r[4]),
                "volume": parse_int(r[5]) or 0,
            }
        )
    return candles


def load_option_band_rows(instrument, from_date, to_date):
    query = f"""
    SELECT
      ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
      atm_strike,
      strike,
      distance_from_atm,
      option_type,
      oi,
      volume,
      ltp,
      iv
    FROM option_band_snapshots_1m
    WHERE instrument = '{instrument}'
      AND DATE(ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY ts ASC, strike ASC, option_type ASC;
    """
    grouped = defaultdict(list)
    for r in psql(query):
        r = r + [None] * (9 - len(r))
        ts = datetime.fromisoformat(r[0])
        grouped[ts].append(
            {
                "atm_strike": parse_int(r[1]),
                "strike": parse_int(r[2]),
                "distance_from_atm": parse_int(r[3]),
                "option_type": r[4],
                "oi": parse_int(r[5]) or 0,
                "volume": parse_int(r[6]) or 0,
                "ltp": parse_float(r[7]) or 0.0,
                "iv": parse_float(r[8]) or 0.0,
            }
        )
    return grouped


def find_latest_band_snapshot(snapshot_map, ts):
    same_day = [snapshot_ts for snapshot_ts in snapshot_map.keys() if snapshot_ts.date() == ts.date() and snapshot_ts <= ts]
    if not same_day:
        return None
    return snapshot_map[max(same_day)]


def to_option_data(band_rows):
    if not band_rows:
        return None

    atm = band_rows[0]["atm_strike"]
    ce_rows = [row for row in band_rows if row["option_type"] == "CE"]
    pe_rows = [row for row in band_rows if row["option_type"] == "PE"]
    ce_oi_ladder = {row["strike"]: row["oi"] for row in ce_rows}
    pe_oi_ladder = {row["strike"]: row["oi"] for row in pe_rows}
    total_ce = sum(row["oi"] for row in ce_rows)
    total_pe = sum(row["oi"] for row in pe_rows)
    pcr = round(total_pe / total_ce, 2) if total_ce else 0

    return {
        "atm": atm,
        "band_snapshots": band_rows,
        "ce_oi_ladder": ce_oi_ladder,
        "pe_oi_ladder": pe_oi_ladder,
        "pcr": pcr,
        "ce_oi": next((row["oi"] for row in ce_rows if row["distance_from_atm"] == 0), 0),
        "pe_oi": next((row["oi"] for row in pe_rows if row["distance_from_atm"] == 0), 0),
        "expiry": None,
    }


def can_trade_at(ts):
    trade_start = datetime.strptime(Config.TRADE_START_TIME, "%H:%M").time()
    trade_end = datetime.strptime(Config.TRADE_END_TIME, "%H:%M").time()
    no_trade_start = datetime.strptime(Config.NO_TRADE_START, "%H:%M").time()
    no_trade_end = datetime.strptime(Config.NO_TRADE_END, "%H:%M").time()
    now = ts.time()
    return trade_start <= now <= trade_end and not (no_trade_start <= now <= no_trade_end)


def replay(candles, snapshot_map, disable_continuation=False):
    strategy = BreakoutStrategy()
    vwap = VWAPCalculator()
    atr = ATRCalculator()
    orb = ORB()
    volume = VolumeAnalyzer()
    oi = OIAnalyzer()
    oi_ladder = OILadder()
    pressure = PressureAnalyzer()

    results = []
    blocker_counter = Counter()
    caution_counter = Counter()

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

        option_data = to_option_data(find_latest_band_snapshot(snapshot_map, ts))
        if option_data:
            oi.update(candle["close"], option_data.get("ce_oi", 0), option_data.get("pe_oi", 0))
            oi_bias = oi.get_bias()
            price_change = oi.price_change
            oi_ladder_data = oi_ladder.analyze(
                option_data["ce_oi_ladder"],
                option_data["pe_oi_ladder"],
                price_change=price_change,
                atm=option_data["atm"],
            )
            pressure_metrics = pressure.analyze(option_data)
        else:
            oi_bias = "NEUTRAL"
            oi_ladder_data = None
            pressure_metrics = None

        signal, reason = strategy.generate_signal(
            price=candle["close"],
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap_value,
            atr=atr_value,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_ladder_data["trend"] if oi_ladder_data else None,
            build_up=oi_ladder_data["build_up"] if oi_ladder_data else None,
            support=oi_ladder_data["support"] if oi_ladder_data else None,
            resistance=oi_ladder_data["resistance"] if oi_ladder_data else None,
            can_trade=can_trade_at(ts),
            buffer=atr.get_buffer(),
            pressure_metrics=pressure_metrics,
            candle_high=candle["high"],
            candle_low=candle["low"],
            candle_close=candle["close"],
            candle_open=candle["open"],
            candle_tick_count=5,
            candle_time=ts,
            candle_volume=candle["volume"],
            expiry=option_data.get("expiry") if option_data else None,
        )

        if disable_continuation and strategy.last_signal_type in {"CONTINUATION", "AGGRESSIVE_CONTINUATION"}:
            signal = None
            reason = f"Continuation disabled in replay | score={strategy.last_score}"

        blocker_counter.update(strategy.last_blockers)
        caution_counter.update(strategy.last_cautions)

        results.append(
            {
                "ts": ts,
                "signal": signal,
                "reason": reason,
                "score": strategy.last_score,
                "signal_type": strategy.last_signal_type,
                "grade": strategy.last_signal_grade,
                "confidence": strategy.last_confidence,
                "regime": strategy.last_regime,
                "blockers": list(strategy.last_blockers),
                "cautions": list(strategy.last_cautions),
            }
        )

    return results, blocker_counter, caution_counter


def print_summary(results, blocker_counter, caution_counter, args):
    total_signals = [row for row in results if row["signal"]]
    by_day = defaultdict(lambda: {"decisions": 0, "signals": 0, "max_score": 0, "signal_types": Counter()})

    for row in results:
        day = row["ts"].date().isoformat()
        by_day[day]["decisions"] += 1
        by_day[day]["max_score"] = max(by_day[day]["max_score"], row["score"] or 0)
        if row["signal"]:
            by_day[day]["signals"] += 1
            by_day[day]["signal_types"][row["signal_type"]] += 1

    print("Replay Summary")
    print(f"Instrument: {args.instrument}")
    print(f"Date Range: {args.from_date} to {args.to_date}")
    print(f"Replay Mode: {'TEST' if args.test_mode else 'REAL'}")
    print(f"Aggressive Mode: {args.aggressive}")
    print(f"Disable Continuation: {args.disable_continuation}")
    print(f"Decisions: {len(results)}")
    print(f"Signals: {len(total_signals)}")
    print()
    print("Daily Summary")
    for day in sorted(by_day):
        signal_mix = ", ".join(
            f"{signal_type}:{count}" for signal_type, count in by_day[day]["signal_types"].most_common()
        ) or "-"
        print(
            f"{day} | decisions={by_day[day]['decisions']} | "
            f"signals={by_day[day]['signals']} | max_score={by_day[day]['max_score']} | {signal_mix}"
        )

    print()
    print("Top Blockers")
    for blocker, count in blocker_counter.most_common(10):
        print(f"{blocker}: {count}")

    print()
    print("Top Cautions")
    for caution, count in caution_counter.most_common(10):
        print(f"{caution}: {count}")

    if args.show_signals:
        print()
        print("Signals")
        for row in total_signals:
            print(
                f"{row['ts']} | {row['signal']} | {row['signal_type']} | "
                f"score={row['score']} | confidence={row['confidence']} | {row['reason']}"
            )


def main():
    args = parse_args()

    Config.TEST_MODE = bool(args.test_mode)
    Config.AGGRESSIVE_MODE = bool(args.aggressive)

    candles = load_5m_candles(args.instrument, args.from_date, args.to_date)
    snapshot_map = load_option_band_rows(args.instrument, args.from_date, args.to_date)

    if not candles:
        print("No 5m candles found for requested range.")
        return

    results, blocker_counter, caution_counter = replay(
        candles,
        snapshot_map,
        disable_continuation=args.disable_continuation,
    )
    print_summary(results, blocker_counter, caution_counter, args)


if __name__ == "__main__":
    main()
