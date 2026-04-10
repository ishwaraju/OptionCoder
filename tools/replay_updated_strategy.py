import argparse
import csv
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config import Config
from core.oi_ladder import OILadder
from core.pressure_analyzer import PressureAnalyzer
from core.volume_analyzer import VolumeAnalyzer
from strategy.breakout_strategy import BreakoutStrategy


def parse_args():
    parser = argparse.ArgumentParser(description="Replay updated strategy on historical DB data.")
    parser.add_argument("--from-date", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--to-date", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--instrument", default=Config.SYMBOL, help="Instrument")
    parser.add_argument("--test-mode", action="store_true", help="Use TEST_MODE strategy branch during replay")
    parser.add_argument("--aggressive", action="store_true", help="Enable AGGRESSIVE_MODE during replay")
    parser.add_argument("--disable-continuation", action="store_true", help="Ignore continuation-style signals during replay")
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


def can_trade_at(ts):
    trade_start = datetime.strptime(Config.TRADE_START_TIME, "%H:%M").time()
    trade_end = datetime.strptime(Config.TRADE_END_TIME, "%H:%M").time()
    no_trade_start = datetime.strptime(Config.NO_TRADE_START, "%H:%M").time()
    no_trade_end = datetime.strptime(Config.NO_TRADE_END, "%H:%M").time()
    now = ts.time()
    return trade_start <= now <= trade_end and not (no_trade_start <= now <= no_trade_end)


def load_decision_rows(instrument, from_date, to_date):
    query = f"""
    SELECT
      d.ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist,
      d.price,
      d.orb_high,
      d.orb_low,
      d.vwap,
      d.atr,
      c.open,
      c.high,
      c.low,
      c.close,
      c.volume,
      LEAD(c.close, 1) OVER (
        PARTITION BY DATE(d.ts AT TIME ZONE 'Asia/Kolkata')
        ORDER BY d.ts
      ) AS next_close_1,
      LEAD(c.close, 2) OVER (
        PARTITION BY DATE(d.ts AT TIME ZONE 'Asia/Kolkata')
        ORDER BY d.ts
      ) AS next_close_2
    FROM strategy_decisions_5m d
    JOIN candles_5m c
      ON c.instrument = d.instrument
     AND c.ts = d.ts
    WHERE d.instrument = '{instrument}'
      AND DATE(d.ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY d.ts ASC;
    """
    raw = psql(query)
    rows = []
    for r in raw:
        r = r + [None] * (13 - len(r))
        rows.append(
            {
                "ts": datetime.fromisoformat(r[0]),
                "price": parse_float(r[1]),
                "orb_high": parse_float(r[2]),
                "orb_low": parse_float(r[3]),
                "vwap": parse_float(r[4]),
                "atr": parse_float(r[5]),
                "open": parse_float(r[6]),
                "high": parse_float(r[7]),
                "low": parse_float(r[8]),
                "close": parse_float(r[9]),
                "volume": parse_int(r[10]) or 0,
                "next_close_1": parse_float(r[11]),
                "next_close_2": parse_float(r[12]),
            }
        )
    return rows


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
    grouped = {}
    for r in psql(query):
        r = r + [None] * (9 - len(r))
        ts = datetime.fromisoformat(r[0])
        grouped.setdefault(ts, []).append(
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
    candidates = [snapshot_ts for snapshot_ts in snapshot_map.keys() if snapshot_ts <= ts]
    if not candidates:
        return None
    return snapshot_map[max(candidates)]


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
    }


def replay(rows, snapshot_map, disable_continuation=False):
    strategy = BreakoutStrategy()
    oi_ladder = OILadder()
    pressure = PressureAnalyzer()
    volume = VolumeAnalyzer()
    signals = []
    last_signal = None
    signal_cooldown_remaining = 0
    prev_price = None

    for row in rows:
        ts = row["ts"]
        strategy.time_utils.now_ist = lambda ts=ts: ts
        strategy.time_utils.current_time = lambda ts=ts: ts.time()
        strategy.expiry_rules.time_utils.now_ist = lambda ts=ts: ts
        strategy.expiry_rules.time_utils.current_time = lambda ts=ts: ts.time()

        volume.update({"time": ts, "volume": row["volume"]})
        volume_signal = volume.get_volume_signal(row["volume"])

        band_rows = find_latest_band_snapshot(snapshot_map, ts)
        option_data = to_option_data(band_rows)

        oi_ladder_data = None
        pressure_metrics = None
        oi_bias = "NEUTRAL"
        if option_data:
            price_change = 0 if prev_price is None else row["price"] - prev_price
            prev_price = row["price"]
            oi_ladder_data = oi_ladder.analyze(
                option_data["ce_oi_ladder"],
                option_data["pe_oi_ladder"],
                price_change=price_change,
                atm=option_data["atm"],
            )
            pressure_metrics = pressure.analyze(option_data)
            oi_bias = oi_ladder_data["trend"] if oi_ladder_data["trend"] in ["BULLISH", "BEARISH"] else "NEUTRAL"
        else:
            prev_price = row["price"]

        tick_count = 1 if row["high"] == row["low"] else 5
        buffer = max(Config.MIN_BUFFER, min(Config.MAX_BUFFER, (row["atr"] or Config.FALLBACK_BUFFER) * Config.ATR_MULTIPLIER))

        signal, reason = strategy.generate_signal(
            price=row["price"],
            orb_high=row["orb_high"],
            orb_low=row["orb_low"],
            vwap=row["vwap"],
            atr=row["atr"],
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_ladder_data["trend"] if oi_ladder_data else None,
            build_up=oi_ladder_data["build_up"] if oi_ladder_data else None,
            support=oi_ladder_data["support"] if oi_ladder_data else None,
            resistance=oi_ladder_data["resistance"] if oi_ladder_data else None,
            can_trade=can_trade_at(ts),
            buffer=buffer,
            pressure_metrics=pressure_metrics,
            candle_high=row["high"],
            candle_low=row["low"],
            candle_close=row["close"],
            candle_open=row["open"],
            candle_tick_count=tick_count,
            candle_time=ts,
            candle_volume=row["volume"],
            expiry=None,
        )

        if disable_continuation and strategy.last_signal_type in {"CONTINUATION", "AGGRESSIVE_CONTINUATION"}:
            signal = None
            reason = f"Continuation disabled in replay | score={strategy.last_score}"

        if (
            signal
            and strategy.last_signal_type == "CONTINUATION"
            and not Config.ALLOW_CONTINUATION_ENTRY
        ):
            signal = None
            reason = f"Continuation watchlist only | score={strategy.last_score}"

        if signal and last_signal == signal and signal_cooldown_remaining > 0:
            signal = None
            signal_cooldown_remaining -= 1
        elif signal:
            last_signal = signal
            signal_cooldown_remaining = Config.SIGNAL_COOLDOWN_BARS
        elif signal_cooldown_remaining > 0:
            signal_cooldown_remaining -= 1

        if not signal:
            continue

        entry = row["close"]
        next1 = row["next_close_1"]
        next2 = row["next_close_2"]
        move1 = None if next1 is None else (next1 - entry if signal == "CE" else entry - next1)
        move2 = None if next2 is None else (next2 - entry if signal == "CE" else entry - next2)
        best = max([x for x in [move1, move2] if x is not None], default=None)

        signals.append(
            {
                "ts": ts,
                "signal": signal,
                "type": strategy.last_signal_type,
                "grade": strategy.last_signal_grade,
                "score": strategy.last_score,
                "confidence": strategy.last_confidence,
                "move1": move1,
                "move2": move2,
                "best": best,
                "reason": reason,
            }
        )

    return signals


def main():
    args = parse_args()
    original_test_mode = Config.TEST_MODE
    original_aggressive_mode = Config.AGGRESSIVE_MODE
    if not args.test_mode:
        Config.TEST_MODE = False
    Config.AGGRESSIVE_MODE = args.aggressive
    rows = load_decision_rows(args.instrument, args.from_date, args.to_date)
    snapshot_map = load_option_band_rows(args.instrument, args.from_date, args.to_date)
    signals = replay(rows, snapshot_map, disable_continuation=args.disable_continuation)
    Config.TEST_MODE = original_test_mode
    Config.AGGRESSIVE_MODE = original_aggressive_mode

    print("Replay Summary")
    print("Instrument:", args.instrument)
    print("Date Range:", args.from_date, "to", args.to_date)
    print("Replay Mode:", "TEST" if args.test_mode else "REAL")
    print("Aggressive Mode:", args.aggressive)
    print("Continuation Enabled:", not args.disable_continuation)
    print("Signals:", len(signals))
    print("By Type:", dict(Counter(s["type"] for s in signals)))
    print("By Grade:", dict(Counter(s["grade"] for s in signals)))

    profitable = [s for s in signals if s["best"] is not None and s["best"] > 0]
    strong = [s for s in signals if s["best"] is not None and s["best"] >= 20]
    print("Profitable (>0):", len(profitable), "/", len([s for s in signals if s["best"] is not None]))
    print("Strong profitable (>=20 pts):", len(strong), "/", len([s for s in signals if s["best"] is not None]))
    print()
    for s in signals:
        print(
            s["ts"],
            s["signal"],
            s["type"],
            s["grade"],
            f"score={s['score']}",
            f"conf={s['confidence']}",
            f"next1={None if s['move1'] is None else round(s['move1'], 2)}",
            f"next2={None if s['move2'] is None else round(s['move2'], 2)}",
            s["reason"],
        )


if __name__ == "__main__":
    main()
