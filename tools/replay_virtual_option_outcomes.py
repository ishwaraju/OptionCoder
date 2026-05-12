#!/usr/bin/env python3
"""Replay strategy signals and estimate option-premium outcomes from band snapshots."""

import argparse
import sys
from collections import Counter, deque
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from shared.indicators.atr import ATRCalculator
from shared.indicators.orb import ORB
from shared.indicators.volume_analyzer import VolumeAnalyzer
from shared.indicators.vwap import VWAPCalculator
from shared.market.oi_analyzer import OIAnalyzer
from shared.market.oi_ladder import OILadder
from shared.market.pressure_analyzer import PressureAnalyzer
from strategies.shared.breakout_strategy import BreakoutStrategy
from strategies.shared.strike_selector import StrikeSelector
from tools.replay_updated_strategy import (
    build_option_participation_metrics,
    can_trade_at,
    derive_15m_trend_from_5m,
    derive_option_volume_signal,
    find_latest_band_snapshot,
    find_previous_band_snapshot,
    load_5m_candles,
    load_option_band_rows,
    to_option_data,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Estimate replay signal option outcomes")
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--instrument", required=True, choices=["NIFTY", "BANKNIFTY", "SENSEX"])
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--max-minutes", type=int, default=20)
    return parser.parse_args()


def latest_snapshot_ts(snapshot_map, ts):
    same_day = [item for item in snapshot_map if item.date() == ts.date() and item <= ts]
    return max(same_day) if same_day else None


def option_row_at(snapshot_map, ts, strike, signal):
    snapshot_ts = latest_snapshot_ts(snapshot_map, ts)
    if snapshot_ts is None:
        return None
    return next(
        (
            row
            for row in snapshot_map[snapshot_ts]
            if row.get("strike") == strike and row.get("option_type") == signal
        ),
        None,
    )


def option_path(snapshot_map, signal_ts, strike, signal, max_minutes):
    rows = []
    for snapshot_ts in sorted(snapshot_map):
        minutes = (snapshot_ts - signal_ts).total_seconds() / 60
        if minutes < 0 or minutes > max_minutes:
            continue
        row = option_row_at(snapshot_map, snapshot_ts, strike, signal)
        if row:
            rows.append((snapshot_ts, row))
    return rows


def replay_with_outcomes(args):
    candles = load_5m_candles(args.instrument, args.from_date, args.to_date)
    snapshot_map = load_option_band_rows(args.instrument, args.from_date, args.to_date)

    strategy = BreakoutStrategy(instrument=args.instrument)
    strike_selector = StrikeSelector(args.instrument)
    vwap = VWAPCalculator()
    atr = ATRCalculator()
    orb = ORB()
    volume = VolumeAnalyzer()
    oi = OIAnalyzer()
    oi_ladder = OILadder()
    pressure = PressureAnalyzer()
    participation_history = {"CE": deque(maxlen=12), "PE": deque(maxlen=12)}

    rows = []
    for idx, candle in enumerate(candles):
        ts = candle["time"]
        strategy.time_utils.now_ist = lambda ts=ts: ts
        strategy.time_utils.current_time = lambda ts=ts: ts.time()
        strategy.expiry_rules.time_utils.now_ist = lambda ts=ts: ts
        strategy.expiry_rules.time_utils.current_time = lambda ts=ts: ts.time()
        strike_selector.time_utils.now_ist = lambda ts=ts: ts
        strike_selector.time_utils.current_time = lambda ts=ts: ts.time()

        vwap_value = vwap.update(candle)
        atr_value = atr.update(candle)
        volume.update(candle)

        orb.add_candle(candle)
        if orb.is_orb_ready():
            orb_high, orb_low = orb.get_orb_levels()
        else:
            orb_high, orb_low = orb.calculate_orb()
            if orb_high is None or orb_low is None:
                orb_high, orb_low = orb.get_fallback_levels(candles[max(0, idx - 2):idx + 1])

        current_band_rows = find_latest_band_snapshot(snapshot_map, ts)
        previous_band_rows = find_previous_band_snapshot(snapshot_map, ts)
        option_data = to_option_data(current_band_rows)
        volume_signal = derive_option_volume_signal(option_data) or volume.get_volume_signal(candle["volume"])
        participation_metrics = build_option_participation_metrics(
            current_band_rows,
            previous_band_rows,
            ts,
            participation_history,
        )

        if option_data:
            oi.update(candle["close"], option_data.get("ce_oi", 0), option_data.get("pe_oi", 0))
            oi_bias = oi.get_bias()
            oi_ladder_data = oi_ladder.analyze(
                option_data["ce_oi_ladder"],
                option_data["pe_oi_ladder"],
                price_change=oi.price_change,
                atm=option_data["atm"],
            )
            pressure_metrics = pressure.analyze(
                option_data,
                underlying_price=candle["close"],
                oi_ladder_data=oi_ladder_data,
            )
        else:
            oi_bias = "NEUTRAL"
            oi_ladder_data = None
            pressure_metrics = None

        recent_candles_5m = candles[max(0, idx - 23):idx + 1]
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
            recent_candles_5m=recent_candles_5m,
            trend_15m=derive_15m_trend_from_5m(recent_candles_5m),
            participation_metrics=participation_metrics,
        )
        if not signal:
            continue

        strike = strike_selector.select_strike(
            price=candle["close"],
            signal=signal,
            volume_signal=volume_signal,
            strategy_score=strategy.last_score or 0,
            pressure_metrics=pressure_metrics,
            cautions=strategy.last_cautions,
            option_chain_data=option_data,
            setup_type=strategy.last_signal_type,
            time_regime=strategy.last_time_regime,
        )
        entry = option_row_at(snapshot_map, ts, strike, signal)
        horizon = option_row_at(snapshot_map, ts + timedelta(minutes=args.horizon), strike, signal)
        if not entry or not horizon:
            rows.append((ts, signal, strike, strategy.last_signal_type, strategy.last_score, None, None, None, None))
            continue

        entry_ltp = float(entry.get("ltp") or 0)
        horizon_ltp = float(horizon.get("ltp") or 0)
        path = option_path(snapshot_map, ts, strike, signal, args.max_minutes)
        ltps = [float(row.get("ltp") or 0) for _, row in path] or [entry_ltp]
        pnl = horizon_ltp - entry_ltp
        mfe = max(ltps) - entry_ltp
        mae = min(ltps) - entry_ltp
        rows.append((ts, signal, strike, strategy.last_signal_type, strategy.last_score, entry_ltp, horizon_ltp, pnl, mfe, mae))

    return rows


def main():
    args = parse_args()
    rows = replay_with_outcomes(args)
    wins = sum(1 for row in rows if row[7] is not None and row[7] > 0)
    known = sum(1 for row in rows if row[7] is not None)
    print(f"{args.instrument} | signals={len(rows)} | known={known} | profitable_{args.horizon}m={wins}")
    print("time | side | strike | setup | score | entry | ltp_h | pnl | mfe | mae")
    for row in rows:
        ts, signal, strike, setup, score, entry, horizon, pnl, mfe, mae = row
        print(
            f"{ts:%H:%M} | {signal} | {strike} | {setup} | {score} | "
            f"{entry if entry is not None else '-'} | {horizon if horizon is not None else '-'} | "
            f"{round(pnl, 2) if pnl is not None else '-'} | "
            f"{round(mfe, 2) if mfe is not None else '-'} | {round(mae, 2) if mae is not None else '-'}"
        )
    by_setup = Counter()
    by_setup_wins = Counter()
    for row in rows:
        setup = row[3] or "UNKNOWN"
        by_setup[setup] += 1
        if row[7] is not None and row[7] > 0:
            by_setup_wins[setup] += 1
    if by_setup:
        print("setup_summary")
        for setup, count in by_setup.items():
            print(f"{setup}: {by_setup_wins[setup]}/{count}")


if __name__ == "__main__":
    main()
