#!/usr/bin/env python3
"""Replay the 1m option leader re-entry detector on stored intraday data."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from services.signal_service import SignalService
from shared.market.option_spike_detector import OptionSpikeDetector
from tools.replay_updated_strategy import load_5m_candles, load_option_band_rows, psql
from tools.replay_virtual_option_outcomes import option_row_at


def load_1m_candles(instrument: str, day: str):
    rows = psql(
        f"""
        SELECT ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist, open, high, low, close, volume
        FROM candles_1m
        WHERE instrument = '{instrument}'
          AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = DATE '{day}'
        ORDER BY ts ASC
        """
    )
    return [
        {
            "time": datetime.fromisoformat(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": int(row[5] or 0),
        }
        for row in rows
    ]


def option_path_stats(snapshot_map, ts, strike, option_type, entry_ltp, minutes=5):
    values = []
    for minute in range(1, minutes + 1):
        row = option_row_at(snapshot_map, ts.replace(second=0, microsecond=0) + __import__("datetime").timedelta(minutes=minute), strike, option_type)
        if row:
            values.append(float(row.get("ltp") or 0.0))
    if not values:
        return None
    return values[-1] - entry_ltp, max(values) - entry_ltp, min(values) - entry_ltp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", default="NIFTY")
    parser.add_argument("--date", required=True)
    parser.add_argument("--action-only", action="store_true")
    args = parser.parse_args()

    candles_1m = load_1m_candles(args.instrument, args.date)
    candles_5m = load_5m_candles(args.instrument, args.date, args.date)
    snapshot_map = load_option_band_rows(args.instrument, args.date, args.date)
    service = SignalService.__new__(SignalService)
    service.instrument = args.instrument
    service.option_spike_detector = OptionSpikeDetector(50 if args.instrument == "NIFTY" else 100)

    for idx in range(5, len(candles_1m)):
        latest_ts = candles_1m[idx]["time"]
        recent_1m = candles_1m[idx - 5:idx + 1]
        snapshot_times = [ts for ts in snapshot_map.keys() if ts <= latest_ts]
        if len(snapshot_times) < 3:
            continue
        groups = [snapshot_map[ts] for ts in snapshot_times[-3:]]
        recent_5m = [c for c in candles_5m if c["time"] <= latest_ts][-9:]
        vwap_rows = [c for c in candles_5m if c["time"] <= latest_ts]
        if vwap_rows:
            typical = [
                (float(c["high"]) + float(c["low"]) + float(c["close"])) / 3.0
                for c in vwap_rows
            ]
            vwap_value = sum(typical) / len(typical)
            service.vwap = type("VWAPStub", (), {"get_vwap": staticmethod(lambda v=vwap_value: v)})()
        spike = SignalService._detect_option_momentum_reentry(
            service,
            recent_1m_candles=recent_1m,
            snapshot_groups=groups,
            recent_5m=recent_5m,
            oi_ladder_data=None,
        )
        if not spike:
            continue
        watch_payload = {
            "direction": spike.get("direction"),
            "signal_grade": "A",
            "confidence": "HIGH",
            "spike_context": spike,
        }
        action_ready = SignalService._fast_spike_action_ready(service, watch_payload)
        if args.action_only and not action_ready:
            continue
        strike = spike.get("leader_strike")
        direction = spike.get("direction")
        row = option_row_at(snapshot_map, latest_ts, strike, direction)
        if not row:
            continue
        entry = float(row.get("ltp") or 0.0)
        stats = option_path_stats(snapshot_map, latest_ts, strike, direction, entry)
        suffix = ""
        if stats:
            pnl, mfe, mae = stats
            suffix = f" 5m={pnl:+.2f} mfe={mfe:+.2f} mae={mae:+.2f}"
        status = "ACTION" if action_ready else "WATCH"
        print(f"{latest_ts:%Y-%m-%d %H:%M} {args.instrument} {direction} {strike} {status} entry={entry:.2f} {spike.get('summary')}{suffix}")


if __name__ == "__main__":
    main()
