#!/usr/bin/env python3
"""Validate high-expectancy buckets on replayed historical sessions."""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
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
from services.signal_service_support.option_signal_guard import OptionSignalGuard
from strategies.shared.breakout_strategy import BreakoutStrategy
from strategies.shared.strike_selector import StrikeSelector
from tools.replay_updated_strategy import (
    build_option_participation_metrics,
    can_trade_at,
    derive_15m_trend_from_5m,
    derive_option_volume_signal,
    find_latest_band_snapshot,
    find_previous_band_snapshot,
    live_gate,
    load_5m_candles,
    load_option_band_rows,
    to_option_data,
)
from tools.replay_virtual_option_outcomes import option_path, option_row_at


@dataclass
class BucketStat:
    count: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    total_mfe: float = 0.0
    total_mae: float = 0.0

    def add(self, pnl: float, mfe: float, mae: float) -> None:
        self.count += 1
        self.wins += 1 if pnl > 0 else 0
        self.total_pnl += pnl
        self.total_mfe += mfe
        self.total_mae += mae


class ReplayDbReader:
    def __init__(self, snapshot_map):
        self.snapshot_map = snapshot_map

    def fetch_option_contract_snapshot(self, instrument, strike, option_type, before_ts=None):
        if before_ts is None:
            return None
        return option_row_at(self.snapshot_map, before_ts, strike, option_type)


class ReplayServiceStub:
    def __init__(self, instrument, strategy, atr, snapshot_map):
        self.instrument = instrument
        self.strategy = strategy
        self.atr = atr
        self.db_reader = ReplayDbReader(snapshot_map)
        self.option_data = None
        self.option_sweep_context = None

    @staticmethod
    def _spread_percent(option_row):
        return OptionSignalGuard.spread_percent(option_row)

    def _entry_too_extended(self, signal, price, trigger_price, atr_value, buffer):
        return self.strategy._entry_too_extended(signal, price, trigger_price, atr_value, buffer)

    def _should_soften_option_sweep_filters(self, signal):
        return False

    def _get_option_contract_snapshot(self, strike, option_type, before_ts=None):
        if strike is None or option_type not in {"CE", "PE"}:
            return None
        band_rows = (self.option_data or {}).get("band_snapshots") or []
        if before_ts is None:
            for row in band_rows:
                if row.get("strike") == strike and row.get("option_type") == option_type:
                    return dict(row)
        return self.db_reader.fetch_option_contract_snapshot(
            instrument=self.instrument,
            strike=strike,
            option_type=option_type,
            before_ts=before_ts,
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Validate high-expectancy replay buckets.")
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--max-minutes", type=int, default=20)
    parser.add_argument("--show-details", action="store_true", help="Print each replay trade that survives guards")
    return parser.parse_args()


def avg(total, count):
    return round(total / count, 2) if count else 0.0


def replay_instrument(instrument, from_date, to_date, horizon, max_minutes):
    candles = load_5m_candles(instrument, from_date, to_date)
    snapshot_map = load_option_band_rows(instrument, from_date, to_date)
    strategy = BreakoutStrategy(instrument=instrument)
    strike_selector = StrikeSelector(instrument)
    vwap = VWAPCalculator()
    atr = ATRCalculator()
    orb = ORB()
    volume = VolumeAnalyzer()
    oi = OIAnalyzer()
    oi_ladder = OILadder()
    pressure = PressureAnalyzer()
    participation_history = {"CE": deque(maxlen=12), "PE": deque(maxlen=12)}
    service = ReplayServiceStub(instrument, strategy, atr, snapshot_map)
    stats = defaultdict(BucketStat)
    family_stats = defaultdict(BucketStat)
    details = []

    for idx, candle in enumerate(candles):
        ts = candle["time"]
        for obj in (strategy.time_utils, strategy.expiry_rules.time_utils, strike_selector.time_utils):
            obj.now_ist = lambda ts=ts: ts
            obj.current_time = lambda ts=ts: ts.time()

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
        service.option_data = option_data
        volume_signal = derive_option_volume_signal(option_data) or volume.get_volume_signal(candle["volume"])
        participation_metrics = build_option_participation_metrics(
            current_band_rows,
            previous_band_rows,
            ts,
            participation_history,
        )
        if option_data:
            oi.update(candle["close"], option_data.get("ce_oi", 0), option_data.get("pe_oi", 0))
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
            oi_bias = oi.get_bias()
        else:
            oi_ladder_data = None
            pressure_metrics = None
            oi_bias = "NEUTRAL"

        recent_candles_5m = candles[max(0, idx - 23):idx + 1]
        signal, _ = strategy.generate_trade_signal(
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

        old_actionable, _ = live_gate(signal, strategy, instrument, ts)
        if not old_actionable:
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
            candle_time=ts,
        )
        selected = option_row_at(snapshot_map, ts, strike, signal)
        if not selected:
            continue
        premium_guard = OptionSignalGuard.evaluate_premium_quality_guard(service, signal, selected, ts)
        if not premium_guard or (premium_guard.get("label") or "").upper() != "PREMIUM_OK":
            continue

        profile = OptionSignalGuard.assess_high_expectancy(
            service,
            signal,
            ts,
            selected_option_contract=selected,
            premium_guard=premium_guard,
            price=candle["close"],
        )
        if profile.get("watch_only") or not profile.get("allow_trade"):
            continue

        path = option_path(snapshot_map, ts, strike, signal, max_minutes)
        if not path:
            continue
        horizon_ts = ts + timedelta(minutes=horizon)
        horizon_row = option_row_at(snapshot_map, horizon_ts, strike, signal)
        if not horizon_row:
            continue
        entry_ltp = float(selected.get("ltp") or 0)
        horizon_ltp = float(horizon_row.get("ltp") or 0)
        if entry_ltp <= 0:
            continue
        ltps = [float(row.get("ltp") or 0) for _, row in path]
        pnl = horizon_ltp - entry_ltp
        mfe = max(ltps) - entry_ltp
        mae = min(ltps) - entry_ltp
        bucket = profile.get("quality_tag") or "UNKNOWN"
        family = profile.get("signal_family") or "UNKNOWN"
        stats[bucket].add(pnl, mfe, mae)
        family_stats[family].add(pnl, mfe, mae)
        details.append(
            {
                "ts": ts,
                "instrument": instrument,
                "signal": signal,
                "strike": strike,
                "entry_ltp": entry_ltp,
                "horizon_ltp": horizon_ltp,
                "pnl": pnl,
                "mfe": mfe,
                "mae": mae,
                "spot": candle["close"],
                "setup": strategy.last_signal_type,
                "grade": strategy.last_signal_grade,
                "confidence": strategy.last_confidence,
                "score": strategy.last_score,
                "bucket": bucket,
                "family": family,
                "premium_guard": premium_guard.get("label"),
                "reason": profile.get("reason"),
            }
        )

    return {"bucket_stats": stats, "family_stats": family_stats, "details": details}


def main():
    args = parse_args()
    instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]
    all_stats = {instrument: replay_instrument(instrument, args.from_date, args.to_date, args.horizon, args.max_minutes) for instrument in instruments}
    combined = defaultdict(BucketStat)
    combined_family = defaultdict(BucketStat)
    for instrument, stats in all_stats.items():
        print(instrument)
        bucket_stats = stats["bucket_stats"]
        family_stats = stats["family_stats"]
        for bucket in ["HQ", "PA_STRONG_ENTER_SMALL", "RQ", "TQ_CLEAN", "TQ_VOLATILE", "LQ", "AVOID"]:
            stat = bucket_stats.get(bucket)
            if not stat or not stat.count:
                continue
            print(
                f"  {bucket}: count={stat.count} win_rate={round((stat.wins / stat.count) * 100, 1)}% "
                f"avg_5m={avg(stat.total_pnl, stat.count)} avg_mfe={avg(stat.total_mfe, stat.count)} avg_mae={avg(stat.total_mae, stat.count)}"
            )
            combined[bucket].count += stat.count
            combined[bucket].wins += stat.wins
            combined[bucket].total_pnl += stat.total_pnl
            combined[bucket].total_mfe += stat.total_mfe
            combined[bucket].total_mae += stat.total_mae
        if args.show_details and stats["details"]:
            print("  DETAILS")
            for row in stats["details"]:
                print(
                    f"    {row['ts']:%Y-%m-%d %H:%M} {row['instrument']} {row['signal']} "
                    f"{row['strike']} entry={row['entry_ltp']:.2f} spot={row['spot']:.2f} "
                    f"5m={row['pnl']:+.2f} mfe={row['mfe']:+.2f} mae={row['mae']:+.2f} "
                    f"{row['bucket']}/{row['family']} {row['setup']} grade={row['grade']} "
                    f"score={row['score']} guard={row['premium_guard']}"
                )
        if family_stats:
            print("  FAMILIES")
            for family, stat in sorted(family_stats.items(), key=lambda item: (-item[1].count, item[0])):
                print(
                    f"    {family}: count={stat.count} win_rate={round((stat.wins / stat.count) * 100, 1)}% "
                    f"avg_5m={avg(stat.total_pnl, stat.count)} avg_mfe={avg(stat.total_mfe, stat.count)} avg_mae={avg(stat.total_mae, stat.count)}"
                )
                combined_family[family].count += stat.count
                combined_family[family].wins += stat.wins
                combined_family[family].total_pnl += stat.total_pnl
                combined_family[family].total_mfe += stat.total_mfe
                combined_family[family].total_mae += stat.total_mae
    print("ALL")
    for bucket in ["HQ", "PA_STRONG_ENTER_SMALL", "RQ", "TQ_CLEAN", "TQ_VOLATILE", "LQ", "AVOID"]:
        stat = combined.get(bucket)
        if not stat or not stat.count:
            continue
        print(
            f"  {bucket}: count={stat.count} win_rate={round((stat.wins / stat.count) * 100, 1)}% "
            f"avg_5m={avg(stat.total_pnl, stat.count)} avg_mfe={avg(stat.total_mfe, stat.count)} avg_mae={avg(stat.total_mae, stat.count)}"
        )
    if combined_family:
        print("  FAMILIES")
        for family, stat in sorted(combined_family.items(), key=lambda item: (-item[1].count, item[0])):
            print(
                f"    {family}: count={stat.count} win_rate={round((stat.wins / stat.count) * 100, 1)}% "
                f"avg_5m={avg(stat.total_pnl, stat.count)} avg_mfe={avg(stat.total_mfe, stat.count)} avg_mae={avg(stat.total_mae, stat.count)}"
            )


if __name__ == "__main__":
    main()
