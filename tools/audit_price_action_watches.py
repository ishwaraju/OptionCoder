#!/usr/bin/env python3
"""Audit whether price-action-led premium waits were justified on replay."""

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
from services.signal_service import SignalService
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
    load_5m_candles,
    load_option_band_rows,
    psql,
    to_option_data,
)
from tools.replay_virtual_option_outcomes import option_path, option_row_at
from tools.validate_expectancy_buckets import ReplayServiceStub


@dataclass
class WatchAuditStat:
    total: int = 0
    confirmed: int = 0
    never_confirmed: int = 0
    watch_profitable: int = 0
    confirm_profitable: int = 0
    over_wait: int = 0
    justified_wait: int = 0
    watch_total_pnl: float = 0.0
    confirm_total_pnl: float = 0.0
    edge_left_total: float = 0.0

    def record(
        self,
        watch_pnl: float,
        confirm_pnl: float | None,
        confirmed: bool,
        over_wait: bool,
    ) -> None:
        self.total += 1
        self.watch_total_pnl += watch_pnl
        if watch_pnl > 0:
            self.watch_profitable += 1
        if confirmed:
            self.confirmed += 1
            self.confirm_total_pnl += confirm_pnl or 0.0
            if (confirm_pnl or 0.0) > 0:
                self.confirm_profitable += 1
        else:
            self.never_confirmed += 1
        if over_wait:
            self.over_wait += 1
            self.edge_left_total += watch_pnl - (confirm_pnl or 0.0)
        else:
            self.justified_wait += 1


def parse_args():
    parser = argparse.ArgumentParser(description="Audit PA_STRONG_WAIT_PREMIUM replay outcomes.")
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--confirm-minutes", type=int, default=10)
    parser.add_argument("--max-minutes", type=int, default=20)
    parser.add_argument("--details", action="store_true")
    return parser.parse_args()


def avg(total: float, count: int) -> float:
    return round(total / count, 2) if count else 0.0


def load_1m_closes(instrument: str, from_date: str, to_date: str):
    query = f"""
    SELECT ts AT TIME ZONE 'Asia/Kolkata' AS ts_ist, close
    FROM candles_1m
    WHERE instrument = '{instrument}'
      AND DATE(ts AT TIME ZONE 'Asia/Kolkata') BETWEEN DATE '{from_date}' AND DATE '{to_date}'
    ORDER BY ts ASC;
    """
    closes = {}
    for row in psql(query):
        closes[datetime.fromisoformat(row[0])] = float(row[1])
    return closes


def replay_watch_audit(instrument, from_date, to_date, horizon, confirm_minutes, max_minutes):
    candles = load_5m_candles(instrument, from_date, to_date)
    snapshot_map = load_option_band_rows(instrument, from_date, to_date)
    closes_1m = load_1m_closes(instrument, from_date, to_date)
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
    service._is_option_buyer_actionable = lambda signal, candle_time=None, service=service: SignalService._is_option_buyer_actionable(
        service,
        signal,
        candle_time=candle_time,
    )
    stats = defaultdict(WatchAuditStat)
    blocker_counts = defaultdict(Counter)
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
            pressure_metrics = pressure.analyze(option_data, underlying_price=candle["close"], oi_ladder_data=oi_ladder_data)
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

        if not service._is_option_buyer_actionable(signal, candle_time=ts):
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
        profile = OptionSignalGuard.assess_high_expectancy(
            service,
            signal,
            ts,
            selected_option_contract=selected,
            premium_guard=premium_guard,
            price=candle["close"],
        )
        if profile.get("quality_tag") != "PA_STRONG_WAIT_PREMIUM":
            continue

        premium_label = (premium_guard.get("label") or "").upper() if premium_guard else "NONE"
        premium_momentum_pct = premium_guard.get("premium_momentum_pct") if premium_guard else None
        spread_pct = premium_guard.get("spread_pct") if premium_guard else None
        volume_supporting = bool((premium_guard or {}).get("volume_supporting"))
        elasticity = profile.get("elasticity") or {}
        blocker_reasons = []
        if premium_label != "PREMIUM_OK":
            blocker_reasons.append("premium_label_not_ok")
        if not volume_supporting:
            blocker_reasons.append("volume_not_supporting")
        if premium_momentum_pct is None or float(premium_momentum_pct) < 0.0:
            blocker_reasons.append("premium_momentum_negative_or_missing")
        if spread_pct is not None and float(spread_pct) > 3.5:
            blocker_reasons.append("spread_not_clean")
        if len(set(strategy.last_cautions or []).intersection({"participation_weak", "participation_delta_missing"})) > 0:
            blocker_reasons.append("hard_participation_flags")
        if len(set(strategy.last_cautions or []).intersection({"far_from_vwap", "theta_fast_exit_required", "late_day_breakdown_watch"})) > 0:
            blocker_reasons.append("late_risk_flags")
        if float(strategy.last_entry_score or 0) < 86:
            blocker_reasons.append("entry_score_below_small_entry_floor")
        if float(strategy.last_score or 0) < 82:
            blocker_reasons.append("score_below_small_entry_floor")
        if float(getattr(strategy, "last_initiative_strength_score", 0) or 0) < 34 and float((getattr(strategy, "last_futures_acceptance", {}) or {}).get("score") or 0) < 64:
            blocker_reasons.append("sponsorship_not_elite_enough")
        if bool(elasticity.get("dead_premium_risk")):
            blocker_reasons.append("dead_premium_risk")
        if not blocker_reasons:
            blocker_reasons.append("premium_confirmation_still_pending")

        family = profile.get("signal_family") or "UNKNOWN"
        blocker_counts["ALL"].update(blocker_reasons)
        blocker_counts[family].update(blocker_reasons)

        watch_entry = float(selected.get("ltp") or 0.0)
        watch_horizon = option_row_at(snapshot_map, ts + timedelta(minutes=horizon), strike, signal)
        watch_pnl = (float(watch_horizon.get("ltp") or 0.0) - watch_entry) if watch_horizon and watch_entry > 0 else 0.0

        confirmed_at = None
        confirm_entry = None
        confirm_pnl = None
        for minute in range(1, confirm_minutes + 1):
            probe_ts = ts + timedelta(minutes=minute)
            probe_row = option_row_at(snapshot_map, probe_ts, strike, signal)
            if not probe_row:
                continue
            probe_guard = OptionSignalGuard.evaluate_premium_quality_guard(service, signal, probe_row, probe_ts)
            probe_price = closes_1m.get(probe_ts, candle["close"])
            probe_profile = OptionSignalGuard.assess_high_expectancy(
                service,
                signal,
                probe_ts,
                selected_option_contract=probe_row,
                premium_guard=probe_guard,
                price=probe_price,
            )
            if probe_profile.get("allow_trade") and not probe_profile.get("watch_only"):
                confirmed_at = probe_ts
                confirm_entry = float(probe_row.get("ltp") or 0.0)
                confirm_horizon = option_row_at(snapshot_map, probe_ts + timedelta(minutes=horizon), strike, signal)
                if confirm_horizon and confirm_entry > 0:
                    confirm_pnl = float(confirm_horizon.get("ltp") or 0.0) - confirm_entry
                break

        over_wait = False
        if confirmed_at is None:
            over_wait = watch_pnl > 0
        elif confirm_pnl is not None:
            over_wait = watch_pnl - confirm_pnl > 5.0

        stats[family].record(watch_pnl, confirm_pnl, confirmed_at is not None, over_wait)
        stats["ALL"].record(watch_pnl, confirm_pnl, confirmed_at is not None, over_wait)

        watch_path = option_path(snapshot_map, ts, strike, signal, max_minutes)
        watch_mfe = max((float(row.get("ltp") or 0.0) for _, row in watch_path), default=watch_entry) - watch_entry
        details.append(
            {
                "instrument": instrument,
                "ts": ts,
                "side": signal,
                "family": family,
                "strike": strike,
                "watch_pnl": round(watch_pnl, 2),
                "watch_mfe": round(watch_mfe, 2),
                "confirmed_at": confirmed_at,
                "confirm_pnl": round(confirm_pnl, 2) if confirm_pnl is not None else None,
                "decision": "OVER_WAIT" if over_wait else "GOOD_WAIT",
                "blockers": blocker_reasons,
            }
        )

    return stats, details, blocker_counts


def main():
    args = parse_args()
    instruments = ["NIFTY", "BANKNIFTY", "SENSEX"]
    all_details = []
    combined = defaultdict(WatchAuditStat)
    combined_blockers = Counter()
    for instrument in instruments:
        stats, details, blocker_counts = replay_watch_audit(instrument, args.from_date, args.to_date, args.horizon, args.confirm_minutes, args.max_minutes)
        all_details.extend(details)
        print(instrument)
        for family, stat in sorted(stats.items(), key=lambda item: (item[0] != "ALL", -item[1].total, item[0])):
            print(
                f"  {family}: watches={stat.total} confirmed={stat.confirmed} never_confirmed={stat.never_confirmed} "
                f"watch_win={round((stat.watch_profitable / stat.total) * 100, 1) if stat.total else 0}% "
                f"confirm_win={round((stat.confirm_profitable / stat.confirmed) * 100, 1) if stat.confirmed else 0}% "
                f"avg_watch_5m={avg(stat.watch_total_pnl, stat.total)} avg_confirm_5m={avg(stat.confirm_total_pnl, stat.confirmed)} "
                f"over_wait={stat.over_wait} justified_wait={stat.justified_wait} avg_edge_left={avg(stat.edge_left_total, stat.over_wait)}"
            )
            if family == "ALL":
                combined["ALL"].total += stat.total
                combined["ALL"].confirmed += stat.confirmed
                combined["ALL"].never_confirmed += stat.never_confirmed
                combined["ALL"].watch_profitable += stat.watch_profitable
                combined["ALL"].confirm_profitable += stat.confirm_profitable
                combined["ALL"].over_wait += stat.over_wait
                combined["ALL"].justified_wait += stat.justified_wait
                combined["ALL"].watch_total_pnl += stat.watch_total_pnl
                combined["ALL"].confirm_total_pnl += stat.confirm_total_pnl
                combined["ALL"].edge_left_total += stat.edge_left_total
        if blocker_counts.get("ALL"):
            print("  BLOCKERS")
            for reason, count in blocker_counts["ALL"].most_common():
                print(f"    {reason}: {count}")
            combined_blockers.update(blocker_counts["ALL"])
    all_stat = combined["ALL"]
    print("ALL")
    print(
        f"  PA_STRONG_WAIT_PREMIUM: watches={all_stat.total} confirmed={all_stat.confirmed} never_confirmed={all_stat.never_confirmed} "
        f"watch_win={round((all_stat.watch_profitable / all_stat.total) * 100, 1) if all_stat.total else 0}% "
        f"confirm_win={round((all_stat.confirm_profitable / all_stat.confirmed) * 100, 1) if all_stat.confirmed else 0}% "
        f"avg_watch_5m={avg(all_stat.watch_total_pnl, all_stat.total)} avg_confirm_5m={avg(all_stat.confirm_total_pnl, all_stat.confirmed)} "
        f"over_wait={all_stat.over_wait} justified_wait={all_stat.justified_wait} avg_edge_left={avg(all_stat.edge_left_total, all_stat.over_wait)}"
    )
    if combined_blockers:
        print("  BLOCKERS")
        for reason, count in combined_blockers.most_common():
            print(f"    {reason}: {count}")
    if args.details:
        print("DETAILS")
        for row in sorted(all_details, key=lambda item: (item["instrument"], item["ts"])):
            confirm_at = row["confirmed_at"].strftime("%H:%M") if row["confirmed_at"] else "-"
            print(
                f"  {row['instrument']} {row['ts']:%Y-%m-%d %H:%M} {row['side']} {row['family']} strike={row['strike']} "
                f"watch_5m={row['watch_pnl']} confirm_at={confirm_at} confirm_5m={row['confirm_pnl'] if row['confirm_pnl'] is not None else '-'} "
                f"watch_mfe={row['watch_mfe']} decision={row['decision']} blockers={','.join(row['blockers'])}"
            )


if __name__ == "__main__":
    main()
