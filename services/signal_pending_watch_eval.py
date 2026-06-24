"""Pending watch evaluation helpers for SignalService."""

from config import Config
from strategies.shared.one_minute_momentum import OneMinuteMomentumQuality
from strategies.shared.option_buyer_score import calculate_option_buyer_entry_score, option_buyer_action


def evaluate_pending_entry_watch(service, recent_1m_candles):
    if not service.pending_entry_watch or len(recent_1m_candles) < 2:
        return None

    pending = service.pending_entry_watch
    latest = recent_1m_candles[-1]
    previous = recent_1m_candles[-2]
    created_at = pending["created_at"]
    if created_at is not None and latest["time"] is not None:
        created_at, latest_time = service._coerce_comparable_datetimes(created_at, latest["time"])
        latest = {**latest, "time": latest_time}
        previous_time = previous["time"]
        _, previous_time = service._coerce_comparable_datetimes(created_at, previous_time)
        previous = {**previous, "time": previous_time}
    minutes_since_watch = int((latest["time"] - created_at).total_seconds() // 60)
    if minutes_since_watch < 1:
        return None
    max_watch_minutes = service._pending_watch_max_minutes(pending)
    if minutes_since_watch > max_watch_minutes:
        return {"status": "EXPIRED", "reason": "1m confirmation window expired"}
    pending["minutes_since_watch"] = minutes_since_watch

    conflicts_too_high, conflict_reason = service._pending_watch_conflicts_too_high(pending)
    if conflicts_too_high:
        return {"status": "INVALIDATED", "reason": conflict_reason}

    trigger_price = pending["trigger_price"]
    invalidate_price = pending.get("invalidate_price")
    direction = pending["direction"]
    if pending.get("wait_pullback"):
        strike = pending.get("selected_strike")
        entry_max = pending.get("premium_pullback_entry_max")
        if strike is None or entry_max is None:
            return None
        option_contract = service._get_option_contract_snapshot(strike, direction, before_ts=latest["time"])
        current_ltp = float((option_contract or {}).get("ltp") or 0.0)
        spread_pct = service._spread_percent(option_contract) if option_contract else None
        max_spread = float(getattr(Config, "PRO_TRADER_MAX_ACTION_SPREAD_PCT", 3.8) or 3.8)
        if current_ltp <= 0 or current_ltp > float(entry_max):
            return None
        if spread_pct is not None and spread_pct > max_spread:
            return None

    fast_track_ready = pending.get("fast_track_ready", False)
    elite_watch_ready = pending.get("elite_watch_ready", False)
    hybrid_mode = pending.get("hybrid_mode", False)
    starter_entry_ready = pending.get("starter_entry_ready", False)
    one_min_buffer = 2 if service.instrument == "NIFTY" else 5
    if fast_track_ready:
        one_min_buffer = 1 if service.instrument == "NIFTY" else 3
    elif hybrid_mode:
        one_min_buffer = 1 if service.instrument == "NIFTY" else 2
    if elite_watch_ready and minutes_since_watch <= 3:
        one_min_buffer = max(0 if service.instrument == "NIFTY" else 1, one_min_buffer - 1)
    if service.instrument == "BANKNIFTY" and minutes_since_watch >= 6:
        one_min_buffer = max(one_min_buffer, 4)
    if service.instrument == "SENSEX" and minutes_since_watch >= 6:
        one_min_buffer = max(one_min_buffer, 3)

    if not service._pending_watch_risk_reward_ok(pending):
        return {"status": "INVALIDATED", "reason": "Watch risk-reward not attractive for 1m trigger"}

    if direction == "CE":
        if invalidate_price is not None and latest["low"] <= invalidate_price:
            return {"status": "INVALIDATED", "reason": "Watch invalidated before 1m trigger"}
        trigger_hit = latest["close"] > trigger_price and latest["high"] >= trigger_price + one_min_buffer
        body_ok = latest["close"] >= latest["open"]
        follow_through_ok = latest["close"] > previous["high"] or (
            previous["close"] > trigger_price and latest["close"] >= previous["close"]
        )
        if hybrid_mode:
            trigger_hit = trigger_hit or (latest["high"] >= trigger_price and latest["close"] >= trigger_price)
            follow_through_ok = follow_through_ok or latest["close"] >= trigger_price
        if elite_watch_ready and minutes_since_watch <= 3:
            trigger_hit = trigger_hit or (
                latest["high"] >= trigger_price and latest["close"] >= trigger_price and latest["close"] >= latest["open"]
            )
            follow_through_ok = follow_through_ok or (
                latest["close"] >= trigger_price and latest["close"] >= previous["close"]
            )
        starter_trigger_hit = (
            starter_entry_ready
            and minutes_since_watch <= 3
            and latest["high"] >= trigger_price
            and latest["close"] >= trigger_price
            and latest["close"] >= latest["open"]
        )
    else:
        if invalidate_price is not None and latest["high"] >= invalidate_price:
            return {"status": "INVALIDATED", "reason": "Watch invalidated before 1m trigger"}
        trigger_hit = latest["close"] < trigger_price and latest["low"] <= trigger_price - one_min_buffer
        body_ok = latest["close"] <= latest["open"]
        follow_through_ok = latest["close"] < previous["low"] or (
            previous["close"] < trigger_price and latest["close"] <= previous["close"]
        )
        if hybrid_mode:
            trigger_hit = trigger_hit or (latest["low"] <= trigger_price and latest["close"] <= trigger_price)
            follow_through_ok = follow_through_ok or latest["close"] <= trigger_price
        if elite_watch_ready and minutes_since_watch <= 3:
            trigger_hit = trigger_hit or (
                latest["low"] <= trigger_price and latest["close"] <= trigger_price and latest["close"] <= latest["open"]
            )
            follow_through_ok = follow_through_ok or (
                latest["close"] <= trigger_price and latest["close"] <= previous["close"]
            )
        starter_trigger_hit = (
            starter_entry_ready
            and minutes_since_watch <= 3
            and latest["low"] <= trigger_price
            and latest["close"] <= trigger_price
            and latest["close"] <= latest["open"]
        )

    volume_ok = service._one_minute_trigger_volume_ok(recent_1m_candles)
    starter_volume_ok = _starter_volume_ok(recent_1m_candles)
    if hybrid_mode and pending.get("strong_watch_setup") and latest.get("volume", 0) > 0:
        volume_ok = True if latest["volume"] >= max(previous.get("volume", 0) * 0.8, 1) else volume_ok
    if elite_watch_ready and latest.get("volume", 0) > 0:
        prior = recent_1m_candles[-4:-1]
        prior_volumes = [candle.get("volume") or 0 for candle in prior]
        avg_prior_volume = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0
        volume_ok = volume_ok or latest["volume"] >= max(avg_prior_volume * 0.7, previous.get("volume", 0) * 0.75, 1)
    if fast_track_ready and trigger_hit and volume_ok:
        if direction == "CE":
            body_ok = body_ok or latest["high"] >= trigger_price + (one_min_buffer * 2)
            follow_through_ok = follow_through_ok or latest["close"] >= trigger_price
        else:
            body_ok = body_ok or latest["low"] <= trigger_price - (one_min_buffer * 2)
            follow_through_ok = follow_through_ok or latest["close"] <= trigger_price
    elif hybrid_mode and trigger_hit and volume_ok:
        body_ok = body_ok or abs((latest["close"] or 0) - (latest["open"] or 0)) > 0
    if trigger_hit and not service._pending_watch_not_too_late(pending, latest["close"]):
        return {"status": "INVALIDATED", "reason": "1m trigger arrived too late after move extension"}
    if starter_trigger_hit and starter_volume_ok and not service._pending_watch_not_too_late(pending, latest["close"]):
        return {"status": "INVALIDATED", "reason": "Starter trigger arrived too late after move extension"}
    starter_entry = bool(starter_trigger_hit and starter_volume_ok and not trigger_hit)
    if not trigger_hit or not body_ok or not follow_through_ok or not volume_ok:
        if not starter_entry:
            return None

    return _evaluate_triggered_watch(
        service,
        pending,
        latest,
        previous,
        recent_1m_candles,
        direction,
        volume_ok or starter_volume_ok,
        hybrid_mode,
        fast_track_ready,
        elite_watch_ready,
        starter_entry,
    )


def _starter_volume_ok(recent_1m_candles):
    if len(recent_1m_candles or []) < 2:
        return False
    latest = recent_1m_candles[-1]
    previous = recent_1m_candles[-2]
    latest_volume = float(latest.get("volume") or 0.0)
    if latest_volume <= 0:
        return False
    prior = recent_1m_candles[-4:-1]
    prior_volumes = [float(candle.get("volume") or 0.0) for candle in prior]
    avg_prior_volume = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0.0
    return latest_volume >= max(avg_prior_volume * 0.65, float(previous.get("volume") or 0.0) * 0.70, 1.0)


def _evaluate_triggered_watch(service, pending, latest, previous, recent_1m_candles, direction, volume_ok, hybrid_mode, fast_track_ready, elite_watch_ready, starter_entry=False):
    live_pressure_metrics = (
        service.pressure.analyze(service.option_data, underlying_price=latest["close"])
        if getattr(service, "pressure", None) and getattr(service, "option_data", None)
        else None
    )
    quality_ok, quality_reason = service._pending_watch_quality_ok(pending, latest, previous)
    if not quality_ok:
        if service._pending_watch_retrigger_eligible(pending, latest, previous):
            return {"status": "REARM", "reason": quality_reason}
        return {"status": "INVALIDATED", "reason": quality_reason}
    selected_strike = pending.get("selected_strike")
    if selected_strike is None:
        selected_strike, _ = service.strike_selector.select_strike_with_reason(
            price=latest["close"],
            signal=direction,
            volume_signal="STRONG" if volume_ok or pending.get("strong_watch_setup") else "NORMAL",
            strategy_score=pending.get("score") or 0,
            pressure_metrics=live_pressure_metrics,
            cautions=pending.get("cautions"),
            option_chain_data=service.option_data,
            setup_type=pending.get("signal_type"),
            time_regime=pending.get("time_regime"),
            candle_time=latest["time"],
        )
    strict_confirmation = (pending.get("signal_type") or "").upper() in {"REVERSAL", "BREAKOUT_CONFIRM", "TRAP_REVERSAL"}
    if starter_entry:
        strict_confirmation = False
    confirmed, micro_reason, _, _ = service._confirm_signal_microstructure(
        signal=direction,
        selected_strike=selected_strike,
        timestamp=latest["time"],
        price=latest["close"],
        strict=strict_confirmation,
    )
    if not confirmed:
        if (
            micro_reason in {"Microstructure data unavailable", "Insufficient OI data"}
            and service._pending_watch_spike_micro_override_ready(pending, latest)
        ):
            confirmed = True
            micro_reason = "Spike watch microstructure override"
        else:
            if starter_entry and micro_reason in {"Microstructure data unavailable", "Insufficient OI data"}:
                confirmed = True
                micro_reason = "Starter entry accepted with price-volume confirmation"
            else:
                if service._pending_watch_retrigger_eligible(pending, latest, previous):
                    return {"status": "REARM", "reason": f"1m trigger lacked microstructure confirmation: {micro_reason}"}
                return {"status": "INVALIDATED", "reason": f"1m trigger lacked microstructure confirmation: {micro_reason}"}
    momentum_checker = getattr(service, "one_minute_momentum", None) or OneMinuteMomentumQuality(min_score=30)
    momentum_quality = momentum_checker.evaluate(
        direction=direction,
        recent_1m_candles=recent_1m_candles,
        volume_signal="STRONG" if volume_ok else "NORMAL",
        oi_bias="BULLISH" if direction == "CE" else "BEARISH",
        pressure_bias=(
            "NEUTRAL"
            if pending.get("pressure_conflict_level") not in {None, "NONE"}
            else ("BULLISH" if direction == "CE" else "BEARISH")
        ),
    )
    if not momentum_quality["ok"] and not (fast_track_ready or hybrid_mode or elite_watch_ready or starter_entry):
        if service._pending_watch_retrigger_eligible(pending, latest, previous):
            return {"status": "REARM", "reason": momentum_quality["reason"]}
        return {"status": "INVALIDATED", "reason": momentum_quality["reason"]}
    final_entry_score = calculate_option_buyer_entry_score(
        base_entry_score=pending.get("entry_score"),
        strategy_score=pending.get("score"),
        momentum_score=momentum_quality.get("score"),
        blockers=pending.get("blockers"),
        cautions=pending.get("cautions"),
        confidence=pending.get("confidence"),
        signal_grade=pending.get("signal_grade"),
    )
    min_final_score = 60 if starter_entry else 64 if (fast_track_ready or hybrid_mode or elite_watch_ready) else 70
    if final_entry_score < min_final_score:
        if service._pending_watch_retrigger_eligible(pending, latest, previous):
            return {"status": "REARM", "reason": f"Option-buyer entry score weak ({final_entry_score} < {min_final_score})"}
        return {"status": "INVALIDATED", "reason": f"Option-buyer entry score weak ({final_entry_score} < {min_final_score})"}
    return {
        "status": "TRIGGERED",
        "price": latest["close"],
        "time": latest["time"],
        "reason": (
            f"STARTER_ENTRY | early A-grade trigger after 5m watch | {momentum_quality['reason']} ({momentum_quality['score']})"
            if starter_entry
            else f"1m trigger confirmed after 5m watch | {momentum_quality['reason']} ({momentum_quality['score']})"
        ),
        "selected_strike": selected_strike,
        "micro_reason": micro_reason,
        "momentum_quality": momentum_quality,
        "option_buyer_entry_score": final_entry_score,
        "option_buyer_action": option_buyer_action(final_entry_score),
        "entry_mode": "STARTER" if starter_entry else "CONFIRMED",
    }
