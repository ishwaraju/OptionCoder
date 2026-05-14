"""Pending-watch policy helpers extracted from SignalService."""


class PendingEntryWatchPolicy:
    @staticmethod
    def preserve_existing_pending_watch(service, candle_5m):
        pending = service.pending_entry_watch
        if not pending or not candle_5m:
            return False

        created_at = pending.get("created_at")
        current_time = candle_5m.get("time")
        if created_at is None or current_time is None:
            return False

        created_at, current_time = service._coerce_comparable_datetimes(created_at, current_time)
        age_minutes = int((current_time - created_at).total_seconds() // 60)
        if age_minutes < 0:
            return False
        if age_minutes > service._pending_watch_max_minutes(pending):
            service._clear_pending_entry_watch()
            return False
        return True

    @staticmethod
    def pending_watch_is_recoverable(service, pending, current_time=None):
        if not pending:
            return False
        created_at = pending.get("created_at")
        if created_at is None:
            return False
        current_time = current_time or service.time_utils.now_ist()
        created_at, current_time = service._coerce_comparable_datetimes(created_at, current_time)
        age_minutes = max(0, int((current_time - created_at).total_seconds() // 60))
        return age_minutes <= service._pending_watch_max_minutes(pending)

    @staticmethod
    def active_trade_monitor_is_recoverable(service, current_time=None):
        monitor = service.active_trade_monitor
        if not monitor:
            return False
        entry_time = monitor.get("entry_time")
        if entry_time is None:
            return False
        current_time = current_time or service.time_utils.now_ist()
        entry_time, current_time = service._coerce_comparable_datetimes(entry_time, current_time)
        age_minutes = max(0, int((current_time - entry_time).total_seconds() // 60))
        return age_minutes <= 25

    @staticmethod
    def collect_unprocessed_5m_candles(service, recent_candles, current_time):
        if not recent_candles:
            return []

        if service.last_processed_5m_ts is None:
            closed_candles = []
            for candle in recent_candles:
                latest_close_time = service._effective_candle_close_time(candle)
                current_cmp, latest_close_time = service._coerce_comparable_datetimes(current_time, latest_close_time)
                if current_cmp >= latest_close_time:
                    closed_candles.append(candle)
            return closed_candles[-3:]

        pending_candles = []
        processed_cutoff = service.last_processed_5m_ts
        for candle in recent_candles:
            candle_time = candle.get("time")
            if candle_time is None:
                continue
            processed_cmp, candle_time = service._coerce_comparable_datetimes(processed_cutoff, candle_time)
            if candle_time <= processed_cmp:
                continue
            effective_close_time = service._effective_candle_close_time({**candle, "time": candle_time})
            current_cmp, effective_close_time = service._coerce_comparable_datetimes(current_time, effective_close_time)
            if current_cmp >= effective_close_time:
                pending_candles.append({**candle, "time": candle_time})
        return pending_candles[-12:]

    @staticmethod
    def one_minute_trigger_volume_ok(recent_1m_candles):
        if not recent_1m_candles:
            return False
        latest = recent_1m_candles[-1]
        latest_volume = latest.get("volume") or 0
        if latest_volume <= 0:
            return False
        prior = recent_1m_candles[-4:-1]
        if not prior:
            return True
        prior_volumes = [candle.get("volume") or 0 for candle in prior]
        avg_prior_volume = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0
        previous_volume = prior_volumes[-1] if prior_volumes else 0
        minimum_needed = max(avg_prior_volume, previous_volume * 0.9)
        return latest_volume >= minimum_needed

    @staticmethod
    def pending_watch_risk_reward_ok(pending):
        trigger_price = pending.get("trigger_price")
        invalidate_price = pending.get("invalidate_price")
        first_target = pending.get("first_target_price")
        if trigger_price is None or invalidate_price is None or first_target is None:
            return True
        risk = abs(float(trigger_price) - float(invalidate_price))
        reward = abs(float(first_target) - float(trigger_price))
        if risk <= 0:
            return False
        return reward >= (risk * 0.9)

    @staticmethod
    def rebalance_pending_watch_plan(service, pending, candle_5m):
        if not pending:
            return pending

        setup = (pending.get("signal_type") or "NONE").upper()
        direction = pending.get("direction")
        trigger_price = pending.get("trigger_price")
        invalidate_price = pending.get("invalidate_price")
        first_target = pending.get("first_target_price")
        if (
            setup not in {"BREAKOUT_CONFIRM", "RETEST"}
            or direction not in {"CE", "PE"}
            or trigger_price is None
            or candle_5m is None
        ):
            return pending

        if service._pending_watch_risk_reward_ok(pending):
            return pending

        atr_value = candle_5m.get("atr") or service.atr.get_atr()
        if atr_value is None:
            return pending

        atr_value = float(atr_value)
        risk_cap = max(
            atr_value * 0.6,
            10.0 if service.instrument == "NIFTY" else 18.0 if service.instrument == "BANKNIFTY" else 14.0,
        )
        trigger_price = float(trigger_price)
        current_invalidate = float(invalidate_price) if invalidate_price is not None else None
        current_target = float(first_target) if first_target is not None else None

        if direction == "CE":
            capped_invalidate = round(trigger_price - risk_cap, 2)
            if current_invalidate is not None and current_invalidate > capped_invalidate:
                capped_invalidate = current_invalidate
            pending["invalidate_price"] = capped_invalidate
            min_target = round(trigger_price + (abs(trigger_price - capped_invalidate) * 0.95), 2)
            pending["first_target_price"] = max(current_target or min_target, min_target)
        else:
            capped_invalidate = round(trigger_price + risk_cap, 2)
            if current_invalidate is not None and current_invalidate < capped_invalidate:
                capped_invalidate = current_invalidate
            pending["invalidate_price"] = capped_invalidate
            min_target = round(trigger_price - (abs(capped_invalidate - trigger_price) * 0.95), 2)
            pending["first_target_price"] = min(current_target or min_target, min_target)

        return pending

    @staticmethod
    def pending_watch_not_too_late(pending, latest_price):
        trigger_price = pending.get("trigger_price")
        first_target = pending.get("first_target_price")
        direction = pending.get("direction")
        if trigger_price is None or first_target is None or latest_price is None or direction not in {"CE", "PE"}:
            return True
        total_path = abs(float(first_target) - float(trigger_price))
        if total_path <= 0:
            return True
        covered = (
            float(latest_price) - float(trigger_price)
            if direction == "CE"
            else float(trigger_price) - float(latest_price)
        )
        return covered <= (total_path * 0.55)

    @staticmethod
    def rearm_pending_entry_watch(service, latest, reason):
        if not service.pending_entry_watch:
            return
        service.pending_entry_watch["created_at"] = latest.get("time")
        service.pending_entry_watch["last_checked_minute"] = None
        service.pending_entry_watch["retrigger_count"] = int(service.pending_entry_watch.get("retrigger_count") or 0) + 1
        service.pending_entry_watch["retrigger_reason"] = reason

    @staticmethod
    def pending_watch_quality_ok(service, pending, latest, previous):
        direction = pending.get("direction")
        if direction not in {"CE", "PE"}:
            return True, None

        close_strength = service._candle_close_strength(latest, direction)
        body_ratio = service._candle_body_ratio(latest)
        hybrid_mode = pending.get("hybrid_mode", False)
        fast_track_ready = pending.get("fast_track_ready", False)
        strong_watch_setup = pending.get("strong_watch_setup", False)
        minutes_since_watch = pending.get("minutes_since_watch", 0)
        trigger_price = pending.get("trigger_price")
        invalidate_price = pending.get("invalidate_price")
        spike_origin = pending.get("spike_origin", False)
        spike_quality = ((pending.get("spike_context") or {}).get("quality") or "").upper()

        min_close_strength = 0.52
        min_body_ratio = 0.18
        if service.instrument == "BANKNIFTY":
            min_close_strength = 0.6
            min_body_ratio = 0.24
        elif service.instrument == "SENSEX":
            min_close_strength = 0.58
            min_body_ratio = 0.22
        elif service.instrument == "NIFTY":
            min_close_strength = 0.55
            min_body_ratio = 0.2

        if hybrid_mode and strong_watch_setup:
            min_close_strength -= 0.04
            min_body_ratio -= 0.03
        if fast_track_ready:
            min_close_strength -= 0.03
        if spike_origin and spike_quality == "STRONG":
            min_close_strength -= 0.05
            min_body_ratio -= 0.04

        spike_breadth_override = (
            spike_origin
            and (pending.get("spike_context") or {}).get("price_breadth", 0) >= 6
            and (pending.get("spike_context") or {}).get("volume_breadth", 0) >= 6
            and minutes_since_watch <= 2
        )

        if close_strength < min_close_strength:
            if spike_breadth_override and close_strength >= max(min_close_strength - 0.08, 0.45):
                close_strength = min_close_strength
            else:
                return False, "1m trigger close weak"
        if body_ratio < min_body_ratio:
            if spike_breadth_override and body_ratio >= max(min_body_ratio - 0.08, 0.10):
                body_ratio = min_body_ratio
            else:
                return False, "1m trigger body weak"

        if service.instrument == "NIFTY" and invalidate_price is not None and trigger_price is not None:
            total_risk = abs(float(trigger_price) - float(invalidate_price))
            if total_risk > 0:
                current_buffer = (
                    float(latest["close"]) - float(invalidate_price)
                    if direction == "CE"
                    else float(invalidate_price) - float(latest["close"])
                )
                if current_buffer < (total_risk * 0.28):
                    return False, "1m trigger too close to invalidation"

        if service.instrument in {"BANKNIFTY", "SENSEX"} and minutes_since_watch >= 8:
            if close_strength < (min_close_strength + 0.06):
                return False, "late 1m trigger close not strong enough"

        prev_close = previous.get("close")
        prev_open = previous.get("open")
        if prev_close is not None and prev_open is not None:
            if direction == "CE" and float(prev_close) < float(prev_open) and close_strength < 0.62 and service.instrument == "NIFTY":
                return False, "1m trigger against fresh opposite candle"
            if direction == "PE" and float(prev_close) > float(prev_open) and close_strength < 0.62 and service.instrument == "NIFTY":
                return False, "1m trigger against fresh opposite candle"

        return True, None

    @staticmethod
    def pending_watch_spike_micro_override_ready(pending, latest):
        spike_context = pending.get("spike_context") or {}
        if not pending.get("spike_origin"):
            return False
        if (spike_context.get("quality") or "").upper() != "STRONG":
            return False
        if (pending.get("direction") or "").upper() not in {"CE", "PE"}:
            return False
        latest_close = float(latest.get("close") or 0.0)
        latest_open = float(latest.get("open") or 0.0)
        trigger_price = pending.get("trigger_price")
        if trigger_price is None:
            return False
        if pending.get("direction") == "CE":
            return latest_close >= float(trigger_price) and latest_close >= latest_open
        return latest_close <= float(trigger_price) and latest_close <= latest_open

    @staticmethod
    def pending_watch_has_caution(pending, caution):
        cautions = pending.get("cautions") or []
        return caution in cautions

    @staticmethod
    def pending_watch_conflicts_too_high(service, pending):
        setup = (pending.get("signal_type") or "NONE").upper()
        cautions = set(pending.get("cautions") or [])
        blockers = set(pending.get("blockers") or [])
        pressure_conflict_level = (pending.get("pressure_conflict_level") or "NONE").upper()
        time_regime = (pending.get("time_regime") or "").upper()
        active_day_state = (pending.get("active_day_state") or "").upper()
        day_state_direction = (pending.get("day_state_direction") or "").upper()
        watch_direction = (pending.get("direction") or "").upper()

        if setup != "BREAKOUT_CONFIRM":
            return False, None

        opposite_pressure = "opposite_pressure" in cautions
        weak_participation = (
            "participation_weak" in cautions
            or "participation_delta_missing" in cautions
        )
        retest_wait = "late_confirmation_wait_retest" in cautions
        expiry_mode = "expiry_day_mode" in cautions
        pre_expiry_watch_friendly = "pre_expiry_watch_friendly" in cautions
        strong_day_state_alignment = (
            active_day_state in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
            and watch_direction in {"CE", "PE"}
            and day_state_direction == watch_direction
            and float(pending.get("score") or 0) >= 74
            and float(pending.get("entry_score") or 0) >= 64
        )

        if opposite_pressure and weak_participation and pressure_conflict_level in {"MILD", "MODERATE", "HIGH"}:
            if (pre_expiry_watch_friendly or strong_day_state_alignment) and pressure_conflict_level == "MILD":
                return False, None
            return True, "breakout watch has opposite pressure with weak participation"

        if retest_wait and opposite_pressure:
            return True, "breakout watch is already in retest-wait mode"

        if time_regime == "LATE_DAY" and (
            opposite_pressure and weak_participation and pressure_conflict_level in {"MODERATE", "HIGH"}
        ):
            return True, "late-day breakout watch too conflicted"

        if service.instrument in {"NIFTY", "BANKNIFTY"} and expiry_mode and opposite_pressure and weak_participation:
            if (pre_expiry_watch_friendly or strong_day_state_alignment) and pressure_conflict_level == "MILD":
                return False, None
            return True, "expiry breakout watch has poor option confirmation"

        if "direction_present_but_filters_incomplete" in blockers and opposite_pressure and weak_participation:
            if (pre_expiry_watch_friendly or strong_day_state_alignment) and pressure_conflict_level == "MILD":
                return False, None
            return True, "direction incomplete with opposite pressure and weak participation"

        return False, None
