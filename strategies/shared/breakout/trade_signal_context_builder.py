from config import Config

from .trend_quality_refiner import TrendQualityRefiner


class TradeSignalContextBuilder:
    @staticmethod
    def build(
        strategy,
        *,
        price,
        orb_high,
        orb_low,
        vwap,
        volume_signal,
        oi_bias,
        oi_trend=None,
        build_up=None,
        support=None,
        resistance=None,
        can_trade=True,
        buffer=0,
        pressure_metrics=None,
        atr=None,
        expiry=None,
        candle_high=None,
        candle_low=None,
        candle_close=None,
        candle_open=None,
        candle_tick_count=None,
        candle_time=None,
        candle_volume=None,
        atm_ce_volume=None,
        atm_pe_volume=None,
        recent_candles_5m=None,
        trend_15m=None,
        participation_metrics=None,
        oi_ladder_data=None,
        option_sweep_context=None,
    ):
        score, scored_direction, components = strategy._score_signal(
            price=price,
            orb_high=orb_high,
            orb_low=orb_low,
            vwap=vwap,
            volume_signal=volume_signal,
            oi_bias=oi_bias,
            oi_trend=oi_trend,
            build_up=build_up,
            pressure_metrics=pressure_metrics,
            buffer=buffer,
        )
        strategy.last_score = score
        strategy.last_context_score = score
        strategy.last_entry_score = score
        strategy.last_score_components = components
        strategy.last_decision_state = "IGNORE"
        strategy.last_watch_bucket = "NONE"
        strategy.last_pressure_conflict_level = "NONE"
        strategy.last_confidence_summary = None
        strategy.last_entry_plan = {}
        strategy.last_participation_metrics = participation_metrics
        strategy.last_option_sweep_context = option_sweep_context
        strategy.last_signal_type = "NONE"
        strategy.last_signal_grade = "SKIP"
        strategy.last_opening_bias = "UNKNOWN"
        strategy.last_active_day_state = "UNKNOWN"
        strategy.last_day_state_direction = "NONE"
        strategy.last_day_state_detail = ""
        blockers = []
        cautions = []
        tuning = strategy._instrument_tuning()

        if not can_trade and not Config.TEST_MODE:
            blockers.append("time_filter")
            strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="NO_TRADE_WINDOW")
            return (None, f"Trade not allowed (time filter) | score={score}"), None

        if vwap is None:
            blockers.append("vwap_unavailable")
            strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="NO_DATA")
            return (None, f"VWAP not ready | score={score}"), None

        if strategy._is_invalid_candle(candle_open, candle_high, candle_low, candle_close, candle_volume):
            blockers.append("invalid_candle_data")
            strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="NO_DATA", signal_type="NONE")
            return (None, f"Invalid candle data | score={score}"), None

        candle_range = 0
        candle_body = 0
        if candle_high is not None and candle_low is not None:
            candle_range = max(candle_high - candle_low, 0)
        if candle_close is not None and candle_open is not None:
            candle_body = abs(candle_close - candle_open)

        if candle_high is None or candle_low is None or candle_close is None or candle_open is None:
            breakout_body_ok = True
            breakout_structure_ok = True
        else:
            breakout_body_ok = candle_body >= max(buffer * tuning["body_buffer_mult"], tuning["body_floor"])
            breakout_structure_ok = candle_range == 0 or (candle_body / candle_range) >= 0.30
        candle_liquidity_ok = candle_tick_count is None or candle_tick_count >= 3
        heikin_ashi = strategy._compute_heikin_ashi(candle_open, candle_high, candle_low, candle_close)
        ha_bias = heikin_ashi["bias"] if heikin_ashi else "NEUTRAL"
        ha_strength = heikin_ashi["strength"] if heikin_ashi else "NEUTRAL"
        ha_has_context = heikin_ashi["had_prior_context"] if heikin_ashi else False
        bullish_ha_ok = (ha_bias == "BULLISH") if ha_has_context else True
        bearish_ha_ok = (ha_bias == "BEARISH") if ha_has_context else True
        strong_bullish_ha = ha_strength == "BULLISH_STRONG"
        strong_bearish_ha = ha_strength == "BEARISH_STRONG"

        if scored_direction == "CE" and ha_has_context and ha_strength == "BEARISH_STRONG":
            cautions.append("heikin_ashi_strong_opposite")
        elif scored_direction == "PE" and ha_has_context and ha_strength == "BULLISH_STRONG":
            cautions.append("heikin_ashi_strong_opposite")

        score, cautions = strategy._apply_participation_adjustment(
            score=score,
            direction=scored_direction,
            cautions=cautions,
            components=components,
            participation_metrics=participation_metrics,
        )
        if option_sweep_context and option_sweep_context.get("direction") == scored_direction:
            score = min(100, score + int(option_sweep_context.get("score_boost", 0) or 0))
            components.append(f"option_sweep_{(option_sweep_context.get('quality') or 'moderate').lower()}")
            if option_sweep_context.get("micro_confirmed"):
                components.append("option_sweep_micro_confirmed")
            if option_sweep_context.get("persistence_pairs", 0) >= 3:
                components.append("option_sweep_persistent")
            if option_sweep_context.get("quality") == "STRONG":
                cautions = [
                    caution
                    for caution in cautions
                    if caution not in {"participation_weak", "participation_delta_missing"}
                ]
        strategy.last_score = score
        strategy.last_context_score = score
        strategy.last_score_components = components

        current_now = candle_time.time() if candle_time is not None else strategy.time_utils.current_time()
        time_regime = strategy._derive_time_regime(current_now)
        strategy.last_time_regime = time_regime
        opening_bias, opening_detail = strategy._derive_opening_bias(recent_candles_5m, vwap, atr)
        day_state = strategy._derive_active_day_state(
            recent_candles_5m=recent_candles_5m,
            price=price,
            vwap=vwap,
            atr=atr,
            pressure_metrics=pressure_metrics,
            opening_bias=opening_bias,
            time_regime=time_regime,
        )
        strategy.last_opening_bias = opening_bias
        strategy.last_active_day_state = day_state.get("state") or "UNKNOWN"
        strategy.last_day_state_direction = day_state.get("direction") or "NONE"
        strategy.last_day_state_detail = day_state.get("detail") or opening_detail
        score, cautions = strategy._apply_day_state_adjustment(
            score=score,
            scored_direction=scored_direction,
            cautions=cautions,
            components=components,
            day_state=day_state,
        )
        strategy.last_score = score
        strategy.last_context_score = score
        strategy.last_entry_score = score
        strategy.last_score_components = components

        oi_divergence = (oi_ladder_data or {}).get("price_vs_oi_divergence")
        wall_break_alert = (oi_ladder_data or {}).get("wall_break_alert")
        support_wall_state = (oi_ladder_data or {}).get("support_wall_state")
        resistance_wall_state = (oi_ladder_data or {}).get("resistance_wall_state")
        divergence_against_direction = (
            (scored_direction == "CE" and oi_divergence == "BEARISH_DIVERGENCE")
            or (scored_direction == "PE" and oi_divergence == "BULLISH_DIVERGENCE")
        )
        divergence_supports_direction = (
            (scored_direction == "CE" and oi_divergence == "BULLISH_DIVERGENCE")
            or (scored_direction == "PE" and oi_divergence == "BEARISH_DIVERGENCE")
        )
        wall_break_supports_direction = (
            (scored_direction == "CE" and wall_break_alert == "RESISTANCE_BREAK_RISK")
            or (scored_direction == "PE" and wall_break_alert == "SUPPORT_BREAK_RISK")
        )
        wall_strength_supports_direction = (
            (scored_direction == "CE" and resistance_wall_state == "WEAKENING")
            or (scored_direction == "PE" and support_wall_state == "WEAKENING")
        )
        if divergence_against_direction:
            score = max(0, score - 8)
            cautions = strategy._append_cautions(cautions, "oi_divergence_against")
            components.append("oi_divergence_against")
        elif divergence_supports_direction:
            score = min(100, score + 4)
            components.append("oi_divergence_support")
        if wall_break_supports_direction:
            score = min(100, score + 6)
            components.append("oi_wall_break_risk_support")
        elif wall_strength_supports_direction:
            score = min(100, score + 3)
            components.append("oi_wall_strength_support")
        strategy.last_score = score
        strategy.last_context_score = score
        strategy.last_score_components = components

        bullish_buildups = ["LONG_BUILDUP", "SHORT_COVERING"]
        bearish_buildups = ["SHORT_BUILDUP", "LONG_UNWINDING"]
        fallback_mode = pressure_metrics is None
        bullish_build_up_ok = strategy._has_bullish_build_up(build_up) or (
            fallback_mode and oi_bias == "BULLISH" and oi_trend in ["BULLISH", "NEUTRAL"] and score >= 60
        )
        bearish_build_up_ok = strategy._has_bearish_build_up(build_up) or (
            fallback_mode and oi_bias == "BEARISH" and oi_trend in ["BEARISH", "NEUTRAL"] and score >= 60
        )
        if strategy.instrument == "SENSEX":
            bullish_build_up_ok = bullish_build_up_ok or (
                oi_bias == "BULLISH" and oi_trend in ["BULLISH", "NEUTRAL", None] and score >= 56
            )
            bearish_build_up_ok = bearish_build_up_ok or (
                oi_bias == "BEARISH" and oi_trend in ["BEARISH", "NEUTRAL", None] and score >= 56
            )

        previous_candle = strategy._previous_candle(recent_candles_5m)
        prev_high = previous_candle.get("high") if previous_candle else None
        prev_low = previous_candle.get("low") if previous_candle else None
        prev_close = previous_candle.get("close") if previous_candle else None
        reversal_trap_context = strategy._reversal_trap_context(
            prev_high=prev_high,
            prev_low=prev_low,
            prev_close=prev_close,
            candle_open=candle_open,
            candle_high=candle_high,
            candle_low=candle_low,
            candle_close=candle_close,
            vwap=vwap,
            support=support,
            resistance=resistance,
            volume_signal=volume_signal,
            candle_liquidity_ok=candle_liquidity_ok,
        )
        if reversal_trap_context["CE"]["ready"] and scored_direction == "CE":
            score = min(100, score + reversal_trap_context["CE"]["score_boost"])
            components.append("reversal_trap_reclaim")
        elif reversal_trap_context["PE"]["ready"] and scored_direction == "PE":
            score = min(100, score + reversal_trap_context["PE"]["score_boost"])
            components.append("reversal_trap_reclaim")

        nifty_trend_day_context = strategy._nifty_trend_day_context_ready(
            direction=scored_direction,
            price=price,
            vwap=vwap,
            orb_high=orb_high,
            orb_low=orb_low,
            candle_close=candle_close,
            candle_range=candle_range,
            atr=atr,
            time_regime=strategy._derive_time_regime(
                candle_time.time() if candle_time is not None else strategy.time_utils.current_time()
            ),
            volume_signal=volume_signal,
            pressure_conflict_level=strategy._pressure_conflict_level(pressure_metrics, scored_direction, cautions),
            ha_strength=ha_strength,
            recent_candles_5m=recent_candles_5m,
        )
        if nifty_trend_day_context:
            score = min(100, score + (10 if volume_signal == "WEAK" else 8))
            components.append("nifty_trend_day_context")
        strategy.last_score = score
        strategy.last_context_score = score
        strategy.last_score_components = components

        strategy._update_retest_setup(candle_time)
        strategy._update_confirmation_setup(candle_time)

        strong_sweep_trade_ready = False
        regime = strategy._derive_regime(price, vwap, atr, volume_signal, candle_range)
        if day_state.get("state") in {"BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"} and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            regime = "TRENDING"
            components.append("day_state_regime_trending")
        elif day_state.get("state") == "RANGE_ACTIVE" and regime == "TRENDING" and score < 82:
            regime = "RANGING"
            components.append("day_state_regime_range")
        if nifty_trend_day_context and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            regime = "TRENDING"
            components.append("nifty_trend_day_regime_override")
        breakout_regime_ok = regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"] or (
            regime == "RANGING" and score >= 72 and volume_signal == "STRONG"
        )
        continuation_regime_ok = regime in ["TRENDING", "EXPANDING"] or (
            regime == "RANGING" and score >= 78 and volume_signal == "STRONG"
        )
        if strong_sweep_trade_ready and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            breakout_regime_ok = True
            continuation_regime_ok = True
            components.append("option_sweep_regime_override")
        retest_regime_ok = regime in ["TRENDING", "EXPANDING", "RANGING"] and not (
            regime == "RANGING" and score < 68
        )
        reversal_regime_ok = regime in ["RANGING", "CHOPPY"] or (
            scored_direction == "CE" and reversal_trap_context["CE"]["ready"]
        ) or (
            scored_direction == "PE" and reversal_trap_context["PE"]["ready"]
        )

        trade_start = strategy.time_utils._parse_clock(Config.TRADE_START_TIME)
        opening_session = trade_start <= current_now < strategy.time_utils._parse_clock("09:45")
        opening_drive_window = trade_start <= current_now < strategy.time_utils._parse_clock("09:40")
        if opening_session:
            cautions.append("opening_session")

        opening_direction_pressure_aligned = (
            pressure_metrics
            and (
                (scored_direction == "CE" and pressure_metrics["pressure_bias"] == "BULLISH")
                or (scored_direction == "PE" and pressure_metrics["pressure_bias"] == "BEARISH")
            )
        )
        opening_breakout_override = (
            opening_session
            and scored_direction in {"CE", "PE"}
            and score >= 90
            and volume_signal == "STRONG"
            and breakout_body_ok
            and breakout_structure_ok
            and candle_liquidity_ok
            and opening_direction_pressure_aligned
        )
        opening_far_vwap_override = opening_breakout_override and score >= 95

        if atr is not None and abs(price - vwap) > max(atr * tuning["far_vwap_atr_mult"], buffer * 4):
            cautions.append("far_from_vwap")

        cautions = strategy._append_cautions(
            cautions,
            *strategy._adverse_sr_cautions(scored_direction, price, support, resistance, buffer),
        )

        sweep_pressure_override = (
            option_sweep_context
            and option_sweep_context.get("direction") == scored_direction
            and option_sweep_context.get("quality") == "STRONG"
            and option_sweep_context.get("micro_confirmed")
            and option_sweep_context.get("persistence_pairs", 0) >= 3
        )
        if pressure_metrics:
            if scored_direction == "CE" and pressure_metrics["pressure_bias"] == "BEARISH":
                if not sweep_pressure_override:
                    cautions.append("opposite_pressure")
                else:
                    cautions = strategy._append_cautions(cautions, "option_sweep_pressure_override")
            if scored_direction == "PE" and pressure_metrics["pressure_bias"] == "BULLISH":
                if not sweep_pressure_override:
                    cautions.append("opposite_pressure")
                else:
                    cautions = strategy._append_cautions(cautions, "option_sweep_pressure_override")
        pressure_conflict_level = strategy._pressure_conflict_level(pressure_metrics, scored_direction, cautions)
        if sweep_pressure_override and pressure_conflict_level == "MODERATE":
            pressure_conflict_level = "MILD"
        if (
            day_state.get("state") == "REVERSAL_UNDERWAY"
            and day_state.get("direction") == scored_direction
            and pressure_conflict_level == "MODERATE"
        ):
            pressure_conflict_level = "MILD"
        strategy.last_pressure_conflict_level = pressure_conflict_level
        late_day_expansion = strategy._late_day_price_expansion_ready(
            current_now=current_now,
            scored_direction=scored_direction,
            price=price,
            vwap=vwap,
            prev_low=prev_low,
            prev_high=prev_high,
            candle_open=candle_open,
            candle_high=candle_high,
            candle_low=candle_low,
            candle_close=candle_close,
            candle_range=candle_range,
            candle_body=candle_body,
            atr=atr,
            buffer=buffer,
            volume_signal=volume_signal,
            candle_liquidity_ok=candle_liquidity_ok,
            pressure_conflict_level=pressure_conflict_level,
            support=support,
            resistance=resistance,
            wall_break_alert=wall_break_alert,
            support_wall_state=support_wall_state,
            resistance_wall_state=resistance_wall_state,
        )
        if late_day_expansion:
            late_day_label = "breakout" if scored_direction == "CE" else "breakdown"
            cautions = strategy._append_cautions(
                cautions,
                f"late_day_{late_day_label}_watch",
                "theta_fast_exit_required",
            )
            if late_day_expansion["wall_aligned"]:
                cautions = strategy._append_cautions(cautions, "oi_wall_break_confirmed")
                components.append("late_day_oi_wall_confirmed")
            else:
                cautions = strategy._append_cautions(cautions, "oi_wall_not_confirmed")
            aligned_floor = 72 if volume_signal == "STRONG" else 68
            unconfirmed_floor = 70 if volume_signal == "STRONG" else 66
            score = max(
                score,
                aligned_floor if late_day_expansion["wall_aligned"] else unconfirmed_floor,
            )
            strategy.last_score = score
            strategy.last_context_score = score
            strategy.last_score_components = components
            if strategy.last_entry_score <= 0:
                strategy.last_entry_score = max(62, score - 4)
            return (
                strategy._emit_trade_signal(
                    scored_direction,
                    "BREAKOUT_CONFIRM",
                    score,
                    volume_signal,
                    pressure_metrics,
                    cautions,
                    blockers=blockers,
                    regime=f"LATE_DAY_{late_day_label.upper()}",
                    candle_time=candle_time,
                    message=f"Late-day {'resistance breakout above' if scored_direction == 'CE' else 'support breakdown below'} {round(late_day_expansion['level'], 2)}",
                    trigger_price=late_day_expansion["level"],
                    invalidate_price=late_day_expansion["invalidate_price"],
                    atr=atr,
                    support=support,
                    resistance=resistance,
                    remember_level=late_day_expansion["level"],
                    emitted_level=late_day_expansion["level"],
                    buffer=buffer,
                    reset_retest=True,
                    reset_confirmation=True,
                    mark_emitted=True,
                ),
                None,
            )
        strong_sweep_trade_ready = strategy._strong_option_sweep_trade_ready(
            option_sweep_context=option_sweep_context,
            direction=scored_direction,
            price=price,
            vwap=vwap,
            candle_close=candle_close,
            trigger_level=(
                prev_high if scored_direction == "CE" and prev_high is not None
                else prev_low if scored_direction == "PE" and prev_low is not None
                else orb_high if scored_direction == "CE"
                else orb_low
            ),
            atr=atr,
            buffer=buffer,
            candle_liquidity_ok=candle_liquidity_ok,
            score=score,
            entry_score=strategy.last_entry_score,
            pressure_conflict_level=pressure_conflict_level,
        )
        if strong_sweep_trade_ready and regime in {"CHOPPY", "RANGING", "EARLY_SESSION"}:
            breakout_regime_ok = True
            continuation_regime_ok = True
            if "option_sweep_regime_override" not in components:
                components.append("option_sweep_regime_override")
        if nifty_trend_day_context:
            if scored_direction == "CE" and oi_bias in {"BULLISH", "NEUTRAL"} and oi_trend in {"BULLISH", "NEUTRAL", None}:
                bullish_build_up_ok = True
                if pressure_conflict_level == "MODERATE":
                    cautions = strategy._append_cautions(cautions, "trend_day_price_override")
            elif scored_direction == "PE" and oi_bias in {"BEARISH", "NEUTRAL"} and oi_trend in {"BEARISH", "NEUTRAL", None}:
                bearish_build_up_ok = True
                if pressure_conflict_level == "MODERATE":
                    cautions = strategy._append_cautions(cautions, "trend_day_price_override")
        provisional_confidence = strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
        time_thresholds = strategy._get_time_regime_thresholds(time_regime, fallback_mode, market_regime=regime)
        neutral_pressure_soft_watch = (
            pressure_metrics
            and pressure_metrics["pressure_bias"] == "NEUTRAL"
            and scored_direction in {"CE", "PE"}
            and strategy._direction_vwap_aligned(scored_direction, price, vwap)
            and volume_signal in ["NORMAL", "STRONG"]
            and score >= max(time_thresholds["breakout_min_score"] - 2, 52)
        )
        neutral_pressure_reversal_ok = strategy._neutral_pressure_reversal_ready(
            scored_direction=scored_direction,
            score=score,
            entry_score=score,
            pressure_metrics=pressure_metrics,
            prev_high=prev_high,
            prev_low=prev_low,
            prev_close=prev_close,
            candle_close=candle_close,
            candle_high=candle_high,
            candle_low=candle_low,
            vwap=vwap,
            support=support,
            resistance=resistance,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            candle_liquidity_ok=candle_liquidity_ok,
            cautions=cautions,
        )
        if pressure_metrics and pressure_metrics["pressure_bias"] == "NEUTRAL":
            day_state_aligned = (
                (day_state.get("state") or "").upper() in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
                and scored_direction in {"CE", "PE"}
                and (day_state.get("direction") or "").upper() == scored_direction
            )
            if neutral_pressure_reversal_ok:
                cautions.append("pressure_neutral")
            elif neutral_pressure_soft_watch or (
                day_state_aligned
                and scored_direction in {"CE", "PE"}
                and score >= max(time_thresholds["breakout_min_score"], 70)
                and volume_signal in ["NORMAL", "STRONG"]
            ):
                cautions.append("pressure_neutral")
            else:
                blockers.append("pressure_neutral")
                strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime="CHOPPY")
                strategy.last_is_expiry_day = strategy.expiry_rules.is_expiry_day(expiry)
                return (None, f"Pressure not aligned | score={score}"), None
        adx_trade_ok = True
        mtf_trade_ok = True
        if scored_direction and recent_candles_5m and len(recent_candles_5m) >= 15:
            adx_ok, _, _ = strategy._check_adx_filter(recent_candles_5m, scored_direction)
            adx_trade_ok = adx_ok or (score >= 85 and volume_signal == "STRONG")
            if not adx_ok:
                cautions.append("adx_not_confirmed")
        if scored_direction and trend_15m is not None:
            tf_ok, _, _ = strategy._check_multi_timeframe_filter(trend_15m, scored_direction)
            mtf_trade_ok = tf_ok or (score >= 80 and volume_signal == "STRONG")
            if not tf_ok:
                cautions.append("higher_tf_not_aligned")

        clean_trend_leg = TrendQualityRefiner.is_clean_trend_leg(
            scored_direction=scored_direction,
            day_state=day_state,
            price=price,
            vwap=vwap,
            volume_signal=volume_signal,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            candle_liquidity_ok=candle_liquidity_ok,
            pressure_conflict_level=pressure_conflict_level,
            opposite_pressure_present="opposite_pressure" in cautions,
            adx_trade_ok=adx_trade_ok,
            mtf_trade_ok=mtf_trade_ok,
            score=score,
            entry_score=strategy.last_entry_score,
        )
        cautions = TrendQualityRefiner.refine_cautions(cautions, clean_trend_leg=clean_trend_leg)
        if clean_trend_leg and "clean_trend_leg" not in components:
            components.append("clean_trend_leg")

        strategy.last_entry_score = strategy._calculate_entry_score(
            score=score,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            candle_liquidity_ok=candle_liquidity_ok,
            volume_signal=volume_signal,
            cautions=cautions,
            blockers=blockers,
            adx_trade_ok=adx_trade_ok,
            mtf_trade_ok=mtf_trade_ok,
            pressure_conflict_level=pressure_conflict_level,
            price_structure_score=strategy.last_price_structure_score,
            option_flow_score=strategy.last_option_flow_score,
            oi_structure_score=strategy.last_oi_structure_score,
            contract_quality_score=strategy.last_contract_quality_score,
        )
        if strategy.instrument == "SENSEX" and Config.FOCUSED_MANUAL_MODE and strategy.last_entry_score > 0:
            strategy.last_entry_score = min(100, strategy.last_entry_score + 6)
        if wall_break_supports_direction and strategy.last_entry_score > 0:
            strategy.last_entry_score = min(100, strategy.last_entry_score + 4)
        if divergence_against_direction and strategy.last_entry_score > 0:
            strategy.last_entry_score = max(0, strategy.last_entry_score - 6)
        recent_breakout_context = strategy._recent_breakout_context(
            scored_direction,
            candle_time,
            price,
            vwap,
            buffer,
        )

        expiry_eval = strategy.expiry_rules.evaluate(
            expiry_value=expiry,
            score=score,
            entry_score=strategy.last_entry_score,
            confidence=provisional_confidence,
            price=price,
            vwap=vwap,
            volume_signal=volume_signal,
            pressure_metrics=pressure_metrics,
            current_signal=scored_direction,
            blockers=blockers,
            cautions=cautions,
            day_state=day_state,
        )
        blockers = expiry_eval["blockers"]
        cautions = expiry_eval["cautions"]
        strategy.last_is_expiry_day = expiry_eval["is_expiry_day"]
        adaptive_expiry_continuation_mode = expiry_eval.get("adaptive_continuation_mode", False)
        soften_build_up_requirement = expiry_eval.get("soften_build_up_requirement", False)
        soften_pressure_conflict = expiry_eval.get("soften_pressure_conflict", False)
        expiry_session_mode = expiry_eval.get("session_mode", "NORMAL")

        if not expiry_eval["allow_trade"]:
            strategy._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else regime,
                signal_type="NONE",
            )
            return (None, f"Expiry filter blocked trade | score={score}"), None

        sensex_late_day_block = strategy._sensex_late_day_guard_with_context(
            current_now=current_now,
            score=score,
            entry_score=strategy.last_entry_score,
            confidence=provisional_confidence,
            volume_signal=volume_signal,
            pressure_conflict_level=pressure_conflict_level,
            option_sweep_context=option_sweep_context,
            direction=scored_direction,
        )
        if sensex_late_day_block:
            blockers.append(sensex_late_day_block)
            strategy._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=provisional_confidence,
                regime=regime,
                signal_type="NONE",
            )
            return (None, f"SENSEX late-day guard blocked trade | score={score}"), None

        if soften_build_up_requirement:
            if scored_direction == "CE" and oi_bias == "BULLISH" and score >= max(expiry_eval["score_floor"], 64):
                bullish_build_up_ok = True
            elif scored_direction == "PE" and oi_bias == "BEARISH" and score >= max(expiry_eval["score_floor"], 64):
                bearish_build_up_ok = True

        adaptive_expiry_continuation_ok = (
            adaptive_expiry_continuation_mode
            and scored_direction in {"CE", "PE"}
            and volume_signal in {"NORMAL", "STRONG"}
            and candle_liquidity_ok
            and (breakout_body_ok or breakout_structure_ok)
            and pressure_conflict_level in {"NONE", "MILD"}
            and strategy.last_entry_score >= 60
            and (
                (scored_direction == "CE" and bullish_ha_ok and price > vwap)
                or (scored_direction == "PE" and bearish_ha_ok and price < vwap)
            )
        )
        nifty_post_expiry_continuation_ok = strategy._nifty_post_expiry_continuation_ready(
            direction=scored_direction,
            score=score,
            entry_score=strategy.last_entry_score,
            expiry_session_mode=expiry_session_mode,
            time_regime=time_regime,
            price=price,
            vwap=vwap,
            orb_high=orb_high,
            orb_low=orb_low,
            candle_close=candle_close,
            candle_range=candle_range,
            atr=atr,
            candle_liquidity_ok=candle_liquidity_ok,
            breakout_body_ok=breakout_body_ok,
            breakout_structure_ok=breakout_structure_ok,
            ha_ok=bullish_ha_ok if scored_direction == "CE" else bearish_ha_ok,
            pressure_conflict_level=pressure_conflict_level,
            volume_signal=volume_signal,
            recent_breakout_context=recent_breakout_context,
        )
        continuation_override_ok = adaptive_expiry_continuation_ok or nifty_post_expiry_continuation_ok or nifty_trend_day_context

        ce_options_vol, ce_vol_boost, ce_vol_reason = strategy._analyze_options_volume(atm_ce_volume, atm_pe_volume, "CE")
        pe_options_vol, pe_vol_boost, pe_vol_reason = strategy._analyze_options_volume(atm_ce_volume, atm_pe_volume, "PE")

        if Config.TEST_MODE:
            ce_options_ok = ce_options_vol in ["STRONG", "NORMAL", "NEUTRAL"]
            if ce_options_vol == "WEAK":
                cautions.append(f"ce_{ce_vol_reason}")

            if (
                price > vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and oi_bias == "BULLISH"
                and oi_trend == "BULLISH"
                and build_up in ["LONG_BUILDUP", "SHORT_COVERING"]
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BULLISH", "NEUTRAL"])
                and score >= 55
                and ce_options_ok
            ):
                adjusted_score = score + ce_vol_boost
                confidence = strategy._confidence_from_score(adjusted_score, volume_signal, pressure_metrics, cautions)
                strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=regime, signal_type="CONTINUATION")
                return ("CE", f"VWAP + Vol + OI + OptVol({ce_vol_reason}) | score={adjusted_score}"), None

            pe_options_ok = pe_options_vol in ["STRONG", "NORMAL", "NEUTRAL"]
            if pe_options_vol == "WEAK":
                cautions.append(f"pe_{pe_vol_reason}")

            elif (
                price < vwap
                and volume_signal in ["STRONG", "NORMAL"]
                and oi_bias == "BEARISH"
                and oi_trend == "BEARISH"
                and build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]
                and (not pressure_metrics or pressure_metrics["pressure_bias"] in ["BEARISH", "NEUTRAL"])
                and score >= 55
                and pe_options_ok
            ):
                adjusted_score = score + pe_vol_boost
                confidence = strategy._confidence_from_score(adjusted_score, volume_signal, pressure_metrics, cautions)
                strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence=confidence, regime=regime, signal_type="CONTINUATION")
                return ("PE", f"VWAP + Vol + OI + OptVol({pe_vol_reason}) | score={adjusted_score}"), None

            blockers.append("test_mode_filters_incomplete")
            strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
            return (None, f"TEST MODE: No setup | score={score}"), None

        orb_ready = orb_high is not None and orb_low is not None
        if not orb_ready and score < 70:
            blockers.append("orb_not_ready")
            strategy._set_diagnostics(blockers=blockers, cautions=cautions, confidence="LOW", regime=regime, signal_type="NONE")
            return (None, f"ORB not ready | score={score}"), None

        retest_zone = max(buffer * 1.2, tuning["retest_zone_floor"])
        active_retest = strategy.retest_setup
        active_confirmation = strategy.confirmation_setup

        section_ctx = locals().copy()
        section_ctx.pop("strategy", None)
        return None, section_ctx


def build_trade_signal_context(strategy, **kwargs):
    return TradeSignalContextBuilder.build(strategy, **kwargs)
