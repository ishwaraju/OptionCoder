from config import Config


class NoSetupFinalizer:
    @staticmethod
    def finalize(strategy, ctx):
        price = ctx["price"]
        vwap = ctx["vwap"]
        volume_signal = ctx["volume_signal"]
        oi_bias = ctx["oi_bias"]
        oi_trend = ctx["oi_trend"]
        build_up = ctx["build_up"]
        buffer = ctx["buffer"]
        pressure_metrics = ctx["pressure_metrics"]
        score = ctx["score"]
        scored_direction = ctx["scored_direction"]
        components = ctx["components"]
        blockers = ctx["blockers"]
        cautions = ctx["cautions"]
        tuning = ctx["tuning"]
        candle_liquidity_ok = ctx["candle_liquidity_ok"]
        divergence_against_direction = ctx["divergence_against_direction"]
        wall_break_supports_direction = ctx["wall_break_supports_direction"]
        pressure_conflict_level = ctx["pressure_conflict_level"]
        bullish_buildups = ctx["bullish_buildups"]
        bearish_buildups = ctx["bearish_buildups"]
        fallback_mode = ctx["fallback_mode"]
        bullish_build_up_ok = ctx["bullish_build_up_ok"]
        bearish_build_up_ok = ctx["bearish_build_up_ok"]
        expiry_eval = ctx["expiry_eval"]
        expiry_session_mode = ctx["expiry_session_mode"]
        soften_build_up_requirement = ctx["soften_build_up_requirement"]
        soften_pressure_conflict = ctx["soften_pressure_conflict"]
        time_regime = ctx["time_regime"]
        orb_high = ctx["orb_high"]
        orb_low = ctx["orb_low"]

        blockers.append("no_valid_setup")
        if fallback_mode:
            blockers.append("oi_only_context")
        if score < Config.MIN_SCORE_THRESHOLD:
            blockers.append("score_below_threshold")
        if scored_direction is None:
            blockers.append("direction_unresolved")
        if volume_signal == "WEAK":
            if not (
                strategy.instrument == "SENSEX"
                and score >= 54
                and scored_direction in {"CE", "PE"}
                and strategy._direction_vwap_aligned(scored_direction, price, vwap)
                and candle_liquidity_ok
                and time_regime in {"OPENING", "MID_MORNING"}
            ):
                blockers.append("volume_weak")
            else:
                cautions = strategy._append_cautions(cautions, "volume_weak")
        if not candle_liquidity_ok:
            blockers.append("low_tick_density")
        if divergence_against_direction and (
            volume_signal != "STRONG"
            or strategy.last_entry_score < 66
            or pressure_conflict_level not in {"NONE", "MILD"}
        ):
            blockers.append("oi_divergence_conflict")
        if scored_direction == "CE":
            if price <= vwap:
                blockers.append("vwap_not_supportive")
            if oi_bias == "BEARISH" or oi_trend == "BEARISH":
                blockers.append("oi_conflict")
            if not bullish_build_up_ok and not fallback_mode:
                if soften_build_up_requirement and expiry_session_mode == "POST_EXPIRY_REBUILD" and score >= expiry_eval["score_floor"]:
                    cautions = strategy._append_cautions(cautions, "build_up_missing")
                else:
                    blockers.append("build_up_missing")
            elif build_up not in bullish_buildups:
                blockers.append("build_up_inferred")
            if "opposite_pressure" in cautions or (pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH"):
                if not (
                    strategy.instrument == "SENSEX"
                    and pressure_conflict_level == "MILD"
                    and score >= 56
                    and time_regime in {"OPENING", "MID_MORNING"}
                ) and not (
                    wall_break_supports_direction
                    and strategy.last_entry_score >= 68
                    and pressure_conflict_level == "MILD"
                ) and not (
                    soften_pressure_conflict
                    and pressure_conflict_level == "MILD"
                    and score >= expiry_eval["score_floor"]
                    and strategy.last_entry_score >= 60
                ):
                    blockers.append("pressure_conflict")
                else:
                    cautions = strategy._append_cautions(cautions, "pressure_conflict")
            if orb_high is not None and price <= orb_high:
                blockers.append("orb_breakout_missing")
            if orb_high is not None and price > orb_high + (buffer * tuning["extension_buffer_mult"]):
                blockers.append("orb_extension_too_far")
        elif scored_direction == "PE":
            if price >= vwap:
                blockers.append("vwap_not_supportive")
            if oi_bias == "BULLISH" or oi_trend == "BULLISH":
                blockers.append("oi_conflict")
            if not bearish_build_up_ok and not fallback_mode:
                if soften_build_up_requirement and expiry_session_mode == "POST_EXPIRY_REBUILD" and score >= expiry_eval["score_floor"]:
                    cautions = strategy._append_cautions(cautions, "build_up_missing")
                else:
                    blockers.append("build_up_missing")
            elif build_up not in bearish_buildups:
                blockers.append("build_up_inferred")
            if "opposite_pressure" in cautions or (pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH"):
                if not (
                    strategy.instrument == "SENSEX"
                    and pressure_conflict_level == "MILD"
                    and score >= 56
                    and time_regime in {"OPENING", "MID_MORNING"}
                ) and not (
                    wall_break_supports_direction
                    and strategy.last_entry_score >= 68
                    and pressure_conflict_level == "MILD"
                ) and not (
                    soften_pressure_conflict
                    and pressure_conflict_level == "MILD"
                    and score >= expiry_eval["score_floor"]
                    and strategy.last_entry_score >= 60
                ):
                    blockers.append("pressure_conflict")
                else:
                    cautions = strategy._append_cautions(cautions, "pressure_conflict")
            if orb_low is not None and price >= orb_low:
                blockers.append("orb_breakout_missing")
            if orb_low is not None and price < orb_low - (buffer * tuning["extension_buffer_mult"]):
                blockers.append("orb_extension_too_far")
        pending_confirmation_blockers = {
            "no_valid_setup",
            "build_up_missing",
            "build_up_inferred",
            "pressure_conflict",
            "orb_breakout_missing",
            "orb_extension_too_far",
        }
        hard_invalid_blockers = {
            "score_below_threshold",
            "direction_unresolved",
            "volume_weak",
            "low_tick_density",
            "vwap_not_supportive",
            "oi_conflict",
            "oi_divergence_conflict",
        }
        if (
            score >= 90
            and scored_direction in {"CE", "PE"}
            and not set(blockers).intersection(hard_invalid_blockers)
            and set(blockers).intersection(pending_confirmation_blockers)
        ):
            blockers = [flag for flag in blockers if flag != "no_valid_setup"]
            blockers.append("high_score_confirmation_pending")
            confidence = strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions)
            strategy._set_diagnostics(
                blockers=blockers,
                cautions=cautions,
                confidence=confidence,
                regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else ctx["regime"],
                signal_type=strategy._watch_signal_type(cautions, "NONE"),
            )
            strategy.last_decision_state = strategy._derive_decision_state(
                signal_type=strategy._watch_signal_type(cautions, "NONE"),
                signal=None,
                score=strategy.last_context_score,
                entry_score=strategy.last_entry_score,
                confidence=strategy.last_confidence,
                blockers=strategy.last_blockers,
                cautions=strategy.last_cautions,
            )
            return None, f"High-score setup pending confirmation | score={score}"
        strategy._set_diagnostics(
            blockers=blockers,
            cautions=cautions,
            confidence=strategy._confidence_from_score(score, volume_signal, pressure_metrics, cautions),
            regime="EXPIRY_DAY" if expiry_eval["is_expiry_day"] else ctx["regime"],
            signal_type=strategy._watch_signal_type(cautions, "NONE"),
        )
        strategy.last_decision_state = strategy._derive_decision_state(
            signal_type=strategy._watch_signal_type(cautions, "NONE"),
            signal=None,
            score=strategy.last_context_score,
            entry_score=strategy.last_entry_score,
            confidence=strategy.last_confidence,
            blockers=strategy.last_blockers,
            cautions=strategy.last_cautions,
        )
        return None, f"No valid setup | score={score}"


def finalize_no_setup(strategy, ctx):
    return NoSetupFinalizer.finalize(strategy, ctx)
