"""Option guardrails and candidate helpers for SignalService."""

from datetime import timedelta

from config import Config
from .premium_elasticity_engine import PremiumElasticityEngine


class OptionSignalGuard:
    @staticmethod
    def pro_trader_quality_check(
        service,
        *,
        signal,
        selected_option_contract=None,
        premium_guard=None,
        risk_profile=None,
        elasticity=None,
        entry_phase=None,
    ):
        """20-year option-trader style sanity check before converting setup to ACTION."""
        signal = (signal or "").upper()
        premium_guard = premium_guard or {}
        risk_profile = risk_profile or {}
        elasticity = elasticity or {}
        option = selected_option_contract or {}
        reasons = []
        score = 100.0

        ltp = option.get("ltp")
        if ltp in {None, ""}:
            ltp = premium_guard.get("current_ltp")
        ltp = float(ltp) if ltp not in {None, ""} else None
        spread_pct = premium_guard.get("spread_pct")
        if spread_pct is None and selected_option_contract:
            spread_pct = service._spread_percent(selected_option_contract)
        spread_pct = float(spread_pct) if spread_pct not in {None, ""} else None
        raw_distance = option.get("distance_from_atm")
        distance = int(raw_distance) if raw_distance not in {None, ""} else None
        theta = option.get("theta")
        theta = abs(float(theta)) if theta not in {None, ""} else None
        target_pct = risk_profile.get("target_pct") or risk_profile.get("target")
        stop_pct = risk_profile.get("hard_premium_stop_pct") or risk_profile.get("hard_stop") or risk_profile.get("stop_loss_pct")
        rr_ratio = None
        if target_pct is not None and stop_pct not in {None, 0, "0"}:
            rr_ratio = float(target_pct) / max(float(stop_pct), 0.01)

        max_spread = float(getattr(Config, "PRO_TRADER_MAX_ACTION_SPREAD_PCT", 3.8) or 3.8)
        min_premium = float(getattr(Config, "PRO_TRADER_MIN_PREMIUM", 25.0) or 25.0)
        max_otm_distance = int(getattr(Config, "PRO_TRADER_MAX_OTM_DISTANCE", 2) or 2)
        max_itm_distance = int(getattr(Config, "PRO_TRADER_MAX_ITM_DISTANCE", 5) or 5)
        min_rr = float(getattr(Config, "PRO_TRADER_MIN_RR", 1.25) or 1.25)
        max_theta_pct = float(getattr(Config, "PRO_TRADER_MAX_THETA_PCT", 8.0) or 8.0)

        hard_reject = False
        if ltp is None or ltp <= 0:
            if selected_option_contract:
                hard_reject = True
                score -= 45
                reasons.append("premium_missing")
            else:
                score -= 10
                reasons.append("contract_snapshot_missing")
        elif ltp < min_premium:
            lottery_exception = (
                premium_guard.get("volume_supporting")
                and (premium_guard.get("premium_momentum_pct") is not None and float(premium_guard.get("premium_momentum_pct")) >= 2.5)
                and (spread_pct is None or spread_pct <= 2.5)
            )
            if not lottery_exception:
                score -= 30
                reasons.append("cheap_lottery_premium")

        if spread_pct is not None and spread_pct > max_spread:
            score -= 35
            reasons.append("execution_spread_wide")
            if spread_pct >= max_spread + 2.0:
                hard_reject = True

        if signal == "CE":
            otm_distance = max(distance or 0, 0)
            itm_distance = max(-(distance or 0), 0)
        else:
            otm_distance = max(-(distance or 0), 0)
            itm_distance = max(distance or 0, 0)
        if distance is not None and otm_distance > max_otm_distance:
            score -= 28
            reasons.append("too_far_otm")
            if otm_distance > max_otm_distance + 1:
                hard_reject = True
        if distance is not None and itm_distance > max_itm_distance:
            score -= 12
            reasons.append("too_deep_itm")

        if rr_ratio is not None and rr_ratio < min_rr:
            score -= 24
            reasons.append("rr_not_worth_risk")

        theta_pct = None
        if theta is not None and ltp:
            theta_pct = (theta / max(ltp, 1.0)) * 100.0
            if theta_pct > max_theta_pct:
                score -= 22
                reasons.append("theta_drag_high")

        if elasticity.get("dead_premium_risk"):
            score -= 35
            hard_reject = True
            reasons.append("dead_premium_risk")
        elif float(elasticity.get("score") or 100.0) < 58:
            score -= 16
            reasons.append("premium_elasticity_dull")

        if (entry_phase or "").upper() == "LATE_CHASE_SIGNAL" and (rr_ratio is None or rr_ratio < 1.5):
            score -= 25
            reasons.append("late_chase_asymmetry_poor")

        score = round(max(min(score, 100.0), 0.0), 2)
        if hard_reject or score < 55:
            label = "PRO_REJECT"
        elif score < 75:
            label = "PRO_WATCH"
        else:
            label = "PRO_PASS"
        if not selected_option_contract and label == "PRO_REJECT":
            label = "PRO_WATCH"
        summary_bits = [label, f"score={score}"]
        if rr_ratio is not None:
            summary_bits.append(f"RR={rr_ratio:.2f}")
        if spread_pct is not None:
            summary_bits.append(f"spread={spread_pct:.2f}%")
        if theta_pct is not None:
            summary_bits.append(f"theta={theta_pct:.1f}%")
        return {
            "label": label,
            "score": score,
            "reasons": reasons,
            "rr_ratio": rr_ratio,
            "spread_pct": spread_pct,
            "theta_pct": theta_pct,
            "summary": " | ".join(summary_bits),
        }

    @staticmethod
    def classify_signal_family(service, signal, signal_type, entry_phase):
        signal = (signal or "").upper()
        signal_type = (signal_type or "").upper()
        entry_phase = (entry_phase or "").upper()
        trend_leg_stage = (getattr(service.strategy, "last_trend_leg_stage", None) or "NEUTRAL").upper()

        if signal_type == "TRAP_REVERSAL":
            return "TRAP_REVERSAL"
        if signal_type == "REVERSAL":
            return "FAILURE_REVERSAL"
        if signal_type == "RETEST" or entry_phase == "RETEST_SIGNAL" or trend_leg_stage == "FIRST_RETEST":
            return "RETEST_CONTINUATION"
        if entry_phase == "LATE_CHASE_SIGNAL" or trend_leg_stage == "STRETCHED":
            return "LATE_EXTENSION"
        if signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "CONTINUATION", "AGGRESSIVE_CONTINUATION", "OPENING_DRIVE"}:
            return "IMPULSE_BREAKOUT"
        return "GENERAL_SETUP"

    @staticmethod
    def classify_entry_phase(service, signal, signal_type, candle_time, price=None, trigger_price=None):
        signal = (signal or "").upper()
        signal_type = (signal_type or "").upper()
        cautions = set(getattr(service.strategy, "last_cautions", []) or [])
        trend_leg_stage = (getattr(service.strategy, "last_trend_leg_stage", None) or "NEUTRAL").upper()
        trend_family = {
            "BREAKOUT",
            "BREAKOUT_CONFIRM",
            "CONTINUATION",
            "AGGRESSIVE_CONTINUATION",
            "OPENING_DRIVE",
        }
        if signal_type == "RETEST":
            return "RETEST_SIGNAL"
        if trend_leg_stage == "FIRST_RETEST":
            return "RETEST_SIGNAL"
        if trend_leg_stage == "STRETCHED":
            return "LATE_CHASE_SIGNAL"

        last = getattr(service.strategy, "last_emitted_signal", None) or {}
        if (
            signal in {"CE", "PE"}
            and candle_time is not None
            and last
            and last.get("direction") == signal
            and last.get("session_day") == candle_time.date()
        ):
            try:
                minutes_apart = int((candle_time - last.get("time")).total_seconds() // 60)
            except Exception:
                minutes_apart = None
            same_family_repeat = (
                signal_type == (last.get("signal_type") or "").upper()
                or (
                    signal_type in trend_family
                    and (last.get("signal_type") or "").upper() in trend_family
                )
            )
            if minutes_apart is not None and minutes_apart <= 20 and same_family_repeat:
                return "REENTRY_SIGNAL"

        late_session = candle_time is not None and (candle_time.hour, candle_time.minute) >= (14, 25)
        late_risk_flags = {"far_from_vwap", "late_day_breakdown_watch", "theta_fast_exit_required"}
        late_bias = late_session or (
            candle_time is not None
            and (candle_time.hour, candle_time.minute) >= (13, 45)
            and bool(cautions.intersection(late_risk_flags))
        )
        if late_bias:
            return "LATE_CHASE_SIGNAL"

        if (
            signal in {"CE", "PE"}
            and trigger_price is not None
            and price is not None
            and service._entry_too_extended(signal, price, trigger_price, getattr(service.atr, "atr", None), service.atr.get_buffer())
        ):
            return "LATE_CHASE_SIGNAL"

        return "FIRST_SIGNAL_IN_MOVE"

    @staticmethod
    def assess_high_expectancy(
        service,
        signal,
        candle_time,
        balanced_pro=None,
        selected_option_contract=None,
        premium_guard=None,
        risk_profile=None,
        price=None,
    ):
        signal = (signal or "").upper()
        signal_type = (getattr(service.strategy, "last_signal_type", None) or "NONE").upper()
        instrument = (getattr(service, "instrument", None) or "").upper()
        score = float(getattr(service.strategy, "last_score", 0) or 0)
        entry_score = float(getattr(service.strategy, "last_entry_score", score) or score)
        confidence = (getattr(service.strategy, "last_confidence", None) or "LOW").upper()
        signal_grade = (getattr(service.strategy, "last_signal_grade", None) or "SKIP").upper()
        cautions = set(getattr(service.strategy, "last_cautions", []) or [])
        pressure_conflict = (getattr(service.strategy, "last_pressure_conflict_level", None) or "NONE").upper()
        active_day_state = (getattr(service.strategy, "last_active_day_state", None) or "").upper()
        day_state_direction = (getattr(service.strategy, "last_day_state_direction", None) or "").upper()
        trend_leg_stage = (getattr(service.strategy, "last_trend_leg_stage", None) or "NEUTRAL").upper()
        session_map_phase = (getattr(service.strategy, "last_session_map_phase", None) or "UNKNOWN").upper()
        futures_acceptance = getattr(service.strategy, "last_futures_acceptance", None) or {}
        initiative_strength_score = float(getattr(service.strategy, "last_initiative_strength_score", 0) or 0)
        trigger_price = (
            service.strategy.last_entry_plan.get("entry_above")
            if signal == "CE"
            else service.strategy.last_entry_plan.get("entry_below")
        )
        entry_phase = OptionSignalGuard.classify_entry_phase(
            service,
            signal,
            signal_type,
            candle_time,
            price=price,
            trigger_price=trigger_price,
        )
        premium_guard = premium_guard or {}
        premium_label = (premium_guard.get("label") or "").upper()
        premium_momentum_pct = premium_guard.get("premium_momentum_pct")
        spread_pct = premium_guard.get("spread_pct")
        if spread_pct is None and selected_option_contract:
            spread_pct = service._spread_percent(selected_option_contract)
        if premium_momentum_pct is None and premium_label == "PREMIUM_OK":
            premium_momentum_pct = 0.0

        raw_participation_hard_count = len(
            cautions.intersection(
                {
                    "participation_weak",
                    "participation_delta_missing",
                }
            )
        )
        participation_soft_count = len(cautions.intersection({"participation_baseline_weak"}))
        weak_participation_count = raw_participation_hard_count + participation_soft_count
        late_risk_count = len(
            cautions.intersection(
                {"far_from_vwap", "theta_fast_exit_required", "late_day_breakdown_watch"}
            )
        )
        day_state_aligned = (
            active_day_state in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
            and day_state_direction == signal
        )
        clean_spread = spread_pct is None or float(spread_pct) <= 3.5
        clean_trend_leg = trend_leg_stage in {"FIRST_IMPULSE", "FIRST_RETEST"}
        futures_acceptance_strong = bool(futures_acceptance.get("accepted")) and float(futures_acceptance.get("score") or 0) >= 58
        premium_confirmed = False
        if premium_label == "PREMIUM_OK":
            if signal_type in {"REVERSAL", "TRAP_REVERSAL"}:
                premium_confirmed = premium_momentum_pct is None or float(premium_momentum_pct) >= 0.0
            else:
                premium_threshold = 1.0
                if (
                    signal_type == "RETEST"
                    and futures_acceptance_strong
                    and clean_spread
                    and clean_trend_leg
                ):
                    premium_threshold = 0.25 if instrument in {"BANKNIFTY", "SENSEX"} else 0.35
                if (
                    day_state_aligned
                    and entry_phase in {"FIRST_SIGNAL_IN_MOVE", "RETEST_SIGNAL"}
                    and clean_spread
                    and signal_type in {
                        "BREAKOUT",
                        "BREAKOUT_CONFIRM",
                        "CONTINUATION",
                        "AGGRESSIVE_CONTINUATION",
                        "RETEST",
                        "OPENING_DRIVE",
                    }
                ):
                    premium_threshold = 0.5
                premium_confirmed = (
                    premium_momentum_pct is not None
                    and float(premium_momentum_pct) >= premium_threshold
                )
        volume_supporting = bool(premium_guard.get("volume_supporting"))
        clean_breakout_premium = bool(
            premium_guard.get("clean_breakout_premium")
            or (
                premium_confirmed
                and clean_spread
                and volume_supporting
                and premium_momentum_pct is not None
                and float(premium_momentum_pct) >= 1.0
            )
        )
        breakout_family = {
            "BREAKOUT",
            "BREAKOUT_CONFIRM",
            "CONTINUATION",
            "AGGRESSIVE_CONTINUATION",
            "RETEST",
            "OPENING_DRIVE",
        }
        signal_family = OptionSignalGuard.classify_signal_family(service, signal, signal_type, entry_phase)
        strong_price_action_watch = (
            futures_acceptance_strong
            and initiative_strength_score >= 28
            and signal_type in breakout_family
            and entry_phase in {"FIRST_SIGNAL_IN_MOVE", "RETEST_SIGNAL"}
        )
        nifty_watch_allowed = not (
            instrument == "NIFTY"
            and (
                signal_family != "IMPULSE_BREAKOUT"
                or raw_participation_hard_count >= 1
                or initiative_strength_score < 34
                or float(futures_acceptance.get("score") or 0) < 64
            )
        )
        price_led_participation_override = (
            strong_price_action_watch
            and clean_spread
            and signal_family == "IMPULSE_BREAKOUT"
            and raw_participation_hard_count <= 2
            and initiative_strength_score >= 32
            and float(futures_acceptance.get("score") or 0) >= 60
        )
        participation_hard_count = 0 if price_led_participation_override else raw_participation_hard_count
        clean_tactical_context = (
            pressure_conflict in {"NONE", "MILD"}
            and participation_hard_count == 0
            and participation_soft_count <= 1
            and clean_spread
            and entry_phase in {"FIRST_SIGNAL_IN_MOVE", "RETEST_SIGNAL"}
            and late_risk_count <= 1
        )
        elasticity = PremiumElasticityEngine.evaluate(
            signal=signal,
            underlying_price=price,
            trigger_price=trigger_price,
            premium_guard=premium_guard,
            selected_option_contract=selected_option_contract,
            futures_acceptance=futures_acceptance,
        )
        dead_premium_risk = bool(elasticity.get("dead_premium_risk"))
        pro_check = OptionSignalGuard.pro_trader_quality_check(
            service,
            signal=signal,
            selected_option_contract=selected_option_contract,
            premium_guard=premium_guard,
            risk_profile=risk_profile,
            elasticity=elasticity,
            entry_phase=entry_phase,
        )
        option_sweep_starter = (
            signal in {"CE", "PE"}
            and bool(getattr(service, "option_sweep_context", None))
            and service._should_soften_option_sweep_filters(signal)
            and premium_label == "PREMIUM_OK"
            and clean_spread
            and pressure_conflict in {"NONE", "MILD"}
            and participation_hard_count == 0
            and late_risk_count <= 1
            and signal_type in breakout_family
            and signal_family in {"IMPULSE_BREAKOUT", "RETEST_CONTINUATION"}
            and entry_score >= 88
            and score >= 88
            and not dead_premium_risk
        )
        small_size_price_led_entry = (
            strong_price_action_watch
            and signal_family == "IMPULSE_BREAKOUT"
            and premium_label == "PREMIUM_OK"
            and clean_spread
            and participation_hard_count == 0
            and late_risk_count <= 1
            and entry_score >= (88 if instrument == "NIFTY" else 84)
            and score >= (84 if instrument == "NIFTY" else 80)
            and (
                volume_supporting
                or (
                    instrument in {"BANKNIFTY", "SENSEX"}
                    and (
                        initiative_strength_score >= 38
                        or float(futures_acceptance.get("score") or 0) >= 68
                    )
                )
            )
            and premium_momentum_pct is not None
            and float(premium_momentum_pct) >= (0.25 if instrument == "NIFTY" else 0.0)
            and not dead_premium_risk
        )

        quality_tag = "AVOID"
        allow_trade = False
        watch_only = False
        likely_runner = False
        path_quality = "VOLATILE_PATH"
        reasons = []

        if confidence not in {"MEDIUM", "HIGH"} or signal_grade == "WATCH":
            reasons.append("confidence_or_grade_too_weak")
            return {
                "quality_tag": quality_tag,
                "allow_trade": False,
                "watch_only": False,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": path_quality,
                "likely_runner": likely_runner,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }

        if (
            entry_phase == "LATE_CHASE_SIGNAL"
            and late_risk_count >= 2
            and entry_score < 94
        ):
            reasons.append("late_chase_with_poor_asymmetry")
            return {
                "quality_tag": quality_tag,
                "allow_trade": False,
                "watch_only": False,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": path_quality,
                "likely_runner": likely_runner,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }

        if (
            candle_time is not None
            and (candle_time.hour, candle_time.minute) <= (11, 0)
            and signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "OPENING_DRIVE"}
            and weak_participation_count >= 2
        ):
            reasons.append("opening_breakout_without_participation")
            return {
                "quality_tag": quality_tag,
                "allow_trade": False,
                "watch_only": False,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": path_quality,
                "likely_runner": likely_runner,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }

        if signal_type in {"REVERSAL", "TRAP_REVERSAL"}:
            if pressure_conflict == "NONE" and entry_score >= 90 and (day_state_aligned or score >= 85):
                quality_tag = "RQ"
                reversal_premium_momentum = (
                    premium_momentum_pct is not None
                    and float(premium_momentum_pct) >= 2.0
                )
                allow_trade = (
                    premium_confirmed
                    and reversal_premium_momentum
                    and participation_hard_count == 0
                    and clean_spread
                )
                path_quality = "CLEAN_PATH" if participation_hard_count == 0 else "TACTICAL_PATH"
                likely_runner = allow_trade
                if not allow_trade:
                    watch_only = True
                    reasons.append("reversal_needs_runner_premium_confirmation")
            else:
                reasons.append("reversal_not_elite_enough")
            if allow_trade and pro_check["label"] != "PRO_PASS":
                allow_trade = False
                watch_only = pro_check["label"] == "PRO_WATCH"
                likely_runner = False
                reasons.extend(["pro_trader_check_watch_only", *pro_check["reasons"]])
            return {
                "quality_tag": quality_tag,
                "allow_trade": allow_trade,
                "watch_only": watch_only,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": path_quality,
                "likely_runner": likely_runner,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }

        if dead_premium_risk:
            reasons.append("dead_premium_risk")
            return {
                "quality_tag": "AVOID",
                "allow_trade": False,
                "watch_only": False,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": "NO_EXPANSION",
                "likely_runner": False,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }
        if pro_check["label"] == "PRO_REJECT":
            reasons.extend(pro_check["reasons"])
            return {
                "quality_tag": "AVOID",
                "allow_trade": False,
                "watch_only": False,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": "POOR_CONTRACT",
                "likely_runner": False,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }

        if not premium_confirmed and signal_type in breakout_family:
            clean_tactical = clean_tactical_context and entry_score >= 82 and score >= 78
            if option_sweep_starter:
                quality_tag = "PA_STRONG_ENTER_SMALL"
                allow_trade = True
                path_quality = "OPTION_SWEEP_PATH"
                likely_runner = True
                reasons.append("option_sweep_strong_small_size_entry")
            elif small_size_price_led_entry:
                quality_tag = "PA_STRONG_ENTER_SMALL"
                allow_trade = True
                path_quality = "PRICE_LED_PATH"
                likely_runner = bool(volume_supporting) or initiative_strength_score >= 36
                reasons.append("price_action_strong_small_size_entry")
            elif strong_price_action_watch and nifty_watch_allowed:
                quality_tag = "PA_STRONG_WAIT_PREMIUM"
                path_quality = "PRICE_LED_PATH"
                reasons.append("price_action_strong_waiting_for_premium")
            else:
                quality_tag = "TQ_CLEAN" if clean_tactical else "TQ_VOLATILE"
                path_quality = "TACTICAL_PATH"
            watch_only = not allow_trade
            reasons.append("premium_confirmation_pending")
            if allow_trade and pro_check["label"] != "PRO_PASS":
                allow_trade = False
                watch_only = pro_check["label"] == "PRO_WATCH"
                likely_runner = False
                reasons.extend(["pro_trader_check_watch_only", *pro_check["reasons"]])
            return {
                "quality_tag": quality_tag,
                "allow_trade": allow_trade,
                "watch_only": watch_only,
                "entry_phase": entry_phase,
                "premium_confirmed": premium_confirmed,
                "path_quality": path_quality,
                "likely_runner": likely_runner,
                "signal_family": signal_family,
                "session_map_phase": session_map_phase,
                "price_action_watch_ready": strong_price_action_watch,
                "initiative_strength_score": initiative_strength_score,
                "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
                "elasticity": elasticity,
                "pro_check": pro_check,
                "reasons": reasons,
            }

        if (
            entry_score >= 86
            and score >= 82
            and clean_tactical_context
            and entry_phase in {"FIRST_SIGNAL_IN_MOVE", "RETEST_SIGNAL"}
            and clean_breakout_premium
            and clean_spread
            and clean_trend_leg
            and (day_state_aligned or entry_score >= 90)
        ):
            quality_tag = "HQ"
            allow_trade = True
            path_quality = "CLEAN_PATH"
            likely_runner = premium_momentum_pct is not None and float(premium_momentum_pct) >= 2.5
        elif (
            entry_phase == "LATE_CHASE_SIGNAL"
            and entry_score >= 94
            and premium_momentum_pct is not None
            and float(premium_momentum_pct) >= 3.0
            and (spread_pct is None or float(spread_pct) <= 3.5)
        ):
            quality_tag = "LQ"
            allow_trade = True
            path_quality = "TACTICAL_PATH"
            likely_runner = True
        elif (
            signal_type in breakout_family
            and premium_confirmed
            and clean_tactical_context
            and entry_score >= 82
            and score >= 78
        ):
            quality_tag = "TQ_CLEAN" if clean_breakout_premium and clean_trend_leg else "TQ_VOLATILE"
            allow_trade = True
            path_quality = "CLEAN_PATH" if clean_breakout_premium and (signal_type in {"BREAKOUT_CONFIRM", "RETEST"} or day_state_aligned) else "TACTICAL_PATH"
            likely_runner = premium_momentum_pct is not None and float(premium_momentum_pct) >= 2.5
        elif entry_score >= 80 and score >= 78:
            clean_tactical = (
                pressure_conflict in {"NONE", "MILD"}
                and participation_hard_count == 0
                and participation_soft_count <= 1
                and clean_spread
                and entry_phase in {"FIRST_SIGNAL_IN_MOVE", "RETEST_SIGNAL"}
            )
            quality_tag = "TQ_CLEAN" if clean_tactical else "TQ_VOLATILE"
            allow_trade = True
            path_quality = "TACTICAL_PATH" if clean_tactical else "VOLATILE_PATH"
        else:
            reasons.append("expectancy_below_trade_floor")

        if quality_tag == "TQ_VOLATILE":
            allow_trade = False
            watch_only = True
            likely_runner = False
            if "volatile_tactical_watch_only" not in reasons:
                reasons.append("volatile_tactical_watch_only")
        if allow_trade and pro_check["label"] == "PRO_WATCH":
            allow_trade = False
            watch_only = True
            likely_runner = False
            if "pro_trader_check_watch_only" not in reasons:
                reasons.extend(["pro_trader_check_watch_only", *pro_check["reasons"]])

        return {
            "quality_tag": quality_tag,
            "allow_trade": allow_trade,
            "watch_only": watch_only,
            "entry_phase": entry_phase,
            "premium_confirmed": premium_confirmed,
            "path_quality": path_quality,
            "likely_runner": likely_runner,
            "signal_family": signal_family,
            "session_map_phase": session_map_phase,
            "price_action_watch_ready": strong_price_action_watch,
            "initiative_strength_score": initiative_strength_score,
            "futures_acceptance_score": float(futures_acceptance.get("score") or 0),
            "elasticity": elasticity,
            "pro_check": pro_check,
            "reasons": reasons,
        }

    @staticmethod
    def assess_raw_feed_health(service, candle_time):
        session_start = service._session_start_for(candle_time)
        candle_health = service.db_reader.fetch_intraday_candle_health(
            service.instrument,
            session_start=session_start,
            end_time=candle_time,
            timeframe="5m",
        )
        oi_health = service.db_reader.fetch_intraday_oi_health(
            service.instrument,
            session_start=session_start,
            end_time=candle_time,
        )
        label = "GOOD"
        reasons = []
        if candle_health["coverage_pct"] < 84 or oi_health["coverage_pct"] < 12:
            label = "REJECT"
            reasons.append("coverage_too_low")
        elif candle_health["coverage_pct"] < 92 or oi_health["coverage_pct"] < 90:
            label = "RISKY"
            reasons.append("coverage_soft")
        if candle_health["max_gap_seconds"] >= 901 or oi_health["max_gap_seconds"] >= 2101:
            label = "REJECT"
            reasons.append("large_gap_detected")
        elif candle_health["max_gap_seconds"] >= 601 or oi_health["max_gap_seconds"] >= 361:
            if label != "REJECT":
                label = "RISKY"
            reasons.append("gap_risk")
        if oi_health["non_good_rows"] > 0:
            if label == "GOOD":
                label = "RISKY"
            reasons.append("oi_quality_flagged")
        summary = (
            f"feed={label} | candle_cov={candle_health['coverage_pct']}% ({candle_health['count']}/{candle_health['expected_count']}) "
            f"| oi_cov={oi_health['coverage_pct']}% ({oi_health['distinct_minutes']}/{oi_health['expected_minutes']}) "
            f"| candle_gap={candle_health['max_gap_seconds']}s | oi_gap={oi_health['max_gap_seconds']}s"
        )
        result = {
            "label": label,
            "summary": summary,
            "reasons": reasons,
            "candle_health": candle_health,
            "oi_health": oi_health,
        }
        service.last_data_health = result
        return result

    @staticmethod
    def maybe_relax_reject_for_strong_setup(service, feed_health, signal=None):
        if not feed_health or (feed_health.get("label") or "").upper() != "REJECT":
            return feed_health
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"}:
            return feed_health

        candle_health = feed_health.get("candle_health") or {}
        oi_health = feed_health.get("oi_health") or {}
        score = float(getattr(service.strategy, "last_score", 0) or 0)
        entry_score = float(getattr(service.strategy, "last_entry_score", score) or score)
        confidence = (getattr(service.strategy, "last_confidence", "") or "").upper()
        signal_type = (getattr(service.strategy, "last_signal_type", "") or "").upper()
        day_state = (getattr(service.strategy, "last_active_day_state", "") or "").upper()
        day_state_direction = (getattr(service.strategy, "last_day_state_direction", "") or "").upper()
        pressure_conflict = (getattr(service.strategy, "last_pressure_conflict_level", "NONE") or "NONE").upper()
        futures_acceptance = getattr(service.strategy, "last_futures_acceptance", None) or {}
        initiative_strength_score = float(getattr(service.strategy, "last_initiative_strength_score", 0) or 0)
        strong_setup_types = {
            "BREAKOUT",
            "BREAKOUT_CONFIRM",
            "CONTINUATION",
            "AGGRESSIVE_CONTINUATION",
            "RETEST",
            "OPENING_DRIVE",
            "REVERSAL",
        }
        candle_cov = float(candle_health.get("coverage_pct") or 0)
        candle_gap = float(candle_health.get("max_gap_seconds") or 0)
        oi_cov = float(oi_health.get("coverage_pct") or 0)
        oi_gap = float(oi_health.get("max_gap_seconds") or 0)

        strong_alignment = (
            day_state in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
            and day_state_direction == signal
        )
        futures_acceptance_strong = bool(futures_acceptance.get("accepted")) and float(futures_acceptance.get("score") or 0) >= 58
        strong_sweep = bool(service._should_soften_option_sweep_filters(signal))
        setup_is_elite = (
            signal_type in strong_setup_types
            and confidence in {"MEDIUM", "HIGH"}
            and score >= 88
            and entry_score >= 88
            and pressure_conflict in {"NONE", "MILD"}
        )
        candle_side_healthy = candle_cov >= 90 and candle_gap <= 600
        oi_side_degraded_but_usable = oi_cov >= 12 and oi_gap <= 3600
        if not (setup_is_elite and candle_side_healthy and oi_side_degraded_but_usable):
            return feed_health
        if not (strong_alignment or strong_sweep or futures_acceptance_strong or initiative_strength_score >= 34):
            return feed_health

        relaxed_reasons = list(feed_health.get("reasons") or [])
        if "strong_setup_oi_softened" not in relaxed_reasons:
            relaxed_reasons.append("strong_setup_oi_softened")
        summary = (
            f"feed=RISKY_SOFTENED | candle_cov={candle_cov}% ({candle_health.get('count')}/{candle_health.get('expected_count')}) "
            f"| oi_cov={oi_cov}% ({oi_health.get('distinct_minutes')}/{oi_health.get('expected_minutes')}) "
            f"| candle_gap={candle_gap}s | oi_gap={oi_gap}s | elite_setup=yes | sponsorship="
            f"{'price_action' if (futures_acceptance_strong or initiative_strength_score >= 34) else 'alignment'}"
        )
        relaxed = dict(feed_health)
        relaxed["label"] = "RISKY"
        relaxed["reasons"] = relaxed_reasons
        relaxed["summary"] = summary
        service.last_data_health = relaxed
        return relaxed

    @staticmethod
    def derive_option_volume_signal(option_data):
        if not option_data:
            return None
        band_rows = option_data.get("band_snapshots") or []
        ce_band_volume = float(option_data.get("ce_volume_band") or 0)
        pe_band_volume = float(option_data.get("pe_volume_band") or 0)
        if band_rows and (ce_band_volume <= 0 or pe_band_volume <= 0):
            ce_band_volume = sum(float(row.get("volume") or 0) for row in band_rows if row.get("option_type") == "CE")
            pe_band_volume = sum(float(row.get("volume") or 0) for row in band_rows if row.get("option_type") == "PE")
        total_volume = ce_band_volume + pe_band_volume
        if total_volume <= 0:
            return None
        smaller_side = max(min(ce_band_volume, pe_band_volume), 1.0)
        dominant_ratio = max(ce_band_volume, pe_band_volume) / smaller_side
        atm_total = float(option_data.get("ce_volume") or 0) + float(option_data.get("pe_volume") or 0)
        if dominant_ratio >= 1.35 or atm_total >= total_volume * 0.18:
            return "STRONG"
        return "NORMAL"

    @staticmethod
    def should_soften_option_sweep_filters(service, signal):
        signal = (signal or "").upper()
        sweep_ctx = getattr(service, "option_sweep_context", None) or {}
        signal_type = (getattr(service.strategy, "last_signal_type", None) or "NONE").upper()
        score = float(getattr(service.strategy, "last_entry_score", 0) or getattr(service.strategy, "last_score", 0) or 0)
        return (
            signal in {"CE", "PE"}
            and sweep_ctx.get("direction") == signal
            and sweep_ctx.get("quality") == "STRONG"
            and sweep_ctx.get("micro_confirmed")
            and sweep_ctx.get("persistence_pairs", 0) >= 3
            and signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "CONTINUATION", "AGGRESSIVE_CONTINUATION", "RETEST", "OPENING_DRIVE"}
            and score >= 88
            and getattr(service.strategy, "last_pressure_conflict_level", "NONE") in {"NONE", "MILD"}
        )

    @staticmethod
    def evaluate_oi_wall_guard(service, signal, price, oi_ladder_data=None, pressure_metrics=None):
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"} or price is None:
            return None
        support = float((oi_ladder_data or {}).get("support") or 0) or None
        resistance = float((oi_ladder_data or {}).get("resistance") or 0) or None
        support_state = (oi_ladder_data or {}).get("support_wall_state")
        resistance_state = (oi_ladder_data or {}).get("resistance_wall_state")
        support_strength = float((oi_ladder_data or {}).get("support_strength") or 0)
        resistance_strength = float((oi_ladder_data or {}).get("resistance_strength") or 0)
        strike_gap = service.profile["strike_step"] or Config.STRIKE_STEP.get(service.instrument, 50)
        near_buffer = max(strike_gap * 0.35, 12)
        pressure_bias = (pressure_metrics or {}).get("pressure_bias")
        call_wall_ratio = float((pressure_metrics or {}).get("call_wall_strength_ratio") or 0)
        put_wall_ratio = float((pressure_metrics or {}).get("put_wall_strength_ratio") or 0)
        sweep_override = service._should_soften_option_sweep_filters(signal)
        if signal == "CE" and resistance is not None and price >= (resistance - near_buffer):
            if resistance_state not in {"WEAKENING"} and pressure_bias != "BULLISH":
                return {"label": "CALL_WALL_OVERHEAD", "reason": f"Price strong CE wall {int(resistance)} ke niche hai; clean break support nahi dikh raha.", "wall_level": resistance}
            if resistance_strength >= max(support_strength, 1.0) * 1.15 and call_wall_ratio >= max(put_wall_ratio, 1.0):
                if sweep_override:
                    return None
                return {"label": "CALL_WALL_HEAVY", "reason": f"Call wall {int(resistance)} abhi heavy hai; CE breakout premium choke ho sakta hai.", "wall_level": resistance}
        if signal == "PE" and support is not None and price <= (support + near_buffer):
            if support_state not in {"WEAKENING"} and pressure_bias != "BEARISH":
                return {"label": "PUT_WALL_SUPPORTING", "reason": f"Price strong PE wall {int(support)} ke paas hai; downside clean open nahi lag raha.", "wall_level": support}
            if support_strength >= max(resistance_strength, 1.0) * 1.15 and put_wall_ratio >= max(call_wall_ratio, 1.0):
                if sweep_override:
                    return None
                return {"label": "PUT_WALL_HEAVY", "reason": f"Put wall {int(support)} abhi heavy hai; PE breakdown premium sustain nahi ho sakta.", "wall_level": support}
        return None

    @staticmethod
    def evaluate_premium_quality_guard(service, signal, selected_option_contract, candle_time):
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"} or not selected_option_contract:
            return None
        ltp = float(selected_option_contract.get("ltp") or 0)
        spread_pct = service._spread_percent(selected_option_contract)
        volume_now = int(selected_option_contract.get("volume") or 0)
        iv_now = float(selected_option_contract.get("iv") or 0)
        if ltp <= 0:
            return {"label": "PREMIUM_MISSING", "reason": "Selected option ka live premium missing hai."}
        if spread_pct is not None and spread_pct >= 5.5:
            return {"label": "PREMIUM_SPREAD_WIDE", "reason": f"Selected option spread {spread_pct:.2f}% hai."}
        effective_candle_time = selected_option_contract.get("ts") or candle_time
        previous_snapshot = service.db_reader.fetch_option_contract_snapshot(
            instrument=service.instrument,
            strike=selected_option_contract.get("strike"),
            option_type=signal,
            before_ts=effective_candle_time - timedelta(minutes=2),
        )
        three_min_snapshot = service.db_reader.fetch_option_contract_snapshot(
            instrument=service.instrument,
            strike=selected_option_contract.get("strike"),
            option_type=signal,
            before_ts=effective_candle_time - timedelta(minutes=3),
        )
        previous_ltp = float(previous_snapshot.get("ltp") or 0) if previous_snapshot else 0.0
        three_min_ltp = float(three_min_snapshot.get("ltp") or 0) if three_min_snapshot else 0.0
        previous_volume = int(previous_snapshot.get("volume") or 0) if previous_snapshot else 0
        directional_participation = (
            (getattr(service.strategy, "last_participation_metrics", None) or {}).get(signal)
            or {}
        )
        same_side_weighted_delta = float(directional_participation.get("same_side_weighted_delta") or 0.0)
        opposite_side_weighted_delta = float(directional_participation.get("opposite_side_weighted_delta") or 0.0)
        same_side_breadth = int(directional_participation.get("same_side_breadth") or 0)
        opposite_side_breadth = int(directional_participation.get("opposite_side_breadth") or 0)
        same_side_volume_positive = volume_now > previous_volume if previous_snapshot else volume_now > 0
        participation_volume_supporting = (
            same_side_weighted_delta > max(opposite_side_weighted_delta * 0.9, 0.0)
            and same_side_breadth >= max(opposite_side_breadth, 1)
        )
        volume_supporting = same_side_volume_positive or participation_volume_supporting
        premium_momentum_pct = round(((ltp - previous_ltp) / previous_ltp) * 100.0, 2) if previous_ltp > 0 else None
        premium_momentum_3m_pct = round(((ltp - three_min_ltp) / three_min_ltp) * 100.0, 2) if three_min_ltp > 0 else None
        atm_row = service._get_option_contract_snapshot((service.option_data or {}).get("atm"), signal, before_ts=candle_time)
        atm_iv = float(atm_row.get("iv") or 0) if atm_row else 0.0
        iv_markup_pct = round(((iv_now - atm_iv) / atm_iv) * 100.0, 2) if atm_iv > 0 and iv_now > 0 else None
        if premium_momentum_pct is not None and premium_momentum_pct <= -2.0 and service.strategy.last_score < 88:
            return {"label": "PREMIUM_NOT_EXPANDING", "reason": f"Selected premium abhi expand nahi kar raha ({premium_momentum_pct:.2f}%).", "premium_momentum_pct": premium_momentum_pct}
        chase_limit_pct = float(getattr(Config, "PREMIUM_CHASE_MAX_2M_PCT", 14.0) or 14.0)
        if premium_momentum_pct is not None and premium_momentum_pct >= chase_limit_pct:
            return {
                "label": "PREMIUM_CHASED",
                "reason": (
                    f"Selected premium already {premium_momentum_pct:.2f}% move kar chuka hai "
                    f"last 2m me; fresh option entry chase ho jayegi."
                ),
                "premium_momentum_pct": premium_momentum_pct,
                "previous_ltp": previous_ltp if previous_ltp > 0 else None,
                "current_ltp": ltp,
            }
        chase_3m_limit_pct = float(getattr(Config, "PREMIUM_CHASE_MAX_3M_PCT", 18.0) or 18.0)
        if premium_momentum_3m_pct is not None and premium_momentum_3m_pct >= chase_3m_limit_pct:
            return {
                "label": "PREMIUM_CHASED_3M",
                "reason": (
                    f"Selected premium already {premium_momentum_3m_pct:.2f}% move kar chuka hai "
                    f"last 3m me; fresh entry ke liye pullback/retest chahiye."
                ),
                "premium_momentum_pct": premium_momentum_pct,
                "premium_momentum_3m_pct": premium_momentum_3m_pct,
                "previous_ltp": previous_ltp if previous_ltp > 0 else None,
                "three_min_ltp": three_min_ltp if three_min_ltp > 0 else None,
                "current_ltp": ltp,
            }
        if premium_momentum_pct is not None and premium_momentum_pct < 1.0 and volume_now <= previous_volume and service.strategy.last_regime in {"RANGING", "CHOPPY"}:
            if service._should_soften_option_sweep_filters(signal) and (spread_pct is None or spread_pct < 4.5):
                return {
                    "label": "PREMIUM_OK",
                    "reason": "Broad option sweep me premium sleepy check soften kiya gaya.",
                    "premium_momentum_pct": premium_momentum_pct,
                    "iv_markup_pct": iv_markup_pct,
                    "spread_pct": spread_pct,
                    "volume_supporting": volume_supporting,
                    "clean_breakout_premium": False,
                }
            return {"label": "PREMIUM_SLEEPY", "reason": "Premium response weak hai aur volume bhi expand nahi hua.", "premium_momentum_pct": premium_momentum_pct}
        if iv_markup_pct is not None and iv_markup_pct >= 18 and spread_pct is not None and spread_pct >= 4.0:
            return {"label": "IV_RICH_PREMIUM", "reason": f"Premium IV-rich hai ({iv_markup_pct:.1f}% ATM se upar) aur spread bhi wide hai.", "premium_momentum_pct": premium_momentum_pct}
        clean_breakout_premium = (
            premium_momentum_pct is not None
            and premium_momentum_pct >= 1.0
            and volume_supporting
            and (spread_pct is None or spread_pct <= 3.2)
        )
        return {
            "label": "PREMIUM_OK",
            "reason": "Premium expansion acceptable hai.",
            "premium_momentum_pct": premium_momentum_pct,
            "premium_momentum_3m_pct": premium_momentum_3m_pct,
            "iv_markup_pct": iv_markup_pct,
            "spread_pct": spread_pct,
            "volume_supporting": volume_supporting,
            "clean_breakout_premium": clean_breakout_premium,
            "previous_ltp": previous_ltp if previous_ltp > 0 else None,
            "current_ltp": ltp,
        }

    @staticmethod
    def compute_flip_score(service, direction, structure_break, vwap_break, latest_1m=None, previous_1m=None):
        direction = (direction or "").upper()
        latest_1m = latest_1m or {}
        previous_1m = previous_1m or {}
        score = 0.0
        reasons = []
        if structure_break:
            score += 30.0
            reasons.append("structure_break")
        if vwap_break:
            score += 20.0
            reasons.append("vwap_break")
        latest_close = latest_1m.get("close")
        previous_close = previous_1m.get("close")
        if latest_close is not None and previous_close is not None:
            if direction == "CE" and latest_close > previous_close:
                score += 15.0
                reasons.append("higher_close")
            elif direction == "PE" and latest_close < previous_close:
                score += 15.0
                reasons.append("lower_close")
        participation = getattr(service.strategy, "last_participation_metrics", None) or {}
        directional = participation.get(direction) or {}
        if directional.get("same_side_dominates"):
            score += 12.0
            reasons.append("same_side_delta")
        if directional.get("oi_supportive"):
            score += 8.0
            reasons.append("oi_support")
        if directional.get("spread_ok"):
            score += 8.0
            reasons.append("spread_ok")
        return {"score": round(min(score, 100.0), 2), "confidence": "HIGH" if score >= 70 else "MEDIUM" if score >= 55 else "LOW", "reasons": reasons}

    @staticmethod
    def classify_no_trade_zone(service, balanced_pro, signal=None, selected_option_contract=None):
        balanced_pro = balanced_pro or {}
        cautions = {str(item).lower() for item in (service.strategy.last_cautions or []) if item}
        time_regime = (balanced_pro.get("time_regime") or service.strategy.last_regime or "").upper()
        signal_type = (balanced_pro.get("setup") or service.strategy.last_signal_type or "").upper()
        active_day_state = (balanced_pro.get("active_day_state") or "").upper()
        day_state_direction = (balanced_pro.get("day_state_direction") or "").upper()
        score = float(service.strategy.last_entry_score or service.strategy.last_score or 0)
        confidence = (getattr(service.strategy, "last_confidence", "") or "").upper()
        spread_percent = service._spread_percent(selected_option_contract) if selected_option_contract else None
        data_health = getattr(service, "last_data_health", None) or {}
        feed_label = (data_health.get("label") or "").upper()
        market_regime = (service.strategy.last_regime or "").upper()
        strong_day_state_alignment = (
            signal in {"CE", "PE"}
            and active_day_state in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
            and day_state_direction == signal
            and confidence in {"MEDIUM", "HIGH"}
            and score >= 78
        )
        critical_noise_flags = {flag for flag in {"participation_weak", "opposite_pressure", "pressure_conflict", "higher_tf_not_aligned", "adx_not_confirmed"} if flag in cautions}
        hard_conflict_flags = {"participation_weak", "opposite_pressure", "pressure_conflict", "higher_tf_not_aligned"}
        has_hard_conflict = bool(critical_noise_flags.intersection(hard_conflict_flags))
        elite_breakout = signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "OPENING_DRIVE"} and confidence == "HIGH" and score >= 88
        clean_momentum_setup = signal_type in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION", "OPENING_DRIVE"} and confidence in {"MEDIUM", "HIGH"} and score >= 80 and not has_hard_conflict
        if feed_label == "RISKY" and score < 85 and not strong_day_state_alignment:
            return {"label": "FEED_RISKY_SKIP", "reason": "Raw feed clean nahi lag raha aur setup elite score ka nahi hai.", "action_text": "Is setup ko skip karo. Feed quality pehle clean honi chahiye."}
        if spread_percent is not None and spread_percent >= 5.5:
            return {"label": "WIDE_SPREAD_SKIP", "reason": f"Selected option spread {spread_percent:.2f}% hai, execution risky hai.", "action_text": "Is setup ko skip karo. Spread bahut wide hai."}
        if "expiry_day_mode" in cautions and time_regime in {"MIDDAY", "LATE_DAY", "ENDGAME"} and score < 74 and not clean_momentum_setup:
            return {"label": "EXPIRY_PREMIUM_CHAOS", "reason": "Expiry session me premium noise aur fast decay high hai.", "action_text": "Fresh entry avoid karo. Expiry premium abhi noisy hai."}
        if time_regime in {"LATE_DAY", "ENDGAME"} and signal_type in {"REVERSAL", "BREAKOUT_CONFIRM", "TRAP_REVERSAL"}:
            conflict = getattr(service.strategy, "last_pressure_conflict_level", "NONE")
            if conflict in {"MODERATE", "HIGH"} and not (clean_momentum_setup and score >= 78):
                return {"label": "LATE_DAY_WHIPSAW", "reason": "Late-day reversal zone me pressure conflict present hai.", "action_text": "Skip ya wait karo. Late-day whipsaw risk high hai."}
        if time_regime == "MIDDAY" and market_regime in {"RANGING", "CHOPPY"} and score < 76 and not (clean_momentum_setup and confidence in {"MEDIUM", "HIGH"}):
            return {"label": "MIDDAY_RANGE_SKIP", "reason": "Midday ranging/choppy regime me premium clean explode karna mushkil hota hai.", "action_text": "Watch-only raho. Midday me cleaner expansion ka wait karo."}
        if (len(critical_noise_flags) >= 2 or (has_hard_conflict and "adx_not_confirmed" in critical_noise_flags) or (has_hard_conflict and score < 84)) and not elite_breakout and not strong_day_state_alignment:
            return {"label": "HIGH_NOISE_SKIP", "reason": "Price aur option participation clean align nahi kar rahe.", "action_text": "Fresh add mat karo. Setup abhi high-noise zone me hai."}
        return None

    @staticmethod
    def optimized_spread_short_strike(service, signal, long_strike, balanced_pro=None, risk_profile=None):
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"} or long_strike is None:
            return None, None
        balanced_pro = balanced_pro or {}
        risk_profile = risk_profile or {}
        strike_step = service.profile["strike_step"] or Config.STRIKE_STEP.get(service.instrument, 50)
        cautions = {str(item).lower() for item in (service.strategy.last_cautions or []) if item}
        expiry_mode = "expiry_day_mode" in cautions or "expiry_fast_decay" in cautions
        late_session = (balanced_pro.get("time_regime") or "").upper() in {"LATE_DAY", "ENDGAME"}
        target_pct = float(risk_profile.get("target_pct") or 0)
        try:
            first_target_price = float((service.strategy.last_entry_plan or {}).get("first_target_price"))
        except Exception:
            first_target_price = None
        try:
            reference_price = float((service.strategy.last_entry_plan or {}).get("entry_above") or (service.strategy.last_entry_plan or {}).get("entry_below"))
        except Exception:
            reference_price = None
        if first_target_price is not None and reference_price is not None:
            target_move_points = abs(first_target_price - reference_price)
        else:
            target_move_points = strike_step * (1 if target_pct <= 20 else 2 if target_pct <= 30 else 3)
        width_steps = max(1, min(3, round(target_move_points / max(strike_step, 1))))
        if expiry_mode or late_session:
            width_steps = max(1, min(width_steps, 2))
        if target_pct <= 20:
            width_steps = 1
        elif target_pct >= 30 and not expiry_mode:
            width_steps = max(width_steps, 2)
        short_strike = int(long_strike) + (strike_step * width_steps) if signal == "CE" else int(long_strike) - (strike_step * width_steps)
        return int(short_strike), width_steps

    @staticmethod
    def build_option_structure_suggestion(service, signal, selected_strike, selected_option_contract, balanced_pro=None, risk_profile=None):
        signal = (signal or "").upper()
        if signal not in {"CE", "PE"} or selected_strike is None:
            return None
        balanced_pro = balanced_pro or {}
        risk_profile = risk_profile or {}
        cautions = {str(item).lower() for item in (service.strategy.last_cautions or []) if item}
        strike_step = service.profile["strike_step"] or Config.STRIKE_STEP.get(service.instrument, 50)
        spread_percent = service._spread_percent(selected_option_contract) if selected_option_contract else None
        iv_rich = False
        if selected_option_contract and service.option_data and service.option_data.get("atm") is not None:
            atm_row = service._get_option_contract_snapshot(service.option_data.get("atm"), signal)
            if atm_row and atm_row.get("iv") and selected_option_contract.get("iv"):
                atm_iv = float(atm_row.get("iv") or 0)
                selected_iv = float(selected_option_contract.get("iv") or 0)
                iv_rich = atm_iv > 0 and selected_iv > (atm_iv * 1.12)
        expiry_mode = "expiry_day_mode" in cautions or "expiry_fast_decay" in cautions
        late_session = (balanced_pro.get("time_regime") or "").upper() in {"LATE_DAY", "ENDGAME"}
        moderate_target = float(risk_profile.get("target_pct") or 0) <= 25.0
        wide_defined_move = float(risk_profile.get("target_pct") or 0) >= 30.0
        noisy_premium = expiry_mode or late_session or iv_rich or (spread_percent is not None and spread_percent >= 3.5) or "participation_spread_wide" in cautions
        if not (noisy_premium or moderate_target or wide_defined_move):
            return None
        short_strike, width_steps = service._optimized_spread_short_strike(signal=signal, long_strike=selected_strike, balanced_pro=balanced_pro, risk_profile=risk_profile)
        if short_strike is None:
            return None
        structure_type = "BULL_CALL_SPREAD" if signal == "CE" else "BEAR_PUT_SPREAD"
        rationale = []
        if expiry_mode:
            rationale.append("expiry theta high hai")
        if late_session:
            rationale.append("late-day premium unstable ho sakta hai")
        if spread_percent is not None and spread_percent >= 3.5:
            rationale.append("spread wide hai")
        if iv_rich:
            rationale.append("premium IV-rich lag raha hai")
        if moderate_target:
            rationale.append("move expectation moderate hai")
        if wide_defined_move:
            rationale.append("planned move bada hai, defined upside bucket useful ho sakta hai")
        rationale.append(f"spread width {width_steps} strike-step rakha gaya")
        rationale_text = ", ".join(rationale) if rationale else "defined-risk structure zyada suitable lag raha hai"
        action_text = f"Plain {signal} buy ke bajay {structure_type.replace('_', ' ')} socho: buy {selected_strike}, sell {short_strike}. {rationale_text}."
        return {"type": structure_type, "long_strike": int(selected_strike), "short_strike": int(short_strike), "width_steps": int(width_steps), "action_text": action_text, "rationale": rationale_text}

    @staticmethod
    def spread_percent(option_row):
        ltp = option_row.get("ltp") if option_row else None
        spread = option_row.get("spread") if option_row else None
        if not ltp or spread is None:
            return None
        try:
            return round((float(spread) / float(ltp)) * 100, 4)
        except Exception:
            return None

    @staticmethod
    def score_option_candidate(service, row, direction, preferred_strike, underlying_price):
        spread_percent = service._spread_percent(row) or 999.0
        bid_qty = int(row.get("top_bid_quantity") or 0)
        ask_qty = int(row.get("top_ask_quantity") or 0)
        volume = int(row.get("volume") or 0)
        previous_volume = int(row.get("previous_volume") or 0) if row.get("previous_volume") is not None else None
        volume_delta = max(volume - previous_volume, 0) if previous_volume is not None else 0
        oi = int(row.get("oi") or 0)
        previous_oi = int(row.get("previous_oi") or 0) if row.get("previous_oi") is not None else None
        oi_delta = oi - previous_oi if previous_oi is not None else 0
        delta_abs = abs(float(row.get("delta") or 0))
        theta_abs = abs(float(row.get("theta") or 0))
        distance = abs(int(row.get("strike") or 0) - int(preferred_strike or row.get("strike") or 0))
        strike_gap = service.profile["strike_step"] or Config.STRIKE_STEP.get(service.instrument, 50)
        distance_from_atm = int(row.get("distance_from_atm") or 0)
        target_delta = 0.5 if service.strategy.last_score >= 75 else 0.62
        spread_score = max(0.0, 30.0 - min(spread_percent, 10.0) * 4.0)
        depth_score = min(15.0, min(bid_qty, ask_qty) / 20.0)
        volume_score = min(18.0, volume / 300.0)
        leader_volume_score = min(
            14.0,
            volume_delta / max(float(getattr(Config, "PREMIUM_LEADER_VOLUME_DELTA_DIVISOR", 50000.0) or 50000.0), 1.0),
        )
        oi_support_score = 4.0 if oi_delta >= 0 else -2.0
        atm_distance_score = max(0.0, 10.0 - abs(distance_from_atm) * 2.5)
        oi_score = min(10.0, oi / 20000.0)
        delta_score = max(0.0, 15.0 * (1.0 - min(abs(delta_abs - target_delta) / 0.45, 1.0)))
        proximity_score = max(0.0, 12.0 - (distance / max(strike_gap, 1)) * 4.0)
        target_price = (service.strategy.last_entry_plan or {}).get("first_target_price")
        target_move = abs(float(target_price) - float(underlying_price)) if target_price is not None and underlying_price is not None else float(strike_gap)
        expected_move = delta_abs * target_move
        theta_penalty = theta_abs * 0.25
        expected_edge = round(expected_move - float(row.get("spread") or 0) - theta_penalty, 2)
        edge_score = max(0.0, min(20.0, expected_edge))
        atm_row = None
        atm = (service.option_data or {}).get("atm")
        if atm is not None:
            atm_row = service._get_option_contract_snapshot(atm, direction)
        iv_penalty = 0.0
        if atm_row and atm_row.get("iv") and row.get("iv"):
            atm_iv = float(atm_row["iv"])
            if atm_iv > 0:
                iv_markup = (float(row["iv"]) - atm_iv) / atm_iv
                if iv_markup > 0.12:
                    iv_penalty = min(8.0, iv_markup * 20.0)
        contract_efficiency_score = round(
            max(0.0, spread_score + depth_score + delta_score + max(0.0, 10.0 - theta_penalty) - iv_penalty),
            2,
        )
        leader_score = round(leader_volume_score + oi_support_score + atm_distance_score + max(0.0, 8.0 - min(spread_percent, 8.0)), 2)
        candidate_score = round(
            spread_score
            + depth_score
            + volume_score
            + leader_volume_score
            + oi_score
            + oi_support_score
            + atm_distance_score
            + delta_score
            + proximity_score
            + edge_score
            - iv_penalty,
            2,
        )
        reason_parts = [
            f"spread={float(row.get('spread') or 0):.2f} ({spread_percent:.2f}%)",
            f"delta={delta_abs:.2f}",
            f"vol={volume}",
            f"vol_delta={volume_delta}",
            f"oi={oi}",
            f"oi_delta={oi_delta}",
            f"edge={expected_edge:.2f}",
            f"leader={leader_score:.1f}",
        ]
        if iv_penalty > 0:
            reason_parts.append("iv_rich")
        reason_parts.append(f"contract_eff={contract_efficiency_score:.1f}")
        return {
            **dict(row),
            "candidate_direction": direction,
            "candidate_score": candidate_score,
            "expected_edge": expected_edge,
            "spread_percent": spread_percent,
            "contract_efficiency_score": contract_efficiency_score,
            "leader_score": leader_score,
            "volume_delta": volume_delta,
            "oi_delta": oi_delta,
            "reason": " | ".join(reason_parts),
        }

    @staticmethod
    def build_option_candidates(service, underlying_price, preferred_strikes=None, signal_direction=None, balanced_pro=None):
        if not service.option_data:
            return []
        preferred_strikes = preferred_strikes or {}
        band_rows = service.option_data.get("band_snapshots") or []
        candidates = []
        for direction in ("CE", "PE"):
            if signal_direction and direction != signal_direction:
                continue
            preferred_strike = preferred_strikes.get(direction)
            direction_rows = [row for row in band_rows if row.get("option_type") == direction and abs(int(row.get("distance_from_atm") or 99)) <= 3]
            scored = [service._score_option_candidate(row, direction, preferred_strike, underlying_price) for row in direction_rows]
            scored.sort(key=lambda item: (item["candidate_score"], -abs(int(item.get("distance_from_atm") or 0))), reverse=True)
            for rank, item in enumerate(scored[:3], start=1):
                candidates.append({**item, "candidate_rank": rank, "underlying_bias": (balanced_pro or {}).get("bias"), "setup_type": (balanced_pro or {}).get("setup")})
        return candidates
