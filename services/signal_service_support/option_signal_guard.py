"""Option guardrails and candidate helpers for SignalService."""

from datetime import timedelta

from config import Config


class OptionSignalGuard:
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
        if candle_health["coverage_pct"] < 84 or oi_health["coverage_pct"] < 78:
            label = "REJECT"
            reasons.append("coverage_too_low")
        elif candle_health["coverage_pct"] < 92 or oi_health["coverage_pct"] < 90:
            label = "RISKY"
            reasons.append("coverage_soft")
        if candle_health["max_gap_seconds"] >= 901 or oi_health["max_gap_seconds"] >= 721:
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
        previous_snapshot = service.db_reader.fetch_option_contract_snapshot(
            instrument=service.instrument,
            strike=selected_option_contract.get("strike"),
            option_type=signal,
            before_ts=candle_time - timedelta(minutes=2),
        )
        previous_ltp = float(previous_snapshot.get("ltp") or 0) if previous_snapshot else 0.0
        previous_volume = int(previous_snapshot.get("volume") or 0) if previous_snapshot else 0
        premium_momentum_pct = round(((ltp - previous_ltp) / previous_ltp) * 100.0, 2) if previous_ltp > 0 else None
        atm_row = service._get_option_contract_snapshot((service.option_data or {}).get("atm"), signal, before_ts=candle_time)
        atm_iv = float(atm_row.get("iv") or 0) if atm_row else 0.0
        iv_markup_pct = round(((iv_now - atm_iv) / atm_iv) * 100.0, 2) if atm_iv > 0 and iv_now > 0 else None
        if premium_momentum_pct is not None and premium_momentum_pct <= -2.0 and service.strategy.last_score < 88:
            return {"label": "PREMIUM_NOT_EXPANDING", "reason": f"Selected premium abhi expand nahi kar raha ({premium_momentum_pct:.2f}%).", "premium_momentum_pct": premium_momentum_pct}
        if premium_momentum_pct is not None and premium_momentum_pct < 1.0 and volume_now <= previous_volume and service.strategy.last_regime in {"RANGING", "CHOPPY"}:
            if service._should_soften_option_sweep_filters(signal) and (spread_pct is None or spread_pct < 4.5):
                return {"label": "PREMIUM_OK", "reason": "Broad option sweep me premium sleepy check soften kiya gaya.", "premium_momentum_pct": premium_momentum_pct, "iv_markup_pct": iv_markup_pct, "spread_pct": spread_pct}
            return {"label": "PREMIUM_SLEEPY", "reason": "Premium response weak hai aur volume bhi expand nahi hua.", "premium_momentum_pct": premium_momentum_pct}
        if iv_markup_pct is not None and iv_markup_pct >= 18 and spread_pct is not None and spread_pct >= 4.0:
            return {"label": "IV_RICH_PREMIUM", "reason": f"Premium IV-rich hai ({iv_markup_pct:.1f}% ATM se upar) aur spread bhi wide hai.", "premium_momentum_pct": premium_momentum_pct}
        return {"label": "PREMIUM_OK", "reason": "Premium expansion acceptable hai.", "premium_momentum_pct": premium_momentum_pct, "iv_markup_pct": iv_markup_pct, "spread_pct": spread_pct}

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
        oi = int(row.get("oi") or 0)
        delta_abs = abs(float(row.get("delta") or 0))
        theta_abs = abs(float(row.get("theta") or 0))
        distance = abs(int(row.get("strike") or 0) - int(preferred_strike or row.get("strike") or 0))
        strike_gap = service.profile["strike_step"] or Config.STRIKE_STEP.get(service.instrument, 50)
        target_delta = 0.5 if service.strategy.last_score >= 75 else 0.62
        spread_score = max(0.0, 30.0 - min(spread_percent, 10.0) * 4.0)
        depth_score = min(15.0, min(bid_qty, ask_qty) / 20.0)
        volume_score = min(18.0, volume / 300.0)
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
        candidate_score = round(spread_score + depth_score + volume_score + oi_score + delta_score + proximity_score + edge_score - iv_penalty, 2)
        reason_parts = [f"spread={float(row.get('spread') or 0):.2f} ({spread_percent:.2f}%)", f"delta={delta_abs:.2f}", f"vol={volume}", f"oi={oi}", f"edge={expected_edge:.2f}"]
        if iv_penalty > 0:
            reason_parts.append("iv_rich")
        return {**dict(row), "candidate_direction": direction, "candidate_score": candidate_score, "expected_edge": expected_edge, "spread_percent": spread_percent, "reason": " | ".join(reason_parts)}

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
