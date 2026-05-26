"""Trade-monitor support helpers extracted from SignalService."""

from config import Config


class TradeMonitorSupport:
    @staticmethod
    def option_three_bar_momentum(service, recent_1m_candles, strike, signal):
        if not recent_1m_candles or strike is None or signal not in {"CE", "PE"}:
            return None
        sampled = recent_1m_candles[-3:]
        prices = []
        for candle in sampled:
            snap = service._get_option_contract_snapshot(strike, signal, before_ts=candle.get("time"))
            if snap and snap.get("ltp") is not None:
                prices.append(float(snap.get("ltp")))
        if len(prices) < 3:
            return None
        if prices[2] > prices[1] > prices[0]:
            return "EXPANDING"
        if prices[2] < prices[1] < prices[0]:
            return "FAILING"
        return "STALLING"

    @staticmethod
    def infer_monitor_flip_signal(service, current_signal, latest_1m, previous_1m, micro_high, micro_low, vwap_break, structure_break):
        current_signal = (current_signal or "").upper()
        latest_close = latest_1m.get("close")
        previous_close = previous_1m.get("close")
        if latest_close is None or previous_close is None or not vwap_break:
            return None
        flip_side = "PE" if current_signal == "CE" else "CE"
        flip_score = service._compute_flip_score(
            direction=flip_side,
            structure_break=structure_break,
            vwap_break=vwap_break,
            latest_1m=latest_1m,
            previous_1m=previous_1m,
        )
        if current_signal == "CE":
            if latest_close <= micro_low and latest_close < previous_close:
                return {
                    "label": service._flip_confirmed_label("PE") if flip_score["score"] >= 65 else "THESIS_FAILED_EXIT",
                    "action_text": "CE thesis fail ho gayi. PE side ab confirmed lag rahi hai.",
                    "flip_score": flip_score,
                }
        elif current_signal == "PE":
            if latest_close >= micro_high and latest_close > previous_close:
                return {
                    "label": service._flip_confirmed_label("CE") if flip_score["score"] >= 65 else "THESIS_FAILED_EXIT",
                    "action_text": "PE thesis fail ho gayi. CE side ab confirmed lag rahi hai.",
                    "flip_score": flip_score,
                }
        return None

    @staticmethod
    def resolve_trade_risk_profile(service, setup_type=None, quality=None, confidence=None, cautions=None):
        setup = (setup_type or "GENERAL").upper()
        quality = (quality or "").upper()
        confidence = (confidence or "").upper()
        cautions = {str(item).lower() for item in (cautions or []) if item}
        breakout_bucket = {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "OPENING_DRIVE"}
        reversal_bucket = {"REVERSAL", "TRAP_REVERSAL"}
        continuation_bucket = {"CONTINUATION", "AGGRESSIVE_CONTINUATION"}
        if setup in reversal_bucket:
            setup_bucket = "REVERSAL"
        elif setup in breakout_bucket:
            setup_bucket = "BREAKOUT"
        elif setup in continuation_bucket:
            setup_bucket = "CONTINUATION"
        else:
            setup_bucket = "BREAKOUT"

        session_bucket = TradeMonitorSupport.resolve_session_bucket(cautions)
        iv_bucket = TradeMonitorSupport.resolve_iv_bucket(
            getattr(service, "_current_risk_option_contract", None),
            getattr(service, "_current_risk_reference_contract", None),
        )
        market_iv_regime = TradeMonitorSupport.resolve_market_iv_regime(
            service,
            reference_option_contract=getattr(service, "_current_risk_reference_contract", None),
            option_type=((getattr(service, "_current_risk_option_contract", None) or {}).get("option_type")),
            before_ts=((getattr(service, "_current_risk_reference_contract", None) or {}).get("ts")),
        )
        matrix = service.RISK_PROFILE_MATRIX.get(service.instrument, service.RISK_PROFILE_MATRIX["NIFTY"])
        profile = (
            matrix.get(session_bucket, matrix["NON_EXPIRY"]).get(setup_bucket)
            or matrix["NON_EXPIRY"]["BREAKOUT"]
        ).get(iv_bucket)
        if not profile:
            profile = matrix["NON_EXPIRY"]["BREAKOUT"]["NORMAL"]

        hard_stop = float(profile["sl"])
        target_pct = float(profile["target"])
        trail_pct = float(profile["trail"])
        expiry_mode = session_bucket == "EXPIRY"
        expiry_fast_decay = "expiry_fast_decay" in cautions or expiry_mode
        time_warn, time_exit = TradeMonitorSupport.resolve_time_stop_minutes(
            session_bucket=session_bucket,
            setup_bucket=setup_bucket,
            quality=quality,
            confidence=confidence,
            iv_bucket=iv_bucket,
        )
        risk_note = (
            f"{service.instrument} {session_bucket.lower()} {setup_bucket.lower()} profile with "
            f"{iv_bucket.lower()} strike IV and {market_iv_regime.lower()} market IV regime. "
            "Underlying invalidation stays primary; premium % is the hard cap."
        )
        if quality == "C" or confidence == "LOW":
            time_exit = min(time_exit, 4)
        if expiry_fast_decay:
            trail_pct = min(trail_pct, 8.0)

        hard_stop, target_pct, trail_pct, time_warn, time_exit = TradeMonitorSupport.apply_market_iv_regime_adjustments(
            hard_stop, target_pct, trail_pct, time_warn, time_exit, market_iv_regime, session_bucket
        )
        profit_lock_trigger_pct = max(8.0, round(target_pct * 0.6, 2))
        if setup_bucket == "BREAKOUT":
            profit_lock_trigger_pct = min(profit_lock_trigger_pct, 12.0 if service.instrument == "NIFTY" else 14.0)
        elif setup_bucket == "CONTINUATION":
            profit_lock_trigger_pct = min(max(profit_lock_trigger_pct, 10.0), 14.0)
        elif setup_bucket == "REVERSAL":
            profit_lock_trigger_pct = min(max(profit_lock_trigger_pct, 10.0), 15.0)
        if expiry_mode:
            profit_lock_trigger_pct = max(6.0, profit_lock_trigger_pct - 2.0)

        runner_profile = TradeMonitorSupport.runner_profile_for_setup(setup_bucket, session_bucket)
        if service.instrument == "SENSEX" and session_bucket == "EXPIRY":
            runner_profile["partial_trigger_pct"] = min(runner_profile["partial_trigger_pct"], 16.0)
            runner_profile["runner_trigger_pct"] = min(runner_profile["runner_trigger_pct"], 26.0)
            runner_profile["runner_trail_bonus"] = max(runner_profile["runner_trail_bonus"], 1.0)
            runner_profile["time_extension_minutes"] = min(int(runner_profile["time_extension_minutes"] or 0), 2)
            risk_note += " SENSEX expiry spike mode: partial jaldi book karo; runner sirf momentum/pressure support ke saath."
        return {
            "setup_bucket": setup_bucket,
            "hard_premium_stop_pct": hard_stop,
            "target_pct": target_pct,
            "trail_from_peak_pct": trail_pct,
            "profit_lock_trigger_pct": round(profit_lock_trigger_pct, 2),
            "time_stop_warn_minutes": time_warn,
            "time_stop_exit_minutes": time_exit,
            "risk_note": risk_note,
            "primary_stop_source": "UNDERLYING_INVALIDATION",
            "expiry_fast_decay": bool(expiry_fast_decay),
            "session_bucket": session_bucket,
            "iv_bucket": iv_bucket,
            "market_iv_regime": market_iv_regime,
            "partial_trigger_pct": round(runner_profile["partial_trigger_pct"], 2),
            "runner_trigger_pct": round(runner_profile["runner_trigger_pct"], 2),
            "runner_trail_bonus": round(runner_profile["runner_trail_bonus"], 2),
            "allow_endgame_runner": bool(runner_profile["allow_endgame_runner"]),
            "time_extension_minutes": int(runner_profile["time_extension_minutes"]),
        }

    @staticmethod
    def resolve_session_bucket(cautions):
        cautions = {str(item).lower() for item in (cautions or []) if item}
        if "expiry_day_mode" in cautions:
            return "EXPIRY"
        if "pre_expiry_positioning_mode" in cautions:
            return "PRE_EXPIRY"
        if "post_expiry_rebuild_mode" in cautions:
            return "POST_EXPIRY"
        return "NON_EXPIRY"

    @staticmethod
    def resolve_iv_bucket(selected_option_contract=None, reference_option_contract=None):
        try:
            selected_iv = float((selected_option_contract or {}).get("iv") or 0)
            reference_iv = float((reference_option_contract or {}).get("iv") or 0)
        except Exception:
            return "NORMAL"
        if selected_iv <= 0 or reference_iv <= 0:
            return "NORMAL"
        iv_markup_pct = ((selected_iv - reference_iv) / reference_iv) * 100.0
        if iv_markup_pct <= -8.0:
            return "CHEAP"
        if iv_markup_pct >= 18.0:
            return "EXTREME"
        if iv_markup_pct >= 8.0:
            return "RICH"
        return "NORMAL"

    @staticmethod
    def resolve_time_stop_minutes(session_bucket, setup_bucket, quality, confidence, iv_bucket):
        if session_bucket == "EXPIRY":
            time_warn, time_exit = 2, 4
        elif session_bucket in {"PRE_EXPIRY", "POST_EXPIRY"}:
            time_warn, time_exit = 3, 5
        else:
            time_warn, time_exit = 3, 5
        if setup_bucket == "CONTINUATION":
            time_exit = min(time_exit, 4)
        elif setup_bucket == "REVERSAL" and session_bucket != "EXPIRY":
            time_exit = max(time_exit, 5)
        if iv_bucket == "EXTREME":
            time_warn = max(2, time_warn - 1)
            time_exit = max(3, time_exit - 1)
        elif iv_bucket == "CHEAP" and session_bucket != "EXPIRY":
            time_exit += 1
        if quality == "C" or confidence == "LOW":
            time_exit = min(time_exit, 4)
        return time_warn, time_exit

    @staticmethod
    def resolve_market_iv_regime(service, reference_option_contract=None, option_type=None, before_ts=None):
        option_type = option_type or ((reference_option_contract or {}).get("option_type")) or "CE"
        try:
            current_atm_iv = float((reference_option_contract or {}).get("iv") or 0)
        except Exception:
            current_atm_iv = 0.0
        if current_atm_iv <= 0:
            service._current_market_iv_context = {"current_atm_iv": None, "baseline_atm_iv": None, "iv_deviation_pct": None}
            service._current_market_iv_regime = "NORMAL"
            return "NORMAL"
        try:
            history = service.db_reader.fetch_recent_atm_iv_series(
                instrument=service.instrument,
                option_type=option_type,
                before_ts=before_ts,
                limit=20,
            )
        except Exception:
            history = []
        iv_values = [float(item["iv"]) for item in history if item.get("iv")]
        if iv_values and abs(iv_values[0] - current_atm_iv) < 1e-9:
            iv_values = iv_values[1:]
        if not iv_values:
            baseline_atm_iv = current_atm_iv
            deviation_pct = 0.0
            regime = "NORMAL"
        else:
            sample = iv_values[: min(len(iv_values), 12)]
            baseline_atm_iv = sum(sample) / len(sample)
            deviation_pct = ((current_atm_iv - baseline_atm_iv) / baseline_atm_iv) * 100.0 if baseline_atm_iv > 0 else 0.0
            if deviation_pct >= 20.0:
                regime = "EVENT"
            elif deviation_pct >= 10.0:
                regime = "HIGH"
            elif deviation_pct <= -10.0:
                regime = "LOW"
            else:
                regime = "NORMAL"
        service._current_market_iv_context = {
            "current_atm_iv": round(current_atm_iv, 2),
            "baseline_atm_iv": round(baseline_atm_iv, 2) if baseline_atm_iv else None,
            "iv_deviation_pct": round(deviation_pct, 2),
        }
        service._current_market_iv_regime = regime
        return regime

    @staticmethod
    def apply_market_iv_regime_adjustments(hard_stop, target_pct, trail_pct, time_warn, time_exit, market_iv_regime, session_bucket):
        hard_stop = float(hard_stop)
        target_pct = float(target_pct)
        trail_pct = float(trail_pct)
        if market_iv_regime == "LOW":
            hard_stop += 1.0
            target_pct += 3.0 if session_bucket != "EXPIRY" else 2.0
        elif market_iv_regime == "HIGH":
            hard_stop = max(5.0, hard_stop - 1.0)
            target_pct = max(hard_stop + 2.0, target_pct - 3.0)
            trail_pct = max(7.0, trail_pct - 0.5)
        elif market_iv_regime == "EVENT":
            hard_stop = max(4.0, hard_stop - 2.0)
            target_pct = max(hard_stop + 2.0, target_pct - 6.0)
            trail_pct = max(7.0, trail_pct - 1.0)
            time_warn = max(1, time_warn - 1)
            time_exit = max(3, time_exit - 1)
        return round(hard_stop, 2), round(target_pct, 2), round(trail_pct, 2), int(time_warn), int(time_exit)

    @staticmethod
    def runner_profile_for_setup(setup_bucket, session_bucket):
        setup_bucket = (setup_bucket or "BREAKOUT").upper()
        session_bucket = (session_bucket or "NON_EXPIRY").upper()
        profile = {"partial_trigger_pct": 20.0, "runner_trigger_pct": 28.0, "runner_trail_bonus": 1.5, "allow_endgame_runner": False, "time_extension_minutes": 0}
        if setup_bucket == "BREAKOUT":
            profile.update({"partial_trigger_pct": 24.0, "runner_trigger_pct": 32.0, "runner_trail_bonus": 2.0, "allow_endgame_runner": True, "time_extension_minutes": 3})
        elif setup_bucket == "CONTINUATION":
            profile.update({"partial_trigger_pct": 22.0, "runner_trigger_pct": 30.0, "runner_trail_bonus": 2.5, "allow_endgame_runner": True, "time_extension_minutes": 2})
        elif setup_bucket == "REVERSAL":
            profile.update({"partial_trigger_pct": 18.0, "runner_trigger_pct": 26.0, "runner_trail_bonus": 1.0, "allow_endgame_runner": False, "time_extension_minutes": 0})
        if session_bucket == "EXPIRY":
            profile["partial_trigger_pct"] = max(14.0, profile["partial_trigger_pct"] - 4.0)
            profile["runner_trigger_pct"] = max(20.0, profile["runner_trigger_pct"] - 4.0)
            profile["runner_trail_bonus"] = max(0.5, profile["runner_trail_bonus"] - 0.5)
        return profile

    @staticmethod
    def option_expansion_metrics(entry_option_price, option_price, entry_underlying_price, underlying_price, entry_delta=None):
        if entry_option_price in (None, 0) or option_price is None:
            return {"underlying_move": None, "actual_option_move": None, "expected_option_move": None, "expansion_ratio": None, "premium_supportive": False}
        try:
            actual_option_move = float(option_price) - float(entry_option_price)
            underlying_move = abs(float(underlying_price) - float(entry_underlying_price)) if underlying_price is not None and entry_underlying_price is not None else None
            delta_abs = abs(float(entry_delta or 0))
            expected_option_move = None if underlying_move is None else max(delta_abs * underlying_move, 0.0)
            if expected_option_move in (None, 0):
                expansion_ratio = None
                premium_supportive = actual_option_move >= (float(entry_option_price) * 0.04)
            else:
                expansion_ratio = round(actual_option_move / expected_option_move, 2)
                premium_supportive = expansion_ratio >= 0.75
            return {
                "underlying_move": round(underlying_move, 2) if underlying_move is not None else None,
                "actual_option_move": round(actual_option_move, 2),
                "expected_option_move": round(expected_option_move, 2) if expected_option_move is not None else None,
                "expansion_ratio": expansion_ratio,
                "premium_supportive": premium_supportive,
            }
        except Exception:
            return {"underlying_move": None, "actual_option_move": None, "expected_option_move": None, "expansion_ratio": None, "premium_supportive": False}

    @staticmethod
    def classify_trade_run_profile(pnl_percent, expansion_metrics, minutes_active, momentum_strong, pressure_flip_exit, drawdown_from_peak_pct, setup_bucket, session_bucket):
        pnl_percent = float(pnl_percent or 0.0)
        expansion_metrics = expansion_metrics or {}
        expansion_ratio = expansion_metrics.get("expansion_ratio")
        premium_supportive = bool(expansion_metrics.get("premium_supportive"))
        setup_bucket = (setup_bucket or "BREAKOUT").upper()
        session_bucket = (session_bucket or "NON_EXPIRY").upper()
        if pressure_flip_exit:
            return "FAILED"
        if (
            pnl_percent >= (24.0 if session_bucket == "EXPIRY" else 28.0)
            and premium_supportive
            and momentum_strong
            and (drawdown_from_peak_pct is None or drawdown_from_peak_pct <= 12.0)
            and minutes_active >= 2
            and setup_bucket in {"BREAKOUT", "CONTINUATION"}
        ):
            return "RUNNER"
        if pnl_percent >= 12.0 and (premium_supportive or (expansion_ratio is not None and expansion_ratio >= 0.65)) and minutes_active >= 1:
            return "SWING_PUSH"
        if pnl_percent > 0:
            return "SCALP"
        return "STALLED"

    @staticmethod
    def build_journal_note(decision_label, action_text, extra=None):
        bits = [bit for bit in [decision_label, action_text, extra] if bit]
        return " | ".join(bits) if bits else None
