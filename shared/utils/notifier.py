"""
Notifier Module
Handles notifications and alerts
"""

from config import Config
import requests
from datetime import datetime
from zoneinfo import ZoneInfo


def _humanize_flag(flag):
    mapping = {
        "direction_present_but_filters_incomplete": "setup almost ready, confirmation pending",
        "weak_breakout_body": "breakout candle body weak hai",
        "breakout_structure_weak": "breakout structure clean nahi hai",
        "adx_not_confirmed": "trend strength abhi confirm nahi hai",
        "opposite_pressure": "option pressure opposite side par hai",
        "pressure_conflict": "price aur option pressure align nahi kar rahe",
        "build_up_missing": "fresh build-up clear nahi hai",
        "oi_conflict": "OI direction mixed hai",
        "orb_breakout_missing": "ORB breakout abhi hua nahi",
        "orb_extension_too_far": "move kaafi extend ho chuka hai",
        "far_from_vwap": "price VWAP se kaafi door hai",
        "near_resistance": "resistance paas hai",
        "near_support": "support paas hai",
        "heikin_ashi_strong_opposite": "Heikin Ashi opposite side strong hai",
        "time_filter": "time window allow nahi karta",
        "opening_session": "opening volatility high hai",
        "participation_weak": "option participation weak hai",
        "participation_spread_wide": "ATM option spread wide hai",
        "participation_delta_missing": "same-side option volume delta strong nahi hai",
        "participation_baseline_weak": "current option participation recent average se strong nahi hai",
        "late_confirmation_wait_retest": "move extend ho chuka hai, retest ka wait better hai",
        "hybrid_price_led_setup": "price structure strong hai, options confirmation partial hai",
    }
    return mapping.get(flag, flag.replace("_", " "))


def _setup_label(setup):
    labels = {
        "BREAKOUT": "Breakout",
        "BREAKOUT_CONFIRM": "Breakout Confirm",
        "RETEST": "Retest",
        "REVERSAL": "Reversal",
        "CONTINUATION": "Continuation",
        "AGGRESSIVE_CONTINUATION": "Aggressive Continuation",
        "OPENING_DRIVE": "Opening Drive",
        "WATCH": "Watch",
        "NONE": "Watch",
    }
    return labels.get((setup or "WATCH").upper(), (setup or "Watch").replace("_", " ").title())


def _watch_bucket_label(bucket):
    labels = {
        "WATCH_SETUP": "Setup Watch",
        "WATCH_CONFIRMATION_PENDING": "Confirmation Watch",
        "WATCH_CONTEXT": "Context Watch",
        "NONE": "Watch",
    }
    return labels.get((bucket or "NONE").upper(), (bucket or "Watch").replace("_", " ").title())


def _watch_trigger_line(direction, trigger_price):
    if trigger_price is None:
        return None
    if direction == "CE":
        return f"CE watch above {trigger_price}"
    if direction == "PE":
        return f"PE watch below {trigger_price}"
    return f"Watch trigger {trigger_price}"


def _flip_summary_line(flip_context):
    if not flip_context:
        return None
    failed_side = flip_context.get("failed_side")
    buy_side = flip_context.get("buy_side")
    if failed_side and buy_side:
        return f"{failed_side} fail lag raha hai -> {buy_side} confirm hone par dekhna"
    return None


def _decision_label_text(label):
    mapping = {
        "CONFIRMED_CE_ENTRY": "Confirmed CE Entry",
        "CONFIRMED_PE_ENTRY": "Confirmed PE Entry",
        "WATCH_CE_SETUP": "Watch CE Setup",
        "WATCH_PE_SETUP": "Watch PE Setup",
        "WATCH_CE_FLIP": "Watch CE Flip",
        "WATCH_PE_FLIP": "Watch PE Flip",
        "CONFIRMED_CE_FLIP": "Confirmed CE Flip",
        "CONFIRMED_PE_FLIP": "Confirmed PE Flip",
        "THESIS_FAILED_EXIT": "Thesis Failed Exit",
        "HARD_STOP_EXIT": "Hard Stop Exit",
        "TRAIL_EXIT": "Trail Exit",
        "BOOK_PARTIAL_NOW": "Book Partial",
        "TIME_STOP_EXIT": "Time Stop Exit",
        "HIGH_NOISE_SKIP": "High Noise Skip",
        "WAIT_CONFIRMATION": "Wait Confirmation",
        "HOLD_CONTEXT": "Hold Context",
        "HOLD_STRONG": "Hold Strong",
    }
    if not label:
        return None
    return mapping.get(label, label.replace("_", " ").title())


def _monitor_guidance_badge(guidance, decision_label=None):
    guidance = (guidance or "").upper()
    decision_label = (decision_label or "").upper()

    if guidance == "HOLD_STRONG":
        return "LET WINNER RUN"
    if guidance == "HOLD_WITH_TRAIL":
        return "TRAIL ACTIVE"
    if guidance == "EXIT_PROFIT_PROTECT":
        return "PROFIT LOCK EXIT"
    if guidance in {"EXIT_BIAS", "EXIT_STOPLOSS", "EXIT_TIMESTOP", "EXIT_TRAIL"}:
        return "EXIT NOW"
    if guidance == "BOOK_PARTIAL":
        return "BOOK PARTIAL"
    if guidance in {"THESIS_WEAKENING", "MOMENTUM_PAUSE", "TIME_DECAY_RISK"}:
        return "WATCH CLOSELY"
    if decision_label in {"THESIS_FAILED_EXIT", "HARD_STOP_EXIT", "TRAIL_EXIT", "TIME_STOP_EXIT"}:
        return "EXIT NOW"
    return "MONITOR"


def _monitor_action_line(guidance, action_text, reason=None):
    guidance = (guidance or "").upper()
    if guidance == "HOLD_STRONG":
        return "Winner chal raha hai. Jab tak trail ya thesis break na ho, jaldi exit mat karo."
    if guidance == "HOLD_WITH_TRAIL":
        return "Profit protect ho raha hai. Position hold rakho, trail ko respect karo."
    if guidance == "EXIT_PROFIT_PROTECT":
        return "Profit bachao aur exit lo. Trade ka best phase shayad nikal chuka hai."
    if guidance in {"EXIT_BIAS", "EXIT_STOPLOSS", "EXIT_TIMESTOP", "EXIT_TRAIL"}:
        return action_text or "Abhi exit karo."
    if guidance == "BOOK_PARTIAL":
        return "Kuch profit book karo, baaki ko trail par chalne do."
    if guidance in {"THESIS_WEAKENING", "MOMENTUM_PAUSE", "TIME_DECAY_RISK"}:
        return action_text or "Agli candle closely dekho."
    return action_text or reason


def _no_trade_zone_line(no_trade_zone):
    if not no_trade_zone:
        return None
    label = _decision_label_text(no_trade_zone.get("label"))
    reason = no_trade_zone.get("reason")
    if label and reason:
        return f"{label}: {reason}"
    return label or reason


def _action_plan_for_setup(setup, direction, trigger_price, invalidate_price):
    setup = (setup or "WATCH").upper()
    direction_word = "upside" if direction == "CE" else "downside"
    if setup == "BREAKOUT":
        return f"Clean breakout dikhe to hi entry lo. False break se bachne ke liye level hold dekhna."
    if setup == "BREAKOUT_CONFIRM":
        return f"Confirmation mil gaya hai. Trigger level ke upar/neeche acceptance dikhe to entry socho."
    if setup == "RETEST":
        return f"Retest hold kare tabhi entry lo. Seedha chase mat karo."
    if setup == "REVERSAL":
        return f"Reversal setup hai. Next candle support kare tabhi trade lo, warna skip karo."
    if setup in {"CONTINUATION", "AGGRESSIVE_CONTINUATION"}:
        return f"Trend continuation hai. Momentum fade ho to avoid karo."
    if setup == "OPENING_DRIVE":
        return f"Opening move fast ho sakta hai. Sirf tab lo jab breakout {direction_word} hold kare."
    if trigger_price is not None:
        side = "above" if direction == "CE" else "below"
        return f"Abhi sirf watch karo. Entry se pehle clean 5m close {side} {trigger_price} ka wait karo."
    if invalidate_price is not None:
        return f"Risk level {invalidate_price} ke aas-paas dhyan rakho."
    return "Abhi sirf watch karo. Entry se pehle price confirmation ka wait karo."


def _reason_summary(reason):
    if not reason:
        return None

    primary = reason.split("|")[0].strip()
    if primary.startswith("ORB Breakout Up"):
        return "Price breakout, VWAP support aur bullish participation saath me aaye."
    if primary.startswith("ORB Breakdown Down"):
        return "Price breakdown, VWAP weakness aur bearish participation saath me aaye."
    if primary.startswith("Breakout confirmation above"):
        return "Breakout ke baad follow-through candle ne move confirm kiya."
    if primary.startswith("Breakdown confirmation below"):
        return "Breakdown ke baad follow-through candle ne move confirm kiya."
    if primary.startswith("Breakout retest support entry"):
        return "Breakout ke baad retest hold hua, isliye setup stronger hai."
    if primary.startswith("Breakdown retest resistance entry"):
        return "Breakdown ke baad retest fail hua, isliye setup stronger hai."
    if primary.startswith("Support Bounce"):
        return "Support zone se rejection aur bullish response dikh raha hai."
    if primary.startswith("Resistance Rejection"):
        return "Resistance zone se rejection aur bearish response dikh raha hai."
    if primary.startswith("High-score bullish continuation"):
        return "Bullish trend continue ho raha hai, momentum abhi intact hai."
    if primary.startswith("High-score bearish continuation"):
        return "Bearish trend continue ho raha hai, momentum abhi intact hai."
    if primary.startswith("Continuation follow-through setup"):
        return "Move continue kar raha hai, but confirmation ke saath hi lena better hoga."
    if primary.startswith("Opening drive breakout up"):
        return "Opening move me strong upside drive bani hai."
    if primary.startswith("Opening drive breakdown down"):
        return "Opening move me strong downside drive bani hai."
    if primary.startswith("Option-buyer filter blocked live alert"):
        return "Setup candidate mila tha, but final action rules ne isse aggressive entry ke liye approve nahi kiya."
    if primary.startswith("Directional context present but filters incomplete"):
        return "Direction clear hai, but entry quality abhi complete nahi hui."
    if primary.startswith("No valid setup"):
        return "Context weak ya mixed hai, isliye abhi clear setup nahi bana."
    if primary.startswith("ORB not ready"):
        return "Opening range clear nahi hai, isliye breakout setup abhi mature nahi hua."
    if primary.startswith("Pressure not aligned"):
        return "Price aur option pressure abhi same side par align nahi hain."
    if primary.startswith("Trade not allowed"):
        return "Time window ke hisaab se abhi trade avoid karna better hai."

    return primary


def _extract_reason_tags(reason):
    tags = {}
    if not reason:
        return tags
    for part in [item.strip() for item in reason.split("|") if item.strip()]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        tags[key.strip()] = value.strip()
    return tags


def _signal_explainer(reason, confidence_summary=None):
    summary = _reason_summary(reason)
    tags = _extract_reason_tags(reason)
    bullets = []
    if summary:
        bullets.append(summary)
    if tags.get("confidence_summary") and tags.get("confidence_summary") != confidence_summary:
        bullets.append(tags.get("confidence_summary"))
    blockers = tags.get("blockers")
    if blockers:
        compact = ", ".join([item.strip() for item in blockers.split(",")[:2] if item.strip()])
        if compact:
            bullets.append(f"Blockers: {compact}")
    cautions = tags.get("cautions")
    if cautions:
        compact = ", ".join([item.strip() for item in cautions.split(",")[:2] if item.strip()])
        if compact:
            bullets.append(f"Cautions: {compact}")
    return bullets[:3]


def _ist_now_label():
    try:
        return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("IST %H:%M")
    except Exception:
        return None


def _short_pressure_read(pressure_read):
    if not pressure_read:
        return None
    parts = [part.strip() for part in pressure_read.split("|") if part.strip()]
    return " | ".join(parts[:2]) if parts else pressure_read


def _short_oi_read(oi_read):
    if not oi_read:
        return None
    parts = [part.strip() for part in oi_read.split("|") if part.strip()]
    return " | ".join(parts[:3]) if parts else oi_read


class Notifier:
    """Notification handler"""
    
    def __init__(self):
        """Initialize notifier"""
        self.enabled = Config.ENABLE_ALERTS
        self.telegram_enabled = (
            self.enabled
            and Config.TELEGRAM_ENABLED
            and bool(Config.TELEGRAM_BOT_TOKEN)
            and bool(Config.TELEGRAM_CHAT_ID)
        )

    def _send_telegram(self, message):
        if not self.telegram_enabled:
            return

        try:
            requests.post(
                f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": Config.TELEGRAM_CHAT_ID,
                    "text": message,
                },
                timeout=5,
            )
        except Exception as e:
            print(f"[TELEGRAM ERROR] {e}")
    
    def send_alert(self, message):
        """Send alert notification"""
        if not self.enabled:
            return
        if Config.ENABLE_SOUND_ALERT:
            print("\a", end="")
        print(f"[ALERT] {message}")
        self._send_telegram(message)
    
    def send_trade_notification(self, trade_data):
        """Send trade notification"""
        if not self.enabled:
            return
        signal = trade_data.get("signal")
        strike = trade_data.get("strike")
        confidence = trade_data.get("confidence")
        signal_type = trade_data.get("signal_type")
        grade = trade_data.get("signal_grade")
        instrument = trade_data.get("instrument")
        price = trade_data.get("price")
        spot_price = trade_data.get("spot_price")
        trigger_price = trade_data.get("trigger_price")
        invalidate_price = trade_data.get("invalidate_price")
        time_regime = trade_data.get("time_regime")
        reason = trade_data.get("reason")
        confidence_summary = trade_data.get("confidence_summary")
        first_target_price = trade_data.get("first_target_price")
        setup_bucket = trade_data.get("setup_bucket")
        option_stop_loss_pct = trade_data.get("option_stop_loss_pct")
        option_target_pct = trade_data.get("option_target_pct")
        option_trail_pct = trade_data.get("option_trail_pct")
        time_stop_warn_minutes = trade_data.get("time_stop_warn_minutes")
        time_stop_exit_minutes = trade_data.get("time_stop_exit_minutes")
        risk_note = trade_data.get("risk_note")
        rr_ratio = trade_data.get("rr_ratio")
        decision_label = trade_data.get("decision_label")
        action_text = trade_data.get("action_text")
        journal_note = trade_data.get("journal_note")
        structure_suggestion = trade_data.get("structure_suggestion")
        pressure_read = trade_data.get("pressure_read")
        oi_read = trade_data.get("oi_read")
        setup_label = _setup_label(signal_type)
        lines = []
        header = f"{instrument} ACTION {signal}" if instrument else f"ACTION {signal}"
        lines.append(header)
        summary = []
        ist_label = _ist_now_label()
        if ist_label:
            summary.append(ist_label)
        if signal_type:
            summary.append(setup_label)
        if setup_bucket:
            summary.append(setup_bucket)
        if decision_label:
            summary.append(_decision_label_text(decision_label))
        if grade:
            summary.append(grade)
        if confidence:
            summary.append(confidence)
        if summary:
            lines.append(" | ".join(summary))
        levels = []
        if price is not None:
            levels.append(f"Buy {price}")
        if spot_price is not None:
            levels.append(f"Spot {spot_price}")
        if trigger_price is not None:
            levels.append(f"Trig {trigger_price}")
        if invalidate_price is not None:
            levels.append(f"SL {invalidate_price}")
        if first_target_price is not None:
            levels.append(f"T1 {first_target_price}")
        if strike is not None:
            levels.append(f"Strike {strike}")
        if levels:
            lines.append(" | ".join(levels))
        risk_bits = []
        if option_stop_loss_pct is not None:
            risk_bits.append(f"OptSL {option_stop_loss_pct:.0f}%")
        if option_target_pct is not None:
            risk_bits.append(f"OptT1 {option_target_pct:.0f}%")
        if option_trail_pct is not None:
            risk_bits.append(f"Trail {option_trail_pct:.0f}%")
        if rr_ratio is not None:
            risk_bits.append(f"RR {rr_ratio}")
        if time_stop_warn_minutes is not None and time_stop_exit_minutes is not None:
            risk_bits.append(f"TS {time_stop_warn_minutes}/{time_stop_exit_minutes}m")
        if risk_bits:
            lines.append(" | ".join(risk_bits))
        flow = []
        short_pressure = _short_pressure_read(pressure_read)
        short_oi = _short_oi_read(oi_read)
        if short_pressure:
            flow.append(short_pressure)
        if short_oi:
            flow.append(short_oi)
        elif confidence_summary:
            flow.append(confidence_summary)
        if flow:
            lines.append(" | ".join(flow[:2]))
        for explainer in _signal_explainer(reason, confidence_summary=confidence_summary):
            lines.append(explainer)
        if action_text:
            lines.append(action_text)
        if structure_suggestion:
            structure_line = (
                f"Structure: {structure_suggestion.get('type')} | "
                f"Buy {structure_suggestion.get('long_strike')} | "
                f"Sell {structure_suggestion.get('short_strike')}"
            )
            lines.append(structure_line)
            if structure_suggestion.get("rationale"):
                lines.append(structure_suggestion.get("rationale"))
        if journal_note:
            lines.append(journal_note)
        if risk_note:
            lines.append(risk_note)
        message = "\n".join(lines)
        self.send_alert(message)

    def send_watch_notification(self, watch_data):
        """Send a curated watch alert for manual traders."""
        if not self.enabled:
            return

        instrument = watch_data.get("instrument")
        direction = watch_data.get("direction")
        setup = watch_data.get("setup")
        grade = watch_data.get("signal_grade")
        confidence = watch_data.get("confidence")
        score = watch_data.get("score")
        entry_score = watch_data.get("entry_score")
        spot_price = watch_data.get("price")
        trigger_price = watch_data.get("trigger_price")
        invalidate_price = watch_data.get("invalidate_price")
        first_target_price = watch_data.get("first_target_price")
        blockers = watch_data.get("blockers") or []
        cautions = watch_data.get("cautions") or []
        context = watch_data.get("context")
        action_hint = watch_data.get("action_hint")
        reason = watch_data.get("reason")
        watch_bucket = watch_data.get("watch_bucket")
        confidence_summary = watch_data.get("confidence_summary")
        entry_if = watch_data.get("entry_if")
        avoid_if = watch_data.get("avoid_if")
        participation_read = watch_data.get("participation_read")
        pressure_read = watch_data.get("pressure_read")
        flip_context = watch_data.get("flip_context")
        setup_bucket = watch_data.get("setup_bucket")
        option_stop_loss_pct = watch_data.get("option_stop_loss_pct")
        option_target_pct = watch_data.get("option_target_pct")
        option_trail_pct = watch_data.get("option_trail_pct")
        time_stop_warn_minutes = watch_data.get("time_stop_warn_minutes")
        time_stop_exit_minutes = watch_data.get("time_stop_exit_minutes")
        risk_note = watch_data.get("risk_note")
        decision_label = watch_data.get("decision_label")
        action_text = watch_data.get("action_text") or watch_data.get("action_hint")
        no_trade_zone = watch_data.get("no_trade_zone")
        journal_note = watch_data.get("journal_note")
        structure_suggestion = watch_data.get("structure_suggestion")
        setup_label = _setup_label(setup)

        lines = []
        trigger_line = _watch_trigger_line(direction, trigger_price)
        if instrument and trigger_line:
            header = f"{instrument} {trigger_line}"
        elif instrument:
            header = f"{instrument} WATCH {direction}"
        else:
            header = trigger_line or f"WATCH {direction}"
        lines.append(header)
        summary = []
        ist_label = _ist_now_label()
        if ist_label:
            summary.append(ist_label)
        if setup and setup != "NONE":
            summary.append(setup_label)
        if setup_bucket:
            summary.append(setup_bucket)
        if decision_label:
            summary.append(_decision_label_text(decision_label))
        if score is not None:
            summary.append(f"S:{score}")
        if entry_score is not None:
            summary.append(f"E:{entry_score}")
        if grade and grade != "SKIP":
            summary.append(f"G:{grade}")
        if summary:
            lines.append(" | ".join(summary))
        levels = []
        if spot_price is not None:
            levels.append(f"Spot {spot_price}")
        if trigger_price is not None:
            levels.append(f"Trig {trigger_price}")
        if invalidate_price is not None:
            levels.append(f"SL {invalidate_price}")
        if first_target_price is not None and grade in {"A", "B", "WATCH"}:
            levels.append(f"T1 {first_target_price}")
        if levels:
            lines.append(" | ".join(levels))
        risk_bits = []
        if option_stop_loss_pct is not None:
            risk_bits.append(f"OptSL {option_stop_loss_pct:.0f}%")
        if option_target_pct is not None:
            risk_bits.append(f"OptT1 {option_target_pct:.0f}%")
        if option_trail_pct is not None:
            risk_bits.append(f"Trail {option_trail_pct:.0f}%")
        if time_stop_warn_minutes is not None and time_stop_exit_minutes is not None:
            risk_bits.append(f"TS {time_stop_warn_minutes}/{time_stop_exit_minutes}m")
        if risk_bits:
            lines.append(" | ".join(risk_bits[:2]))
        flow = []
        flip_line = _flip_summary_line(flip_context)
        if flip_line:
            flow.append(flip_line)
        short_pressure = _short_pressure_read(pressure_read)
        if short_pressure:
            flow.append(short_pressure)
        elif not flip_line and participation_read:
            flow.append(participation_read.split("|", 1)[0].strip())
        if flow:
            lines.append(" | ".join(flow[:2]))
        for explainer in _signal_explainer(reason, confidence_summary=confidence_summary)[:2]:
            lines.append(explainer)

        actions = []
        if entry_if:
            actions.append(f"Entry: 5m close {'above' if direction == 'CE' else 'below'} {trigger_price}")
        if avoid_if:
            actions.append(f"Avoid: {'below' if direction == 'CE' else 'above'} {invalidate_price}")
        if actions:
            lines.append(" | ".join(actions[:2]))
        no_trade_line = _no_trade_zone_line(no_trade_zone)
        if no_trade_line:
            lines.append(no_trade_line)
        if action_text:
            lines.append(action_text)
        if structure_suggestion:
            lines.append(
                f"Structure idea: {structure_suggestion.get('type')} | Buy {structure_suggestion.get('long_strike')} | Sell {structure_suggestion.get('short_strike')}"
            )
        if journal_note:
            lines.append(journal_note)
        if risk_note:
            lines.append(risk_note)

        message = "\n".join(lines)

        self.send_alert(message)

    def send_entry_trigger_notification(self, trigger_data):
        """Send immediate 1-minute entry trigger notification"""
        if not self.enabled:
            return

        signal = trigger_data.get("signal")
        strike = trigger_data.get("strike")
        confidence = trigger_data.get("confidence")
        signal_type = trigger_data.get("signal_type")
        signal_grade = trigger_data.get("signal_grade")
        price = trigger_data.get("price")
        trigger_price = trigger_data.get("trigger_price")

        instrument = trigger_data.get("instrument")
        header_parts = []
        if instrument:
            header_parts.append(instrument)
        header_parts.append(f"{signal} ENTRY")
        header = " ".join(header_parts)

        summary = []
        if signal_type:
            summary.append(_setup_label(signal_type))
        if confidence:
            summary.append(confidence)
        if signal_grade and signal_grade != "SKIP":
            summary.append(f"G:{signal_grade}")
        if strike is not None:
            summary.append(f"Strike {strike}")

        levels = []
        if price is not None:
            levels.append(f"Price {price}")
        if trigger_price is not None:
            levels.append(f"Trig {trigger_price}")

        lines = [header]
        ist_label = _ist_now_label()
        if ist_label:
            summary.insert(0, ist_label)
        if summary:
            lines.append(" | ".join(summary))
        if levels:
            lines.append(" | ".join(levels))
        for explainer in _signal_explainer(trigger_data.get("reason"), confidence_summary=trigger_data.get("confidence_summary"))[:2]:
            lines.append(explainer)

        message = "\n".join(lines)

        self.send_alert(message)
    
    def send_error_alert(self, error_message):
        """Send error alert"""
        if not self.enabled:
            return
        self.send_alert(f"ERROR: {error_message}")
    
    def send_trade_monitor_update(self, monitor_data):
        """Send manual trade monitor guidance update."""
        if not self.enabled:
            return

        instrument = monitor_data.get("instrument")
        signal = monitor_data.get("signal")
        signal_type = monitor_data.get("signal_type")
        setup_bucket = monitor_data.get("setup_bucket")
        price = monitor_data.get("price")
        option_price = monitor_data.get("option_price")
        entry_price = monitor_data.get("entry_price")
        guidance = monitor_data.get("guidance")
        time_regime = monitor_data.get("time_regime")
        quality = monitor_data.get("quality")
        reason = monitor_data.get("reason")
        structure = monitor_data.get("structure")
        pnl_points = monitor_data.get("pnl_points")
        pnl_percent = monitor_data.get("pnl_percent")
        stop_loss_pct = monitor_data.get("stop_loss_pct")
        target_pct = monitor_data.get("target_pct")
        trail_pct = monitor_data.get("trail_pct")
        stop_loss_option_price = monitor_data.get("stop_loss_option_price")
        first_target_option_price = monitor_data.get("first_target_option_price")
        drawdown_from_peak_pct = monitor_data.get("drawdown_from_peak_pct")
        invalidate_underlying_price = monitor_data.get("invalidate_underlying_price")
        time_stop_warn_minutes = monitor_data.get("time_stop_warn_minutes")
        time_stop_exit_minutes = monitor_data.get("time_stop_exit_minutes")
        risk_note = monitor_data.get("risk_note")
        decision_label = monitor_data.get("decision_label")
        action_text = monitor_data.get("action_text")
        journal_note = monitor_data.get("journal_note")
        expansion_ratio = monitor_data.get("expansion_ratio")
        expected_option_move = monitor_data.get("expected_option_move")
        actual_option_move = monitor_data.get("actual_option_move")
        flip_score = monitor_data.get("flip_score")
        flip_confidence = monitor_data.get("flip_confidence")
        heikin_ashi = monitor_data.get("heikin_ashi")
        dynamic_trail_pct = monitor_data.get("dynamic_trail_pct")
        profit_lock_armed = monitor_data.get("profit_lock_armed")
        profit_lock_trigger_pct = monitor_data.get("profit_lock_trigger_pct")
        psar_style_level = monitor_data.get("psar_style_level")
        live_pressure_summary = monitor_data.get("live_pressure_summary")
        lines = []
        header_parts = []
        if instrument:
            header_parts.append(instrument)
        if signal:
            header_parts.append(signal)
        header_parts.append(_monitor_guidance_badge(guidance, decision_label))
        lines.append(" | ".join(header_parts))

        summary = []
        ist_label = _ist_now_label()
        if ist_label:
            summary.append(ist_label)
        if signal_type:
            summary.append(_setup_label(signal_type))
        if setup_bucket:
            summary.append(setup_bucket)
        if pnl_points is not None:
            summary.append(f"{pnl_points:+.2f}pts")
        if pnl_percent is not None:
            summary.append(f"{pnl_percent:+.2f}%")
        if option_price is not None:
            summary.append(f"Opt {option_price}")
        if price is not None:
            summary.append(f"Spot {price}")
        if entry_price is not None:
            summary.append(f"Entry {entry_price}")
        if summary:
            lines.append(" | ".join(summary))

        risk_bits = []
        if stop_loss_option_price is not None and stop_loss_pct is not None:
            risk_bits.append(f"Hard SL {stop_loss_option_price} ({stop_loss_pct:.0f}%)")
        if first_target_option_price is not None and target_pct is not None:
            risk_bits.append(f"T1 {first_target_option_price} ({target_pct:.0f}%)")
        if dynamic_trail_pct is not None:
            risk_bits.append(f"Trail {dynamic_trail_pct:.1f}%")
        elif trail_pct is not None:
            risk_bits.append(f"Trail {trail_pct:.0f}%")
        if drawdown_from_peak_pct is not None:
            risk_bits.append(f"PeakDD {drawdown_from_peak_pct:.2f}%")
        if invalidate_underlying_price is not None:
            risk_bits.append(f"Inv {invalidate_underlying_price}")
        if time_stop_warn_minutes is not None and time_stop_exit_minutes is not None:
            risk_bits.append(f"TS {time_stop_warn_minutes}/{time_stop_exit_minutes}m")
        if expansion_ratio is not None:
            risk_bits.append(f"Exp {expansion_ratio}")
        if flip_score is not None:
            risk_bits.append(f"Flip {flip_score:.0f}")
        if risk_bits:
            lines.append(" | ".join(risk_bits[:2]))
        state_bits = []
        if profit_lock_armed:
            state_bits.append(
                f"Profit lock ON{f' @ +{profit_lock_trigger_pct:.0f}%' if profit_lock_trigger_pct is not None else ''}"
            )
        if psar_style_level is not None:
            state_bits.append(f"Trail level {psar_style_level}")
        if state_bits:
            lines.append(" | ".join(state_bits[:2]))
        if expected_option_move is not None and actual_option_move is not None:
            lines.append(f"Option move {actual_option_move} vs expected {expected_option_move}")
        if flip_confidence:
            lines.append(f"Flip confidence: {flip_confidence}")
        if live_pressure_summary:
            lines.append(live_pressure_summary)
        if risk_note:
            lines.append(risk_note)
        action_line = _monitor_action_line(guidance, action_text, reason=reason)
        if action_line:
            lines.append(action_line)
        if reason and reason != action_line:
            lines.append(reason)
        if journal_note:
            lines.append(journal_note)

        self.send_alert("\n".join(lines))
