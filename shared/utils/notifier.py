"""
Notifier Module
Handles notifications and alerts
"""

from config import Config
import requests


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
        trigger_price = trade_data.get("trigger_price")
        invalidate_price = trade_data.get("invalidate_price")
        time_regime = trade_data.get("time_regime")
        reason = trade_data.get("reason")
        confidence_summary = trade_data.get("confidence_summary")
        first_target_price = trade_data.get("first_target_price")
        setup_label = _setup_label(signal_type)
        short_reason = _reason_summary(reason)
        lines = []
        header = f"{instrument} ACTION {signal}" if instrument else f"ACTION {signal}"
        lines.append(header)
        summary = []
        if signal_type:
            summary.append(f"Setup: {setup_label}")
        if grade:
            summary.append(f"Grade: {grade}")
        if confidence:
            summary.append(f"Confidence: {confidence}")
        if time_regime:
            summary.append(f"Time: {time_regime}")
        if summary:
            lines.append(" | ".join(summary))
        levels = []
        if strike is not None:
            levels.append(f"Strike: {strike}")
        if price is not None:
            levels.append(f"Spot: {price}")
        if trigger_price is not None:
            levels.append(f"Entry zone: {trigger_price}")
        if invalidate_price is not None:
            levels.append(f"Avoid below/above: {invalidate_price}")
        if first_target_price is not None:
            levels.append(f"Target zone: {first_target_price}")
        if levels:
            lines.append(" | ".join(levels))
        if confidence_summary:
            lines.append(f"Read: {confidence_summary}")
        lines.append(
            "Plan: " + _action_plan_for_setup(
                signal_type,
                signal,
                trigger_price,
                invalidate_price,
            )
        )
        if short_reason:
            lines.append(f"Why: {short_reason}")
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
        setup_label = _setup_label(setup)
        short_reason = _reason_summary(reason)

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
        if setup and setup != "NONE":
            summary.append(f"Setup: {setup_label}")
        if watch_bucket:
            summary.append(f"Type: {_watch_bucket_label(watch_bucket)}")
        if score is not None:
            summary.append(f"Score: {score}")
        if entry_score is not None:
            summary.append(f"EntryScore: {entry_score}")
        if confidence:
            summary.append(f"Confidence: {confidence}")
        if grade and grade != "SKIP":
            summary.append(f"Grade: {grade}")
        if summary:
            lines.append(" | ".join(summary))
        levels = []
        if spot_price is not None:
            levels.append(f"Spot: {spot_price}")
        if trigger_price is not None:
            levels.append(f"Trigger: {trigger_price}")
        if invalidate_price is not None:
            levels.append(f"Risk level: {invalidate_price}")
        if first_target_price is not None:
            levels.append(f"If confirms target: {first_target_price}")
        if levels:
            lines.append(" | ".join(levels))
        if context:
            lines.append(f"Context: {context}")
        if confidence_summary:
            lines.append(f"Read: {confidence_summary}")
        if short_reason:
            lines.append(f"Why: {short_reason}")
        if blockers:
            lines.append("Missing: " + ", ".join(_humanize_flag(flag) for flag in blockers[:3]))
        if cautions:
            lines.append("Caution: " + ", ".join(_humanize_flag(flag) for flag in cautions[:3]))
        plan = action_hint or _action_plan_for_setup(setup, direction, trigger_price, invalidate_price)
        if plan:
            lines.append(f"Plan: {plan}")

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
        prefix = f"{instrument} | " if instrument else ""
        message = f"{prefix}1M ENTRY TRIGGER {signal} | strike={strike} | price={price} | confidence={confidence}"
        if signal_type:
            message += f" | type={signal_type}"
        if signal_grade:
            message += f" | grade={signal_grade}"
        if trigger_price is not None:
            message += f" | trigger={trigger_price}"

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
        price = monitor_data.get("price")
        entry_price = monitor_data.get("entry_price")
        guidance = monitor_data.get("guidance")
        time_regime = monitor_data.get("time_regime")
        quality = monitor_data.get("quality")
        reason = monitor_data.get("reason")
        structure = monitor_data.get("structure")
        pnl_points = monitor_data.get("pnl_points")
        heikin_ashi = monitor_data.get("heikin_ashi")
        structure_short = structure or ""
        prefix = f"{instrument} | " if instrument else ""
        message = f"{prefix}{signal} | {guidance}"
        if pnl_points is not None:
            message += f" | {pnl_points:+.2f}pts"
        if structure_short:
            message += f" | {structure_short}"
        if heikin_ashi:
            message += f" | HA_{heikin_ashi}"
        if time_regime:
            message += f" | {time_regime}"
        if quality:
            message += f" | Q{quality}"
        if reason:
            message += f"\n{reason}"

        self.send_alert(message)
