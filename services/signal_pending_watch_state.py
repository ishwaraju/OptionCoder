"""Pending watch state setup helpers for SignalService."""

from config import Config


def set_pending_entry_watch(service, watch_payload, balanced_pro, candle_5m):
    if not watch_payload:
        if not service._preserve_existing_pending_watch(candle_5m):
            service._clear_pending_entry_watch()
        return

    direction = watch_payload.get("direction")
    trigger_price = watch_payload.get("trigger_price")
    if direction not in {"CE", "PE"} or trigger_price is None:
        service._clear_pending_entry_watch()
        return

    score = watch_payload.get("score") or 0
    entry_score = watch_payload.get("entry_score") or 0
    watch_bucket = watch_payload.get("watch_bucket")
    signal_grade = watch_payload.get("signal_grade")
    confidence = watch_payload.get("confidence")
    setup = watch_payload.get("setup")
    hybrid_mode = Config.HYBRID_MANUAL_MODE
    fast_track_ready = (
        score >= 74
        and entry_score >= 68
        and watch_bucket == "WATCH_CONFIRMATION_PENDING"
        and confidence in {"MEDIUM", "HIGH"}
    )
    if hybrid_mode and setup in {"REVERSAL", "BREAKOUT_CONFIRM", "RETEST", "TRAP_REVERSAL"}:
        fast_track_ready = fast_track_ready or (
            score >= 70 and entry_score >= 60 and confidence in {"MEDIUM", "HIGH"}
        )
    strong_watch_setup = (
        score >= 76
        and entry_score >= 70
        and watch_bucket in {"WATCH_CONFIRMATION_PENDING", "WATCH_SETUP"}
    )
    if hybrid_mode:
        strong_watch_setup = strong_watch_setup or (
            score >= 70
            and entry_score >= 60
            and watch_bucket in {"WATCH_CONFIRMATION_PENDING", "WATCH_SETUP", "WATCH_CONTEXT"}
            and setup in {"BREAKOUT_CONFIRM", "RETEST", "REVERSAL", "TRAP_REVERSAL"}
        )

    if watch_bucket == "WATCH_CONTEXT" and not strong_watch_setup:
        service._clear_pending_entry_watch()
        return
    min_context_score = 64 if hybrid_mode else 68
    min_entry_score = 54 if hybrid_mode else 60
    if score < min_context_score and entry_score < min_entry_score:
        service._clear_pending_entry_watch()
        return

    temp_pending = {
        "instrument": service.instrument,
        "signal_type": watch_payload.get("setup"),
        "cautions": list(watch_payload.get("cautions") or []),
        "blockers": list(watch_payload.get("blockers") or []),
        "pressure_conflict_level": (
            getattr(service.strategy, "last_pressure_conflict_level", None)
            or (balanced_pro or {}).get("pressure_conflict_level")
        ),
        "time_regime": (balanced_pro or {}).get("time_regime"),
    }
    conflicts_too_high, _ = service._pending_watch_conflicts_too_high(temp_pending)
    if conflicts_too_high:
        service._clear_pending_entry_watch()
        return

    service.pending_entry_watch = {
        "instrument": service.instrument,
        "direction": direction,
        "trigger_price": float(trigger_price),
        "invalidate_price": watch_payload.get("invalidate_price"),
        "first_target_price": watch_payload.get("first_target_price"),
        "option_stop_loss_pct": watch_payload.get("option_stop_loss_pct"),
        "option_target_pct": watch_payload.get("option_target_pct"),
        "option_trail_pct": watch_payload.get("option_trail_pct"),
        "risk_note": watch_payload.get("risk_note"),
        "context": watch_payload.get("context"),
        "score": score,
        "entry_score": entry_score,
        "confidence": confidence,
        "signal_type": watch_payload.get("setup"),
        "signal_grade": signal_grade,
        "watch_bucket": watch_payload.get("watch_bucket"),
        "quality": (balanced_pro or {}).get("quality"),
        "time_regime": (balanced_pro or {}).get("time_regime"),
        "created_at": candle_5m["time"],
        "last_checked_minute": None,
        "reason": watch_payload.get("reason"),
        "fast_track_ready": fast_track_ready,
        "strong_watch_setup": strong_watch_setup,
        "elite_watch_ready": service._pending_watch_elite_ready(
            {
                "signal_grade": signal_grade,
                "confidence": confidence,
                "signal_type": setup,
                "score": score,
                "entry_score": entry_score,
                "strong_watch_setup": strong_watch_setup,
            }
        ),
        "hybrid_mode": hybrid_mode,
        "cautions": list(watch_payload.get("cautions") or []),
        "blockers": list(watch_payload.get("blockers") or []),
        "pressure_conflict_level": (
            getattr(service.strategy, "last_pressure_conflict_level", None)
            or (balanced_pro or {}).get("pressure_conflict_level")
        ),
        "retrigger_count": 0,
        "retrigger_reason": None,
    }
    service.pending_entry_watch = service._rebalance_pending_watch_plan(service.pending_entry_watch, candle_5m)
