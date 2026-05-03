from config import Config


WATCH_MARKERS = {
    "direction_present_but_filters_incomplete",
    "weak_breakout_body",
    "breakout_structure_weak",
    "pressure_conflict",
    "pressure_neutral",
    "oi_conflict",
    "build_up_missing",
    "orb_breakout_missing",
    "orb_extension_too_far",
    "adx_not_confirmed",
    "higher_tf_not_aligned",
    "volume_weak",
}

WATCH_CAUTION_MARKERS = {
    "opposite_pressure",
    "adx_not_confirmed",
    "far_from_vwap",
    "pressure_neutral",
}


def grade_signal(score, confidence, cautions, blockers, signal_type):
    if signal_type in [None, "NONE"]:
        return "SKIP"

    blocker_penalty = len(blockers or [])
    caution_penalty = len(cautions or [])

    if score >= 85 and confidence == "HIGH" and blocker_penalty == 0 and caution_penalty == 0:
        return "A+"
    if score >= 75 and confidence in ["HIGH", "MEDIUM"] and blocker_penalty == 0 and caution_penalty <= 1:
        return "A"
    if score >= 65 and confidence in ["HIGH", "MEDIUM"]:
        return "B"
    return "WATCH"


def calculate_entry_score(
    score,
    breakout_body_ok,
    breakout_structure_ok,
    candle_liquidity_ok,
    volume_signal,
    cautions,
    blockers,
    adx_trade_ok,
    mtf_trade_ok,
    pressure_conflict_level="NONE",
):
    entry_score = min(int(score), 100)
    if breakout_body_ok:
        entry_score += 8
    else:
        entry_score -= 12
    if breakout_structure_ok:
        entry_score += 8
    else:
        entry_score -= 10
    if candle_liquidity_ok:
        entry_score += 4
    else:
        entry_score -= 10
    if volume_signal == "STRONG":
        entry_score += 8
    elif volume_signal == "WEAK":
        entry_score -= 8
    if not adx_trade_ok:
        entry_score -= 8
    if not mtf_trade_ok:
        entry_score -= 6
    if pressure_conflict_level == "MILD":
        entry_score -= 6
    elif pressure_conflict_level == "MODERATE":
        entry_score -= 11
    elif pressure_conflict_level == "HARD":
        entry_score -= 16
    elif "opposite_pressure" in (cautions or []):
        entry_score -= 12
    if "far_from_vwap" in (cautions or []):
        entry_score -= 6
    if "near_resistance" in (cautions or []) or "near_support" in (cautions or []):
        entry_score -= 4
    entry_score -= min(len(blockers or []) * 4, 20)
    entry_score -= min(len(cautions or []) * 2, 12)
    return max(0, min(entry_score, 100))


def derive_decision_state(signal_type, signal, score, entry_score, confidence, blockers, cautions):
    if signal and signal_type not in {None, "NONE"}:
        if confidence in {"MEDIUM", "HIGH"} and entry_score >= 70 and len(blockers or []) == 0:
            return "ACTION"
        return "WATCH"

    if score >= max(Config.MIN_SCORE_THRESHOLD, 60) and confidence in {"MEDIUM", "HIGH"}:
        if any(marker in (blockers or []) for marker in WATCH_MARKERS) or any(
            marker in (cautions or []) for marker in WATCH_CAUTION_MARKERS
        ):
            return "WATCH"
    return "IGNORE"
