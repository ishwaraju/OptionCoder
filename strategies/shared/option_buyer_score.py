def clamp_score(value):
    return max(0, min(100, int(round(float(value or 0)))))


def option_buyer_action(score):
    score = clamp_score(score)
    if score >= 80:
        return "ACTION"
    if score >= 70:
        return "READY"
    if score >= 55:
        return "WAIT"
    return "AVOID"


def calculate_option_buyer_entry_score(
    *,
    base_entry_score=0,
    strategy_score=0,
    momentum_score=None,
    premium_state=None,
    liquidity_quality=None,
    spread_percent=None,
    blockers=None,
    cautions=None,
    confidence=None,
    signal_grade=None,
):
    """Combine strategy, 1m momentum, and option premium quality into one entry score."""
    base = max(float(base_entry_score or 0), float(strategy_score or 0) * 0.85)
    score = base

    if momentum_score is not None:
        score = (score * 0.68) + (float(momentum_score or 0) * 0.32)

    premium_state = (premium_state or "").upper()
    liquidity_quality = (liquidity_quality or "").upper()
    confidence = (confidence or "").upper()
    signal_grade = (signal_grade or "").upper()

    if premium_state == "EXPANDING":
        score += 8
    elif premium_state == "UP":
        score += 4
    elif premium_state == "FADING":
        score -= 12
    elif premium_state == "UNKNOWN":
        score -= 4

    if liquidity_quality == "GOOD":
        score += 5
    elif liquidity_quality == "OK":
        score += 2
    elif liquidity_quality == "POOR":
        score -= 12
    elif liquidity_quality == "UNKNOWN":
        score -= 4

    if spread_percent is not None:
        spread_percent = float(spread_percent)
        if spread_percent <= 2.0:
            score += 3
        elif spread_percent > 5.0:
            score -= 8

    if confidence == "HIGH":
        score += 4
    elif confidence == "LOW":
        score -= 5

    if signal_grade in {"A+", "A"}:
        score += 3
    elif signal_grade in {"C", "WATCH"}:
        score -= 6

    score -= min(len(blockers or []) * 6, 24)
    score -= min(len(cautions or []) * 2, 12)
    return clamp_score(score)
