def confirmation_ready(
    *,
    direction,
    active_direction,
    price_vwap_aligned,
    close_beyond_level,
    not_extended,
    level_extension_ok,
    volume_signal,
    oi_bias_ok,
    oi_trend_ok,
    build_up_ok,
    no_opposite_pressure,
    candle_liquidity_ok,
    continuation_regime_ok,
    score,
    confirm_min_score,
    strong_context=False,
    breakout_structure_ok=False,
    ha_ok=False,
    strong_context_ready=False,
):
    if active_direction != direction:
        return False
    if not price_vwap_aligned or not close_beyond_level or not not_extended or not level_extension_ok:
        return False
    if strong_context:
        if volume_signal != "STRONG":
            return False
    elif volume_signal not in {"NORMAL", "STRONG"}:
        return False
    if not oi_bias_ok or not oi_trend_ok or not build_up_ok or not candle_liquidity_ok or not continuation_regime_ok:
        return False
    if strong_context and (not breakout_structure_ok or not ha_ok or not strong_context_ready):
        return False
    if not strong_context and not no_opposite_pressure:
        return False
    min_score = max(confirm_min_score + 10, 74) if strong_context else confirm_min_score
    return score >= min_score


def retest_ready(
    *,
    direction,
    active_direction,
    price_vwap_aligned,
    retest_touch_ok,
    close_holds_level,
    volume_signal,
    oi_bias_ok,
    oi_trend_ok,
    build_up_ok,
    no_opposite_pressure,
    candle_liquidity_ok,
    score,
    retest_min_score,
    opening_session,
    retest_regime_ok,
    strong_context=False,
    breakout_structure_ok=False,
    strong_context_ready=False,
):
    if active_direction != direction:
        return False
    if not price_vwap_aligned or not retest_touch_ok or not close_holds_level:
        return False
    if strong_context:
        if volume_signal != "STRONG":
            return False
    elif volume_signal not in {"NORMAL", "STRONG"}:
        return False
    if not oi_bias_ok or not oi_trend_ok or not build_up_ok or not candle_liquidity_ok:
        return False
    if opening_session or not retest_regime_ok:
        return False
    if strong_context and (not breakout_structure_ok or not strong_context_ready):
        return False
    if not strong_context and not no_opposite_pressure:
        return False
    min_score = max(retest_min_score + 8, 72) if strong_context else retest_min_score
    return score >= min_score
