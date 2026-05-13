from config import Config
from strategies.shared.continuation_evaluator import fallback_continuation_ready, high_score_continuation_ready


def evaluate_core_continuations(strategy, ctx):
    scored_direction = ctx["scored_direction"]
    time_thresholds = ctx["time_thresholds"]
    price = ctx["price"]
    vwap = ctx["vwap"]
    volume_signal = ctx["volume_signal"]
    score = ctx["score"]
    candle_range = ctx["candle_range"]
    atr = ctx["atr"]
    oi_bias = ctx["oi_bias"]
    oi_trend = ctx["oi_trend"]
    bullish_build_up_ok = ctx["bullish_build_up_ok"]
    bearish_build_up_ok = ctx["bearish_build_up_ok"]
    orb_high = ctx["orb_high"]
    orb_low = ctx["orb_low"]
    buffer = ctx["buffer"]
    tuning = ctx["tuning"]
    candle_liquidity_ok = ctx["candle_liquidity_ok"]
    breakout_body_ok = ctx["breakout_body_ok"]
    breakout_structure_ok = ctx["breakout_structure_ok"]
    bullish_ha_ok = ctx["bullish_ha_ok"]
    bearish_ha_ok = ctx["bearish_ha_ok"]
    cautions = ctx["cautions"]
    continuation_override_ok = ctx["continuation_override_ok"]
    pressure_metrics = ctx["pressure_metrics"]
    continuation_regime_ok = ctx["continuation_regime_ok"]
    adx_trade_ok = ctx["adx_trade_ok"]
    mtf_trade_ok = ctx["mtf_trade_ok"]
    pressure_conflict_level = ctx["pressure_conflict_level"]
    recent_breakout_context = ctx["recent_breakout_context"]
    time_regime = ctx["time_regime"]
    blockers = ctx["blockers"]
    expiry_eval = ctx["expiry_eval"]
    regime = ctx["regime"]
    candle_time = ctx["candle_time"]
    support = ctx["support"]
    resistance = ctx["resistance"]
    opening_session = ctx["opening_session"]

    if scored_direction == "CE" and high_score_continuation_ready(price_vwap_aligned=price > vwap, volume_ok=strategy._sensex_volume_flexible(volume_signal, score, time_regime, candle_range, atr), score=score, high_continuation_min_score=time_thresholds["high_continuation_min_score"], oi_bias_ok=oi_bias in ["BULLISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BULLISH", "NEUTRAL", None], build_up_ok=bullish_build_up_ok, orb_extension_ok=(orb_high is None or price <= orb_high + (buffer * tuning["extension_buffer_mult"])), candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, ha_ok=bullish_ha_ok, far_from_vwap_ok=("far_from_vwap" not in cautions or (score >= 82 and volume_signal == "STRONG" and pressure_metrics and pressure_metrics["pressure_bias"] == "BULLISH") or continuation_override_ok), opposite_pressure_ok=("opposite_pressure" not in cautions or continuation_override_ok), continuation_regime_ok=continuation_regime_ok, adx_trade_ok=adx_trade_ok, mtf_trade_ok=mtf_trade_ok, pressure_conflict_ok=(pressure_conflict_level in {"NONE", "MILD"} or recent_breakout_context), focused_mode_ok=(not Config.FOCUSED_MANUAL_MODE or recent_breakout_context or (time_regime in ["OPENING", "MID_MORNING"] and score >= 78 and volume_signal == "STRONG"))):
        return strategy._emit_trade_signal("CE", "CONTINUATION", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="High-score bullish continuation", trigger_price=price, invalidate_price=vwap, atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True)

    if scored_direction == "CE" and fallback_continuation_ready(price_vwap_aligned=price > vwap, volume_ok=strategy._sensex_volume_flexible(volume_signal, score, time_regime, candle_range, atr), score=score, continuation_min_score=time_thresholds["continuation_min_score"], oi_bias_ok=oi_bias in ["BULLISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BULLISH", "NEUTRAL", None], build_up_ok=bullish_build_up_ok, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, ha_ok=bullish_ha_ok, opposite_pressure_ok=("opposite_pressure" not in cautions or continuation_override_ok), far_from_vwap_ok=("far_from_vwap" not in cautions or continuation_override_ok), opening_session=opening_session, continuation_regime_ok=continuation_regime_ok, adx_ok=(adx_trade_ok or recent_breakout_context or continuation_override_ok), mtf_ok=(mtf_trade_ok or recent_breakout_context or continuation_override_ok), pressure_conflict_ok=(recent_breakout_context or pressure_conflict_level == "NONE" or continuation_override_ok), focused_mode_ok=(not Config.FOCUSED_MANUAL_MODE or recent_breakout_context or continuation_override_ok)):
        return strategy._emit_trade_signal("CE", "CONTINUATION", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="Continuation follow-through setup", trigger_price=price, invalidate_price=vwap, atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True)

    if scored_direction == "PE" and high_score_continuation_ready(price_vwap_aligned=price < vwap, volume_ok=strategy._sensex_volume_flexible(volume_signal, score, time_regime, candle_range, atr), score=score, high_continuation_min_score=time_thresholds["high_continuation_min_score"], oi_bias_ok=oi_bias in ["BEARISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BEARISH", "NEUTRAL", None], build_up_ok=bearish_build_up_ok, orb_extension_ok=(orb_low is None or price >= orb_low - (buffer * tuning["extension_buffer_mult"])), candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, ha_ok=bearish_ha_ok, far_from_vwap_ok=("far_from_vwap" not in cautions or (score >= 82 and volume_signal == "STRONG" and pressure_metrics and pressure_metrics["pressure_bias"] == "BEARISH") or continuation_override_ok), opposite_pressure_ok=("opposite_pressure" not in cautions or continuation_override_ok), continuation_regime_ok=continuation_regime_ok, adx_trade_ok=adx_trade_ok, mtf_trade_ok=mtf_trade_ok, pressure_conflict_ok=(pressure_conflict_level in {"NONE", "MILD"} or recent_breakout_context), focused_mode_ok=(not Config.FOCUSED_MANUAL_MODE or recent_breakout_context or (time_regime in ["OPENING", "MID_MORNING"] and score >= 78 and volume_signal == "STRONG"))):
        return strategy._emit_trade_signal("PE", "CONTINUATION", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="High-score bearish continuation", trigger_price=price, invalidate_price=vwap, atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True)

    if scored_direction == "PE" and fallback_continuation_ready(price_vwap_aligned=price < vwap, volume_ok=strategy._sensex_volume_flexible(volume_signal, score, time_regime, candle_range, atr), score=score, continuation_min_score=time_thresholds["continuation_min_score"], oi_bias_ok=oi_bias in ["BEARISH", "NEUTRAL"], oi_trend_ok=oi_trend in ["BEARISH", "NEUTRAL", None], build_up_ok=bearish_build_up_ok, candle_liquidity_ok=candle_liquidity_ok, breakout_body_ok=breakout_body_ok, breakout_structure_ok=breakout_structure_ok, ha_ok=bearish_ha_ok, opposite_pressure_ok=("opposite_pressure" not in cautions or continuation_override_ok), far_from_vwap_ok=("far_from_vwap" not in cautions or continuation_override_ok), opening_session=opening_session, continuation_regime_ok=continuation_regime_ok, adx_ok=(adx_trade_ok or recent_breakout_context or continuation_override_ok), mtf_ok=(mtf_trade_ok or recent_breakout_context or continuation_override_ok), pressure_conflict_ok=(recent_breakout_context or pressure_conflict_level == "NONE" or continuation_override_ok), focused_mode_ok=(not Config.FOCUSED_MANUAL_MODE or recent_breakout_context or continuation_override_ok)):
        return strategy._emit_trade_signal("PE", "CONTINUATION", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="Continuation follow-through setup", trigger_price=price, invalidate_price=vwap, atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True)

    return None


def evaluate_aggressive_continuations(strategy, ctx):
    score = ctx["score"]
    price = ctx["price"]
    vwap = ctx["vwap"]
    volume_signal = ctx["volume_signal"]
    oi_bias = ctx["oi_bias"]
    oi_trend = ctx["oi_trend"]
    bullish_build_up_ok = ctx["bullish_build_up_ok"]
    bearish_build_up_ok = ctx["bearish_build_up_ok"]
    candle_liquidity_ok = ctx["candle_liquidity_ok"]
    breakout_body_ok = ctx["breakout_body_ok"]
    breakout_structure_ok = ctx["breakout_structure_ok"]
    bullish_ha_ok = ctx["bullish_ha_ok"]
    bearish_ha_ok = ctx["bearish_ha_ok"]
    cautions = ctx["cautions"]
    continuation_override_ok = ctx["continuation_override_ok"]
    opening_session = ctx["opening_session"]
    regime = ctx["regime"]
    adx_trade_ok = ctx["adx_trade_ok"]
    mtf_trade_ok = ctx["mtf_trade_ok"]
    blockers = ctx["blockers"]
    pressure_metrics = ctx["pressure_metrics"]
    expiry_eval = ctx["expiry_eval"]
    candle_time = ctx["candle_time"]
    atr = ctx["atr"]
    support = ctx["support"]
    resistance = ctx["resistance"]
    time_thresholds = ctx["time_thresholds"]

    if Config.AGGRESSIVE_MODE and not Config.FOCUSED_MANUAL_MODE and time_thresholds["allow_fallback_continuation"] and ctx["scored_direction"] == "CE" and price > vwap and volume_signal in ["STRONG", "NORMAL"] and score >= 50 and oi_bias in ["BULLISH", "NEUTRAL"] and oi_trend in ["BULLISH", "NEUTRAL", None] and bullish_build_up_ok and candle_liquidity_ok and breakout_body_ok and breakout_structure_ok and bullish_ha_ok and ("opposite_pressure" not in cautions or continuation_override_ok) and ("far_from_vwap" not in cautions or continuation_override_ok) and not opening_session and regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"] and adx_trade_ok and mtf_trade_ok:
        return strategy._emit_trade_signal("CE", "AGGRESSIVE_CONTINUATION", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="Aggressive bullish continuation", trigger_price=price, invalidate_price=vwap, atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True)

    if Config.AGGRESSIVE_MODE and not Config.FOCUSED_MANUAL_MODE and time_thresholds["allow_fallback_continuation"] and ctx["scored_direction"] == "PE" and price < vwap and volume_signal in ["STRONG", "NORMAL"] and score >= 50 and oi_bias in ["BEARISH", "NEUTRAL"] and oi_trend in ["BEARISH", "NEUTRAL", None] and bearish_build_up_ok and candle_liquidity_ok and breakout_body_ok and breakout_structure_ok and bearish_ha_ok and ("opposite_pressure" not in cautions or continuation_override_ok) and ("far_from_vwap" not in cautions or continuation_override_ok) and not opening_session and regime in ["TRENDING", "EXPANDING", "OPENING_EXPANSION"] and adx_trade_ok and mtf_trade_ok:
        return strategy._emit_trade_signal("PE", "AGGRESSIVE_CONTINUATION", score, volume_signal, pressure_metrics, cautions, blockers=blockers, regime=strategy._effective_signal_regime(expiry_eval, regime), candle_time=candle_time, message="Aggressive bearish continuation", trigger_price=price, invalidate_price=vwap, atr=atr, support=support, resistance=resistance, reset_retest=True, reset_confirmation=True)

    return None
