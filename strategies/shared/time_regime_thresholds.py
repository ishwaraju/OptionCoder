from config import Config


def build_time_regime_thresholds(instrument, time_regime, fallback_mode, market_regime=None):
    """Return score thresholds for the current instrument/session regime."""
    instrument = (instrument or "NIFTY").upper()
    market_regime = (market_regime or "").upper()

    thresholds = {
        "opening_drive_min_score": 72,
        "breakout_min_score": Config.MIN_SCORE_THRESHOLD,
        "confirm_min_score": 62,
        "continuation_min_score": 60 if fallback_mode else 65,
        "high_continuation_min_score": 75,
        "retest_min_score": Config.MIN_SCORE_THRESHOLD,
        "reversal_min_score": 58 if fallback_mode else 55,
        "allow_continuation": True,
        "allow_fallback_continuation": True,
        "allow_weak_volume_watch": False,
        "allow_mild_pressure_watch": False,
    }

    if instrument == "BANKNIFTY":
        thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 58)
        thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 60)
    elif instrument == "SENSEX":
        thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 57)
        thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 61)
        thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 57)
        thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 58)
        thresholds["high_continuation_min_score"] = max(thresholds["high_continuation_min_score"], 72)
        thresholds["allow_weak_volume_watch"] = True
        thresholds["allow_mild_pressure_watch"] = True
    elif instrument == "NIFTY":
        thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 64)

    if instrument == "SENSEX" and Config.FOCUSED_MANUAL_MODE:
        thresholds["breakout_min_score"] = max(53, thresholds["breakout_min_score"] - 4)
        thresholds["confirm_min_score"] = max(57, thresholds["confirm_min_score"] - 4)
        thresholds["retest_min_score"] = max(54, thresholds["retest_min_score"] - 3)
        thresholds["continuation_min_score"] = max(56, thresholds["continuation_min_score"] - 2)
        thresholds["high_continuation_min_score"] = max(70, thresholds["high_continuation_min_score"] - 2)

    if time_regime == "OPENING":
        thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 60 if fallback_mode else 58)
        thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 68)
    elif time_regime == "MIDDAY":
        thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 63 if fallback_mode else 60)
        thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 64)
        thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 72)
        thresholds["high_continuation_min_score"] = max(thresholds["high_continuation_min_score"], 80)
        thresholds["allow_fallback_continuation"] = False
    elif time_regime == "LATE_DAY":
        thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 60 if fallback_mode else 58)
        thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 62 if fallback_mode else 65)
        thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 60 if instrument == "BANKNIFTY" else 58)
    elif time_regime == "ENDGAME":
        thresholds["breakout_min_score"] = max(thresholds["breakout_min_score"], 65)
        thresholds["confirm_min_score"] = max(thresholds["confirm_min_score"], 66)
        thresholds["continuation_min_score"] = max(thresholds["continuation_min_score"], 78)
        thresholds["high_continuation_min_score"] = max(thresholds["high_continuation_min_score"], 82)
        thresholds["retest_min_score"] = max(thresholds["retest_min_score"], 60)
        thresholds["allow_continuation"] = False
        thresholds["allow_fallback_continuation"] = False

    if Config.ADAPTIVE_THRESHOLDS_ENABLED:
        relax_score = max(0.0, float(Config.ADAPTIVE_THRESHOLD_RELAX_SCORE))
        tighten_score = max(0.0, float(Config.ADAPTIVE_THRESHOLD_TIGHTEN_SCORE))

        if market_regime in {"TRENDING", "EXPANDING", "OPENING_EXPANSION"}:
            thresholds["breakout_min_score"] = max(52, thresholds["breakout_min_score"] - relax_score)
            thresholds["confirm_min_score"] = max(58, thresholds["confirm_min_score"] - min(relax_score, 1.0))
            thresholds["continuation_min_score"] = max(60, thresholds["continuation_min_score"] - relax_score)
            thresholds["high_continuation_min_score"] = max(72, thresholds["high_continuation_min_score"] - relax_score)
            thresholds["retest_min_score"] = max(54, thresholds["retest_min_score"] - min(relax_score, 1.0))
        elif market_regime in {"RANGING", "CHOPPY"}:
            thresholds["breakout_min_score"] += tighten_score
            thresholds["confirm_min_score"] += min(tighten_score, 2.0)
            thresholds["continuation_min_score"] += tighten_score + 2
            thresholds["high_continuation_min_score"] += tighten_score + 2
            thresholds["retest_min_score"] += min(tighten_score, 2.0)
            thresholds["allow_fallback_continuation"] = False

    return thresholds
