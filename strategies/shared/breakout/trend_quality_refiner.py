class TrendQualityRefiner:
    @staticmethod
    def is_clean_trend_leg(
        *,
        scored_direction,
        day_state,
        price,
        vwap,
        volume_signal,
        breakout_body_ok,
        breakout_structure_ok,
        candle_liquidity_ok,
        pressure_conflict_level,
        opposite_pressure_present,
        adx_trade_ok,
        mtf_trade_ok,
        score,
        entry_score,
    ):
        aligned_day_state = (
            (day_state or {}).get("state") in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
            and (day_state or {}).get("direction") == scored_direction
        )
        if scored_direction not in {"CE", "PE"}:
            return False
        if not aligned_day_state:
            return False
        if price is None or vwap is None:
            return False
        if scored_direction == "CE" and price <= vwap:
            return False
        if scored_direction == "PE" and price >= vwap:
            return False
        if volume_signal not in {"NORMAL", "STRONG"}:
            return False
        if not (breakout_body_ok and breakout_structure_ok and candle_liquidity_ok):
            return False
        if pressure_conflict_level not in {"NONE", "MILD"}:
            return False
        if opposite_pressure_present:
            return False
        if not adx_trade_ok and float(score or 0) < 84:
            return False
        if not mtf_trade_ok and float(entry_score or score or 0) < 86:
            return False
        return float(score or 0) >= 78 and float(entry_score or score or 0) >= 76

    @staticmethod
    def refine_cautions(cautions, *, clean_trend_leg):
        cautions = list(cautions or [])
        if not clean_trend_leg:
            return cautions
        removable = {
            "far_from_vwap",
            "participation_baseline_weak",
            "pressure_neutral",
            "adx_not_confirmed",
            "higher_tf_not_aligned",
        }
        return [caution for caution in cautions if caution not in removable]


def is_clean_trend_leg(**kwargs):
    return TrendQualityRefiner.is_clean_trend_leg(**kwargs)


def refine_cautions(cautions, *, clean_trend_leg):
    return TrendQualityRefiner.refine_cautions(cautions, clean_trend_leg=clean_trend_leg)
