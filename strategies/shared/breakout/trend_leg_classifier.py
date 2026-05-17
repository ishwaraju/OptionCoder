class TrendLegClassifier:
    @staticmethod
    def classify(
        strategy,
        *,
        scored_direction,
        candle_time,
        price,
        vwap,
        candle_open,
        candle_close,
        candle_high,
        candle_low,
        prev_high,
        prev_low,
        atr,
        buffer,
    ):
        if scored_direction not in {"CE", "PE"} or candle_time is None or price is None or vwap is None:
            return "NEUTRAL"

        aligned = (scored_direction == "CE" and price > vwap) or (scored_direction == "PE" and price < vwap)
        if not aligned:
            return "NEUTRAL"

        trigger_level = prev_high if scored_direction == "CE" else prev_low
        if trigger_level is not None:
            try:
                if strategy._entry_too_extended(scored_direction, candle_close or price, trigger_level, atr, buffer):
                    return "STRETCHED"
            except Exception:
                pass

        retest_zone = max(buffer or 0, 5)
        memory = getattr(strategy, "breakout_memory", None) or {}
        if (
            memory
            and memory.get("session_day") == candle_time.date()
            and memory.get("direction") == scored_direction
            and memory.get("level") is not None
        ):
            level = memory["level"]
            minutes_apart = int((candle_time - memory["time"]).total_seconds() // 60)
            if 0 <= minutes_apart <= 35:
                if (
                    scored_direction == "CE"
                    and candle_low is not None
                    and candle_close is not None
                    and candle_low <= level + retest_zone
                    and candle_close >= level
                ):
                    return "FIRST_RETEST"
                if (
                    scored_direction == "PE"
                    and candle_high is not None
                    and candle_close is not None
                    and candle_high >= level - retest_zone
                    and candle_close <= level
                ):
                    return "FIRST_RETEST"

        impulse_buffer = max((buffer or 0) * 0.25, 3)
        if scored_direction == "CE" and prev_high is not None and candle_close is not None:
            if candle_close > prev_high and (candle_open is None or candle_open <= prev_high + impulse_buffer):
                return "FIRST_IMPULSE"
        if scored_direction == "PE" and prev_low is not None and candle_close is not None:
            if candle_close < prev_low and (candle_open is None or candle_open >= prev_low - impulse_buffer):
                return "FIRST_IMPULSE"

        return "NEUTRAL"


def classify_trend_leg(strategy, **kwargs):
    return TrendLegClassifier.classify(strategy, **kwargs)
