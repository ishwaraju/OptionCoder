class FuturesAcceptanceEngine:
    @staticmethod
    def evaluate(
        *,
        direction,
        price,
        vwap,
        candle_open,
        candle_close,
        candle_high,
        candle_low,
        prev_high,
        prev_low,
        recent_candles_5m,
        buffer,
        atr,
    ):
        if direction not in {"CE", "PE"} or price is None or vwap is None:
            return {
                "label": "NEUTRAL",
                "score": 0.0,
                "initiative_strength_score": 0.0,
                "accepted": False,
                "reasons": [],
                "summary": "acceptance unavailable",
            }

        score = 0.0
        initiative = 0.0
        reasons = []
        accepted = False

        aligned_vwap = (direction == "CE" and price > vwap) or (direction == "PE" and price < vwap)
        if aligned_vwap:
            score += 18.0
            reasons.append("vwap_aligned")

        trigger_level = prev_high if direction == "CE" else prev_low
        trigger_hold = False
        if trigger_level is not None and candle_close is not None:
            if direction == "CE" and candle_close > trigger_level:
                trigger_hold = True
            elif direction == "PE" and candle_close < trigger_level:
                trigger_hold = True
        if trigger_hold:
            score += 24.0
            reasons.append("trigger_level_held")

        candle_range = max((candle_high or price) - (candle_low or price), 0.0)
        candle_body = abs((candle_close or price) - (candle_open or price))
        body_ratio = (candle_body / candle_range) if candle_range > 0 else 0.0
        if body_ratio >= 0.55:
            score += 14.0
            initiative += 18.0
            reasons.append("displacement_body")

        if direction == "CE" and candle_high is not None and candle_close is not None:
            if candle_high - candle_close <= max(buffer or 0, 3):
                score += 8.0
                initiative += 10.0
                reasons.append("closed_near_high")
        elif direction == "PE" and candle_low is not None and candle_close is not None:
            if candle_close - candle_low <= max(buffer or 0, 3):
                score += 8.0
                initiative += 10.0
                reasons.append("closed_near_low")

        recent = list(recent_candles_5m or [])[-4:]
        if len(recent) >= 2:
            closes = [float(item.get("close") or 0.0) for item in recent]
            move_progress = closes[-1] - closes[0]
            if direction == "PE":
                move_progress *= -1.0
            if move_progress > max(float(atr or 0.0) * 0.3, 12.0):
                score += 14.0
                initiative += 16.0
                reasons.append("multi_bar_progress")

            latest_pullback = 0.0
            if direction == "CE":
                latest_pullback = max(0.0, max(closes[:-1]) - closes[-1])
            else:
                latest_pullback = max(0.0, closes[-1] - min(closes[:-1]))
            if latest_pullback <= max((buffer or 0.0) * 1.25, 8.0):
                score += 10.0
                reasons.append("shallow_pullback")

        accepted = score >= 52.0 and aligned_vwap
        if accepted:
            initiative += 12.0

        label = "STRONG" if score >= 68 else "BUILDING" if score >= 52 else "WEAK"
        return {
            "label": label,
            "score": round(min(score, 100.0), 2),
            "initiative_strength_score": round(min(initiative, 100.0), 2),
            "accepted": accepted,
            "reasons": reasons,
            "summary": f"acceptance={label} | initiative={round(min(initiative, 100.0), 2)} | {'/'.join(reasons[:4])}",
        }


def evaluate_futures_acceptance(**kwargs):
    return FuturesAcceptanceEngine.evaluate(**kwargs)
