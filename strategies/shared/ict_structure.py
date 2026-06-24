from config import Config


def analyze_ict_structure(candles, direction=None, atr=None, buffer=0):
    if not getattr(Config, "ICT_FVG_ENABLED", True):
        return _empty_context()
    if not candles or len(candles) < 8:
        return _empty_context(reason="insufficient_candles")

    window = [c for c in candles[-30:] if _valid_candle(c)]
    if len(window) < 8:
        return _empty_context(reason="invalid_candles")

    latest = window[-1]
    expected = (direction or "").upper()
    bullish = _direction_context(window, "CE", atr=atr, buffer=buffer)
    bearish = _direction_context(window, "PE", atr=atr, buffer=buffer)
    selected = _select_context(bullish, bearish, expected)
    return selected or _empty_context(reason="no_ict_context")


def _direction_context(candles, direction, atr=None, buffer=0):
    latest = candles[-1]
    prior = candles[:-1]
    swing = _recent_swing(prior, direction)
    fvg = _latest_unfilled_fvg(candles, direction)
    displacement = _latest_displacement(candles, direction, atr=atr, buffer=buffer)
    mss = _market_structure_shift(candles, direction, buffer=buffer)
    sweep = _liquidity_sweep(candles, direction, swing=swing, buffer=buffer)
    in_zone = _price_in_fvg_zone(latest, fvg, direction)
    rejected = _fvg_rejection(latest, fvg, direction)
    fresh = fvg and fvg.get("age", 99) <= getattr(Config, "ICT_FVG_MAX_AGE_BARS", 8)

    score = 0
    reasons = []
    if sweep["present"]:
        score += 25 + min(int(sweep.get("strength", 0) / 20), 5)
        reasons.append("liquidity_sweep")
        if sweep.get("pool_type") in {"EQUAL_LOW", "EQUAL_HIGH"}:
            reasons.append("equal_liquidity_pool")
    if mss["present"]:
        score += 25
        reasons.append("mss")
    if displacement["present"]:
        score += 20
        reasons.append("displacement")
    if fvg:
        score += 15
        reasons.append("fvg")
    if in_zone:
        score += 8
        reasons.append("fvg_retest")
    if rejected:
        score += 12
        reasons.append("fvg_rejection")
    if fresh:
        score += 5
        reasons.append("fresh_fvg")

    quality = "NONE"
    if score >= 82 and sweep["present"] and mss["present"] and displacement["present"] and fvg and (in_zone or rejected):
        quality = "A"
    elif score >= 68 and mss["present"] and displacement["present"] and fvg:
        quality = "B"
    elif score >= 50 and (mss["present"] or sweep["present"]) and fvg:
        quality = "C"

    entry_zone = None
    invalidation = None
    if fvg:
        entry_zone = {"low": round(fvg["low"], 2), "high": round(fvg["high"], 2)}
        invalidation = round(fvg["low"], 2) if direction == "CE" else round(fvg["high"], 2)
    sweep_invalidation = sweep.get("extreme", sweep.get("level"))
    if sweep_invalidation is not None:
        if direction == "CE":
            invalidation = round(min(invalidation if invalidation is not None else sweep_invalidation, sweep_invalidation), 2)
        else:
            invalidation = round(max(invalidation if invalidation is not None else sweep_invalidation, sweep_invalidation), 2)

    return {
        "direction": direction,
        "quality": quality,
        "score": min(score, 100),
        "ready": quality in {"A", "B"},
        "action_ready": quality == "A",
        "sweep": sweep,
        "mss": mss,
        "displacement": displacement,
        "fvg": fvg,
        "entry_zone": entry_zone,
        "invalidation": invalidation,
        "in_fvg_zone": bool(in_zone),
        "fvg_rejection": bool(rejected),
        "fresh_fvg": bool(fresh),
        "reason": ",".join(reasons) if reasons else "no_ict_context",
        "summary": _summary(direction, quality, score, sweep, mss, displacement, fvg, in_zone, rejected),
    }


def _empty_context(reason="no_ict_context"):
    return {
        "direction": None,
        "quality": "NONE",
        "score": 0,
        "ready": False,
        "action_ready": False,
        "reason": reason,
        "summary": reason,
    }


def _valid_candle(candle):
    return all(candle.get(k) is not None for k in ("open", "high", "low", "close"))


def _body(candle):
    return abs(float(candle["close"]) - float(candle["open"]))


def _range(candle):
    return max(float(candle["high"]) - float(candle["low"]), 0.0)


def _recent_swing(candles, direction, lookback=6):
    if len(candles) < lookback:
        return None
    sample = candles[-lookback:]
    if direction == "CE":
        low_candle = min(sample, key=lambda c: float(c["low"]))
        return {"level": float(low_candle["low"]), "time": low_candle.get("time")}
    high_candle = max(sample, key=lambda c: float(c["high"]))
    return {"level": float(high_candle["high"]), "time": high_candle.get("time")}


def _liquidity_sweep(candles, direction, swing=None, buffer=0):
    if len(candles) < 6:
        return {"present": False}
    tolerance = _sweep_tolerance(buffer)
    start = max(4, len(candles) - 5)
    for idx in range(len(candles) - 1, start - 1, -1):
        candle = candles[idx]
        prior = candles[max(0, idx - 10):idx]
        if len(prior) < 4:
            continue
        pool = _liquidity_pool(prior, direction, tolerance)
        level = pool["level"] if pool else (
            min(float(c["low"]) for c in prior)
            if direction == "CE"
            else max(float(c["high"]) for c in prior)
        )
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        open_ = float(candle["open"])
        candle_range = max(high - low, 0.01)
        body = abs(close - open_)
        body_ratio = body / candle_range

        if direction == "CE":
            sweep_depth = level - low
            reclaimed = close > level
            rejection = close >= low + candle_range * 0.55
            if sweep_depth >= tolerance and reclaimed and rejection:
                return _build_sweep_result(candle, level, low, pool, sweep_depth, body_ratio, direction)
        else:
            sweep_depth = high - level
            rejected = close < level
            rejection = close <= high - candle_range * 0.55
            if sweep_depth >= tolerance and rejected and rejection:
                return _build_sweep_result(candle, level, high, pool, sweep_depth, body_ratio, direction)
    return {"present": False, "level": float(swing["level"]) if swing else None}


def _sweep_tolerance(buffer):
    return max(float(buffer or 0) * 0.15, 0.75)


def _liquidity_pool(candles, direction, tolerance):
    """
    Detect the pool institutions usually attack first: near-equal lows for CE
    reversal setups, near-equal highs for PE reversal setups.
    """
    if len(candles) < 4:
        return None
    sample = candles[-8:]
    values = [
        float(c["low"]) if direction == "CE" else float(c["high"])
        for c in sample
    ]
    target = min(values) if direction == "CE" else max(values)
    cluster = [value for value in values if abs(value - target) <= tolerance * 1.35]
    if len(cluster) >= 2:
        level = sum(cluster) / len(cluster)
        return {
            "level": level,
            "touches": len(cluster),
            "pool_type": "EQUAL_LOW" if direction == "CE" else "EQUAL_HIGH",
        }
    return {
        "level": target,
        "touches": 1,
        "pool_type": "SWING_LOW" if direction == "CE" else "SWING_HIGH",
    }


def _build_sweep_result(candle, level, extreme, pool, sweep_depth, body_ratio, direction):
    touches = int((pool or {}).get("touches") or 1)
    strength = 45
    strength += min(25, sweep_depth * 3)
    strength += min(15, touches * 4)
    strength += 10 if body_ratio >= 0.45 else 0
    quality = "STRONG" if strength >= 64 and touches >= 2 else "OK"
    return {
        "present": True,
        "level": round(level, 2),
        "extreme": round(extreme, 2),
        "time": candle.get("time"),
        "pool_type": (pool or {}).get("pool_type"),
        "touches": touches,
        "depth": round(sweep_depth, 2),
        "strength": round(min(strength, 100), 1),
        "quality": quality,
        "side": "SELL_SIDE" if direction == "CE" else "BUY_SIDE",
    }


def _market_structure_shift(candles, direction, buffer=0):
    if len(candles) < 6:
        return {"present": False}
    prior = candles[-6:-1]
    latest = candles[-1]
    tolerance = max(float(buffer or 0) * 0.1, 0.5)
    if direction == "CE":
        level = max(float(c["high"]) for c in prior[:-1])
        present = float(latest["close"]) > level + tolerance
    else:
        level = min(float(c["low"]) for c in prior[:-1])
        present = float(latest["close"]) < level - tolerance
    return {"present": present, "level": round(level, 2), "time": latest.get("time") if present else None}


def _latest_displacement(candles, direction, atr=None, buffer=0):
    recent = candles[-3:]
    ranges = [_range(c) for c in candles[-10:-1]]
    avg_range = sum(ranges) / len(ranges) if ranges else 0.0
    min_range = max(float(atr or 0) * 0.45, avg_range * 1.25, float(buffer or 0) * 1.4, 4.0)
    for candle in reversed(recent):
        candle_range = _range(candle)
        if candle_range <= 0:
            continue
        body_ratio = _body(candle) / candle_range
        bullish = float(candle["close"]) > float(candle["open"])
        bearish = float(candle["close"]) < float(candle["open"])
        if candle_range >= min_range and body_ratio >= 0.55:
            if (direction == "CE" and bullish) or (direction == "PE" and bearish):
                return {
                    "present": True,
                    "time": candle.get("time"),
                    "range": round(candle_range, 2),
                    "body_ratio": round(body_ratio, 2),
                }
    if len(candles) >= 3:
        leg = candles[-3:-1]
        same_direction = all(
            (float(c["close"]) > float(c["open"])) if direction == "CE" else (float(c["close"]) < float(c["open"]))
            for c in leg
        )
        if same_direction:
            leg_range = max(float(c["high"]) for c in leg) - min(float(c["low"]) for c in leg)
            leg_body = sum(_body(c) for c in leg)
            if leg_range >= min_range and leg_body / max(leg_range, 0.01) >= 0.55:
                return {
                    "present": True,
                    "time": leg[-1].get("time"),
                    "range": round(leg_range, 2),
                    "body_ratio": round(leg_body / max(leg_range, 0.01), 2),
                }
    return {"present": False}


def _latest_unfilled_fvg(candles, direction):
    max_age = getattr(Config, "ICT_FVG_MAX_AGE_BARS", 8)
    for idx in range(len(candles) - 1, 1, -1):
        first = candles[idx - 2]
        third = candles[idx]
        if direction == "CE" and float(third["low"]) > float(first["high"]):
            zone = {"low": float(first["high"]), "high": float(third["low"]), "created_at": third.get("time"), "age": len(candles) - 1 - idx}
        elif direction == "PE" and float(third["high"]) < float(first["low"]):
            zone = {"low": float(third["high"]), "high": float(first["low"]), "created_at": third.get("time"), "age": len(candles) - 1 - idx}
        else:
            continue
        if zone["age"] > max_age:
            return None
        if not _fvg_filled(candles[idx + 1:], zone, direction):
            zone["size"] = round(zone["high"] - zone["low"], 2)
            return zone
    return None


def _fvg_filled(candles, zone, direction):
    if not candles:
        return False
    if direction == "CE":
        return any(float(c["low"]) <= zone["low"] for c in candles)
    return any(float(c["high"]) >= zone["high"] for c in candles)


def _price_in_fvg_zone(candle, fvg, direction):
    if not fvg:
        return False
    low = float(candle["low"])
    high = float(candle["high"])
    return high >= fvg["low"] and low <= fvg["high"]


def _fvg_rejection(candle, fvg, direction):
    if not fvg or not _price_in_fvg_zone(candle, fvg, direction):
        return False
    close = float(candle["close"])
    open_ = float(candle["open"])
    midpoint = (fvg["low"] + fvg["high"]) / 2.0
    if direction == "CE":
        return close >= midpoint and close > open_
    return close <= midpoint and close < open_


def _select_context(bullish, bearish, expected):
    if expected == "CE":
        return bullish
    if expected == "PE":
        return bearish
    return bullish if bullish.get("score", 0) >= bearish.get("score", 0) else bearish


def _summary(direction, quality, score, sweep, mss, displacement, fvg, in_zone, rejected):
    parts = [f"ICT {direction or '-'} {quality}", f"score={min(score, 100)}"]
    if sweep.get("present"):
        pool = sweep.get("pool_type") or "pool"
        parts.append(f"liquidity sweep {pool} strength={sweep.get('strength')}")
    if mss.get("present"):
        parts.append(f"MSS {mss.get('level')}")
    if displacement.get("present"):
        parts.append("displacement")
    if fvg:
        parts.append(f"FVG {round(fvg['low'], 2)}-{round(fvg['high'], 2)}")
    if in_zone:
        parts.append("retest")
    if rejected:
        parts.append("rejection")
    return " | ".join(parts)
