from shared.utils.time_utils import TimeUtils


class OneMinuteMomentumQuality:
    """1m option-buyer momentum quality check used by entry watch confirmation."""

    def __init__(self, min_score=30):
        self.time_utils = TimeUtils()
        self.min_score = min_score

    @staticmethod
    def _rolling_vwap(candles):
        total_value = 0.0
        total_volume = 0.0
        for candle in candles or []:
            volume = float(candle.get("volume") or 0)
            if volume <= 0:
                continue
            typical_price = (
                float(candle.get("high") or candle.get("close") or 0)
                + float(candle.get("low") or candle.get("close") or 0)
                + float(candle.get("close") or 0)
            ) / 3.0
            total_value += typical_price * volume
            total_volume += volume
        return total_value / total_volume if total_volume > 0 else None

    @staticmethod
    def _avg_true_range(candles):
        if not candles or len(candles) < 2:
            return None
        ranges = []
        for idx in range(1, len(candles)):
            high = float(candles[idx].get("high") or 0)
            low = float(candles[idx].get("low") or 0)
            prev_close = float(candles[idx - 1].get("close") or 0)
            ranges.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        return sum(ranges) / len(ranges) if ranges else None

    def evaluate(
        self,
        direction,
        recent_1m_candles,
        vwap=None,
        volume_signal=None,
        oi_bias="NEUTRAL",
        pressure_bias="NEUTRAL",
    ):
        if not recent_1m_candles:
            return {"ok": False, "score": 0, "reason": "No 1m candle data"}

        latest = recent_1m_candles[-1]
        candle_open = float(latest.get("open") or latest.get("close") or 0)
        candle_high = float(latest.get("high") or latest.get("close") or 0)
        candle_low = float(latest.get("low") or latest.get("close") or 0)
        candle_close = float(latest.get("close") or 0)
        body = abs(candle_close - candle_open)
        candle_range = candle_high - candle_low
        if candle_range <= 0:
            return {"ok": False, "score": 0, "reason": "1m candle has no range"}

        score = 0
        components = []
        if candle_range >= 3 and body >= 2:
            score += 10
            components.append("range_body_ok")
        else:
            return {
                "ok": False,
                "score": 0,
                "reason": f"1m candle too small (range:{candle_range:.1f}, body:{body:.1f})",
            }

        bullish_body = candle_close >= candle_open
        bearish_body = candle_close <= candle_open
        if (direction == "CE" and bullish_body) or (direction == "PE" and bearish_body):
            score += 20
            components.append("body_aligned")
        else:
            return {"ok": False, "score": score, "reason": "1m candle body against trigger direction"}

        vwap_value = vwap or self._rolling_vwap(recent_1m_candles)
        if vwap_value and vwap_value > 0:
            above_vwap = candle_close > vwap_value
            below_vwap = candle_close < vwap_value
            vwap_distance = abs(candle_close - vwap_value) / vwap_value * 100
            if (direction == "CE" and above_vwap) or (direction == "PE" and below_vwap):
                score += 12
                components.append("vwap_aligned")
            if vwap_distance >= 0.03:
                score += min(vwap_distance * 80, 10)
                components.append("vwap_distance")

        atr = self._avg_true_range(recent_1m_candles[-6:])
        if atr is not None:
            if candle_range >= atr * 0.75:
                score += 8
                components.append("atr_expansion")
            elif candle_range < max(atr * 0.35, 2):
                score -= 8
                components.append("atr_compression")

        if volume_signal == "STRONG":
            score += 10
            components.append("strong_volume")
        elif volume_signal == "ABOVE_NORMAL":
            score += 5
            components.append("good_volume")

        if (direction == "CE" and oi_bias == "BULLISH") or (direction == "PE" and oi_bias == "BEARISH"):
            score += 6
            components.append("oi_aligned")
        if (direction == "CE" and pressure_bias == "BULLISH") or (direction == "PE" and pressure_bias == "BEARISH"):
            score += 6
            components.append("pressure_aligned")

        current_time = self.time_utils.current_time()
        if (12, 0) <= (current_time.hour, current_time.minute) <= (13, 30):
            score *= 0.85
            components.append("lunch_penalty")

        final_score = int(max(0, min(100, score)))
        reason = "1m momentum " + ", ".join(components[:4])
        return {"ok": final_score >= self.min_score, "score": final_score, "reason": reason}
