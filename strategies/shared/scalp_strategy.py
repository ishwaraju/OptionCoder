"""
Scalping Strategy for 1-minute candles
Quick momentum-based signals with low threshold
"""

from shared.utils.time_utils import TimeUtils


class ScalpStrategy:
    """
    Fast scalping strategy for 1-minute timeframe
    - Quick momentum detection
    - Low score threshold (30-40)
    - Fast entry/exit (3-5 min hold)
    """

    def __init__(self, min_score=45, min_atr=25, require_atr=True):
        self.time_utils = TimeUtils()
        self.last_score = 0
        self.last_score_components = []
        self.last_signal = None
        self.last_candle = None
        self.cooldown_count = 0
        self.min_score = min_score  # Higher threshold (45 vs 35)
        self.min_atr = min_atr  # Instrument-specific ATR threshold
        self.require_atr = require_atr  # Only trade in volatile markets

    def generate_signal(
        self,
        price,
        candle_1m,
        vwap,
        oi_bias="NEUTRAL",
        oi_trend="SIDEWAYS",
        volume_signal="NORMAL",
        pressure_bias="NEUTRAL",
    ):
        """
        Generate quick scalp signal based on 1m candle momentum
        
        Returns: (signal, score, reason)
            signal: 'CE' for call, 'PE' for put, None for no signal
            score: 0-100
            reason: description
        """
        self.last_score_components = []
        score = 0
        direction = None

        # Extract candle data
        candle_open = candle_1m.get("open", price)
        candle_high = candle_1m.get("high", price)
        candle_low = candle_1m.get("low", price)
        candle_close = candle_1m.get("close", price)
        candle_volume = candle_1m.get("volume", 0)
        atr_value = candle_1m.get("atr", 0)  # ATR for volatility check

        # Calculate candle body and range
        body = abs(candle_close - candle_open)
        candle_range = candle_high - candle_low
        
        # DATA-DRIVEN: Skip extremely small candles (based on 25th percentile analysis)
        # 25th percentile shows range=0, body=0 for many 1m candles - need reasonable minimum
        if candle_range < 3 or body < 2:  # DATA-DRIVEN: Was 8/5 (too strict), now 3/2
            return None, 0, f"Candle too small (range:{candle_range:.1f}, body:{body:.1f})"
        
        # STRICT: Skip if ATR too low (market dead)
        if self.require_atr and atr_value < self.min_atr:  # Use instrument-specific ATR threshold
            return None, 0, f"Low volatility (ATR:{atr_value:.1f} < min:{self.min_atr}) - avoid scalping"

        # Momentum detection (STRICT: Must be away from VWAP)
        vwap_distance = abs(candle_close - vwap) / vwap * 100 if vwap > 0 else 0
        
        bullish_momentum = (
            candle_close > candle_open and 
            candle_close > vwap and 
            vwap_distance > 0.08  # At least 0.08% away from VWAP
        )
        bearish_momentum = (
            candle_close < candle_open and 
            candle_close < vwap and 
            vwap_distance > 0.08
        )
        
        if not (bullish_momentum or bearish_momentum):
            return None, 0, f"No clear momentum (VWAP dist:{vwap_distance:.3f}%)"

        # Body strength (larger body = stronger momentum)
        body_strength = min(body / candle_range * 20, 15) if candle_range > 0 else 0

        if bullish_momentum:
            score += 25
            direction = "CE"
            self.last_score_components.append("bullish_momentum")
            score += body_strength
            if body_strength > 5:
                self.last_score_components.append("strong_body")

        elif bearish_momentum:
            score += 25
            direction = "PE"
            self.last_score_components.append("bearish_momentum")
            score += body_strength
            if body_strength > 5:
                self.last_score_components.append("strong_body")

        # VWAP deviation bonus
        vwap_deviation = abs(price - vwap) / vwap * 100 if vwap > 0 else 0
        if vwap_deviation > 0.05:  # More than 0.05% from VWAP
            score += min(vwap_deviation * 50, 10)
            self.last_score_components.append("vwap_deviation")

        # OI bias alignment
        if direction == "CE" and oi_bias == "BULLISH":
            score += 8
            self.last_score_components.append("oi_bullish_aligned")
        elif direction == "PE" and oi_bias == "BEARISH":
            score += 8
            self.last_score_components.append("oi_bearish_aligned")

        # Volume confirmation
        if volume_signal == "STRONG":
            score += 10
            self.last_score_components.append("strong_volume")
        elif volume_signal == "ABOVE_NORMAL":
            score += 5
            self.last_score_components.append("good_volume")

        # Pressure alignment
        if direction == "CE" and pressure_bias == "BULLISH":
            score += 7
            self.last_score_components.append("pressure_bullish")
        elif direction == "PE" and pressure_bias == "BEARISH":
            score += 7
            self.last_score_components.append("pressure_bearish")

        # Time-based adjustment (avoid scalp during lunch)
        current_time = self.time_utils.current_time()
        # Reduce score during lunch hours 12:00-13:30
        if (12, 0) <= (current_time.hour, current_time.minute) <= (13, 30):
            score *= 0.7
            self.last_score_components.append("lunch_penalty")

        self.last_score = int(score)

        # STRICT: Higher threshold for safer scalping
        if score < self.min_score:
            return None, self.last_score, f"Score {self.last_score} below threshold {self.min_score}"

        reason = f"Scalp {direction} | Score: {self.last_score} | " + ", ".join(self.last_score_components[:3])
        
        return direction, self.last_score, reason

    def get_last_score(self):
        return self.last_score

    def get_last_components(self):
        return self.last_score_components
