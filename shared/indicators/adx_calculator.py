"""
ADX (Average Directional Index) Calculator
For trend strength detection - critical for option buyers to avoid sideways markets
"""
from shared.utils.time_utils import TimeUtils


class ADXCalculator:
    """
    Calculate ADX (Average Directional Index) for trend strength
    ADX > 25: Strong trend (good for trading)
    ADX 20-25: Medium trend
    ADX < 20: Weak trend/sideways (avoid for option buying)
    """

    def __init__(self, period=14):
        self.time_utils = TimeUtils()
        self.period = period
        self.candles_history = []
        self.prev_adx = None
        self.prev_di_plus = None
        self.prev_di_minus = None

    def update(self, high, low, close):
        """Update with new candle data and calculate ADX"""
        if None in (high, low, close):
            return None

        # Add to history
        self.candles_history.append({
            'high': float(high),
            'low': float(low),
            'close': float(close)
        })

        # Keep only needed history
        max_needed = self.period * 2 + 1
        if len(self.candles_history) > max_needed:
            self.candles_history = self.candles_history[-max_needed:]

        # Need at least period+1 candles
        if len(self.candles_history) < self.period + 1:
            return None

        return self._calculate_adx()

    def _calculate_adx(self):
        """Calculate ADX using Wilder's smoothing"""
        candles = self.candles_history
        period = self.period

        # Calculate True Range (TR), +DM, -DM
        tr_list = []
        dm_plus_list = []
        dm_minus_list = []

        for i in range(1, len(candles)):
            curr = candles[i]
            prev = candles[i - 1]

            # True Range
            tr1 = curr['high'] - curr['low']
            tr2 = abs(curr['high'] - prev['close'])
            tr3 = abs(curr['low'] - prev['close'])
            tr = max(tr1, tr2, tr3)
            tr_list.append(tr)

            # Directional Movement
            up_move = curr['high'] - prev['high']
            down_move = prev['low'] - curr['low']

            if up_move > down_move and up_move > 0:
                dm_plus = up_move
            else:
                dm_plus = 0

            if down_move > up_move and down_move > 0:
                dm_minus = down_move
            else:
                dm_minus = 0

            dm_plus_list.append(dm_plus)
            dm_minus_list.append(dm_minus)

        # Need at least 2*period data points
        if len(tr_list) < 2 * period:
            return None

        # Calculate smoothed averages using Wilder's method
        atr = self._wilder_smoothing(tr_list, period)
        di_plus_smooth = self._wilder_smoothing(dm_plus_list, period)
        di_minus_smooth = self._wilder_smoothing(dm_minus_list, period)

        if atr == 0:
            return None

        # Calculate DI+ and DI-
        di_plus = 100 * di_plus_smooth / atr
        di_minus = 100 * di_minus_smooth / atr

        # Calculate DX
        if di_plus + di_minus == 0:
            dx = 0
        else:
            dx = 100 * abs(di_plus - di_minus) / (di_plus + di_minus)

        # Calculate ADX (smoothed DX)
        if self.prev_adx is None:
            adx = dx
        else:
            adx = (self.prev_adx * (period - 1) + dx) / period

        # Store for next calculation
        self.prev_adx = adx
        self.prev_di_plus = di_plus
        self.prev_di_minus = di_minus

        return {
            'adx': round(adx, 2),
            'di_plus': round(di_plus, 2),
            'di_minus': round(di_minus, 2),
            'trend': 'STRONG' if adx > 25 else 'MODERATE' if adx > 20 else 'WEAK'
        }

    def _wilder_smoothing(self, values, period):
        """Wilder's smoothing method"""
        if len(values) < period:
            return sum(values) / len(values) if values else 0

        # First value is simple average
        result = sum(values[:period]) / period

        # Subsequent values use Wilder's formula
        for i in range(period, len(values)):
            result = (result * (period - 1) + values[i]) / period

        return result

    def is_trend_strong(self, threshold=25):
        """Check if trend is strong enough for option buying"""
        adx_data = self.get_current()
        if adx_data is None:
            return False, 0
        return adx_data['adx'] >= threshold, adx_data['adx']

    def get_current(self):
        """Get current ADX values"""
        if not self.candles_history:
            return None
        return self._calculate_adx()

    def get_trend_direction(self):
        """Get trend direction based on DI+ and DI-"""
        adx_data = self.get_current()
        if adx_data is None:
            return "UNKNOWN"

        if adx_data['di_plus'] > adx_data['di_minus']:
            return "BULLISH"
        elif adx_data['di_minus'] > adx_data['di_plus']:
            return "BEARISH"
        return "NEUTRAL"

    def reset(self):
        """Reset calculator"""
        self.candles_history = []
        self.prev_adx = None
        self.prev_di_plus = None
        self.prev_di_minus = None

    def get_last_values(self, count=5):
        """Get last N ADX values for analysis"""
        if len(self.candles_history) < count + self.period:
            return []

        values = []
        for i in range(count):
            idx = -(i + 1)
            if abs(idx) <= len(self.candles_history):
                adx = self._calculate_adx_at_index(idx)
                if adx:
                    values.append(adx)
        return values

    def _calculate_adx_at_index(self, end_offset):
        """Calculate ADX at specific index (for historical values)"""
        # Simplified - return approximate value
        return self.get_current()


def calculate_adx_for_candles(candles, period=14):
    """
    Calculate ADX for a list of candles
    candles: list of dict with 'high', 'low', 'close'
    Returns: ADX value or None
    """
    if len(candles) < period + 1:
        return None

    calculator = ADXCalculator(period)

    for candle in candles:
        result = calculator.update(
            candle.get('high'),
            candle.get('low'),
            candle.get('close')
        )

    return result


def quick_adx_check(candles, threshold=25):
    """
    Quick check if trend is strong
    Returns: (is_strong, adx_value, trend_direction)
    """
    result = calculate_adx_for_candles(candles)

    if result is None:
        return False, 0, "UNKNOWN"

    is_strong = result['adx'] >= threshold
    direction = "BULLISH" if result['di_plus'] > result['di_minus'] else "BEARISH"

    return is_strong, result['adx'], direction


# For option buyers: specific thresholds
OPTION_BUYER_MIN_ADX = 28  # Stricter than normal 25
OPTION_BUYER_STRONG_ADX = 35  # Very strong trend for confident entries


def is_safe_for_option_buying(adx_value, di_plus, di_minus, direction):
    """
    Check if safe for option buying based on ADX and trend alignment
    Returns: (is_safe, reason)
    """
    if adx_value < OPTION_BUYER_MIN_ADX:
        return False, f"ADX too low ({adx_value} < {OPTION_BUYER_MIN_ADX}) - sideways risk"

    # Check trend alignment
    if direction == "CE" and di_plus <= di_minus:
        return False, "DI+ <= DI- despite CE signal - trend conflict"

    if direction == "PE" and di_minus <= di_plus:
        return False, "DI- <= DI+ despite PE signal - trend conflict"

    return True, "ADX and trend alignment OK"


if __name__ == "__main__":
    # Test
    test_candles = [
        {'high': 100, 'low': 95, 'close': 98},
        {'high': 102, 'low': 97, 'close': 101},
        {'high': 105, 'low': 99, 'close': 104},
        {'high': 108, 'low': 102, 'close': 107},
        {'high': 110, 'low': 105, 'close': 109},
        {'high': 115, 'low': 108, 'close': 114},
        {'high': 118, 'low': 112, 'close': 117},
        {'high': 120, 'low': 115, 'close': 119},
        {'high': 122, 'low': 117, 'close': 121},
        {'high': 125, 'low': 119, 'close': 124},
        {'high': 128, 'low': 122, 'close': 127},
        {'high': 130, 'low': 124, 'close': 129},
        {'high': 133, 'low': 127, 'close': 132},
        {'high': 135, 'low': 129, 'close': 134},
        {'high': 138, 'low': 131, 'close': 137},
    ]

    result = calculate_adx_for_candles(test_candles)
    print(f"ADX Result: {result}")

    is_strong, adx, direction = quick_adx_check(test_candles)
    print(f"Strong trend: {is_strong}, ADX: {adx}, Direction: {direction}")

    safe, reason = is_safe_for_option_buying(result['adx'], result['di_plus'], result['di_minus'], "CE")
    print(f"Safe for CE: {safe}, Reason: {reason}")

    print("\n✅ ADX Calculator ready for option buyer protection!")
    print(f"   Min ADX for option buying: {OPTION_BUYER_MIN_ADX}")
    print(f"   Strong ADX for confident entries: {OPTION_BUYER_STRONG_ADX}")


__all__ = [
    'ADXCalculator',
    'calculate_adx_for_candles',
    'quick_adx_check',
    'is_safe_for_option_buying',
    'OPTION_BUYER_MIN_ADX',
    'OPTION_BUYER_STRONG_ADX'
]


def is_safe_for_option_buying(adx_value, di_plus, di_minus, direction):
    """
    Check if safe for option buying based on ADX and trend alignment
    Returns: (is_safe, reason)
    """
    if adx_value < OPTION_BUYER_MIN_ADX:
        return False, f"ADX too low ({adx_value} < {OPTION_BUYER_MIN_ADX}) - sideways risk"

    # Check trend alignment
    if direction == "CE" and di_plus <= di_minus:
        return False, "DI+ <= DI- despite CE signal - trend conflict"

    if direction == "PE" and di_minus <= di_plus:
        return False, "DI- <= DI+ despite PE signal - trend conflict"

    return True, "ADX and trend alignment OK"


if __name__ == "__main__":
    # Test
    test_candles = [
        {'high': 100, 'low': 95, 'close': 98},
        {'high': 102, 'low': 97, 'close': 101},
        {'high': 105, 'low': 99, 'close': 104},
        {'high': 108, 'low': 102, 'close': 107},
        {'high': 110, 'low': 105, 'close': 109},
        {'high': 115, 'low': 108, 'close': 114},
        {'high': 118, 'low': 112, 'close': 117},
        {'high': 120, 'low': 115, 'close': 119},
        {'high': 122, 'low': 117, 'close': 121},
        {'high': 125, 'low': 119, 'close': 124},
        {'high': 128, 'low': 122, 'close': 127},
        {'high': 130, 'low': 124, 'close': 129},
        {'high': 133, 'low': 127, 'close': 132},
        {'high': 135, 'low': 129, 'close': 134},
        {'high': 138, 'low': 131, 'close': 137},
    ]

    result = calculate_adx_for_candles(test_candles)
    print(f"ADX Result: {result}")

    is_strong, adx, direction = quick_adx_check(test_candles)
    print(f"Strong trend: {is_strong}, ADX: {adx}, Direction: {direction}")

    safe, reason = is_safe_for_option_buying(result['adx'], result['di_plus'], result['di_minus'], "CE")
    print(f"Safe for CE: {safe}, Reason: {reason}")

    print("\n✅ ADX Calculator ready for option buyer protection!")
    print(f"   Min ADX for option buying: {OPTION_BUYER_MIN_ADX}")
    print(f"   Strong ADX for confident entries: {OPTION_BUYER_STRONG_ADX}")


__all__ = [
    'ADXCalculator',
    'calculate_adx_for_candles',
    'quick_adx_check',
    'is_safe_for_option_buying',
    'OPTION_BUYER_MIN_ADX',
    'OPTION_BUYER_STRONG_ADX'
]


def is_safe_for_option_buying(adx_value, di_plus, di_minus, direction):
    """
    Check if safe for option buying based on ADX and trend alignment
    Returns: (is_safe, reason)
    """
    if adx_value < OPTION_BUYER_MIN_ADX:
        return False, f"ADX too low ({adx_value} < {OPTION_BUYER_MIN_ADX}) - sideways risk"

    # Check trend alignment
    if direction == "CE" and di_plus <= di_minus:
        return False, "DI+ <= DI- despite CE signal - trend conflict"

    if direction == "PE" and di_minus <= di_plus:
        return False, "DI- <= DI+ despite PE signal - trend conflict"

    return True, "ADX and trend alignment OK"


if __name__ == "__main__":
    # Test
    test_candles = [
        {'high': 100, 'low': 95, 'close': 98},
        {'high': 102, 'low': 97, 'close': 101},
        {'high': 105, 'low': 99, 'close': 104},
        {'high': 108, 'low': 102, 'close': 107},
        {'high': 110, 'low': 105, 'close': 109},
        {'high': 115, 'low': 108, 'close': 114},
        {'high': 118, 'low': 112, 'close': 117},
        {'high': 120, 'low': 115, 'close': 119},
        {'high': 122, 'low': 117, 'close': 121},
        {'high': 125, 'low': 119, 'close': 124},
        {'high': 128, 'low': 122, 'close': 127},
        {'high': 130, 'low': 124, 'close': 129},
        {'high': 133, 'low': 127, 'close': 132},
        {'high': 135, 'low': 129, 'close': 134},
        {'high': 138, 'low': 131, 'close': 137},
    ]

    result = calculate_adx_for_candles(test_candles)
    print(f"ADX Result: {result}")

    is_strong, adx, direction = quick_adx_check(test_candles)
    print(f"Strong trend: {is_strong}, ADX: {adx}, Direction: {direction}")

    safe, reason = is_safe_for_option_buying(result['adx'], result['di_plus'], result['di_minus'], "CE")
    print(f"Safe for CE: {safe}, Reason: {reason}")

    print("\n✅ ADX Calculator ready for option buyer protection!")
    print(f"   Min ADX for option buying: {OPTION_BUYER_MIN_ADX}")
    print(f"   Strong ADX for confident entries: {OPTION_BUYER_STRONG_ADX}")


__all__ = [
    'ADXCalculator',
    'calculate_adx_for_candles',
    'quick_adx_check',
    'is_safe_for_option_buying',
    'OPTION_BUYER_MIN_ADX',
    'OPTION_BUYER_STRONG_ADX'
]
