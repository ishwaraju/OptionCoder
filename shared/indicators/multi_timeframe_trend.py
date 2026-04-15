"""
Multi-Timeframe Trend Analysis
Confirm signals across timeframes - critical for option buyers
15m trend aligns with 5m signal = High conviction
"""
from shared.utils.time_utils import TimeUtils


class MultiTimeframeTrend:
    """
    Analyze trend across multiple timeframes
    Higher timeframe confirms lower timeframe signal
    """

    def __init__(self):
        self.time_utils = TimeUtils()
        self.trend_5m = None
        self.trend_15m = None
        self.trend_30m = None

    def update_trends(self, trend_5m, trend_15m=None, trend_30m=None):
        """Update trend data from different timeframes"""
        self.trend_5m = trend_5m
        self.trend_15m = trend_15m
        self.trend_30m = trend_30m

    def check_alignment(self, signal):
        """
        Check if signal aligns with higher timeframe trends
        Returns: (aligned, strength, reason)
        """
        if self.trend_5m is None:
            return False, 0, "No 5m trend data"

        # Determine signal direction
        signal_bullish = signal == "CE"
        signal_bearish = signal == "PE"

        # Check 5m trend alignment
        trend_5m_bullish = self.trend_5m in ['BULLISH', 'STRONG_BULLISH']
        trend_5m_bearish = self.trend_5m in ['BEARISH', 'STRONG_BEARISH']

        alignment_5m = (signal_bullish and trend_5m_bullish) or (signal_bearish and trend_5m_bearish)

        if not alignment_5m:
            return False, 0, f"5m trend ({self.trend_5m}) against signal ({signal})"

        # If only 5m available
        if self.trend_15m is None:
            return True, 50, "5m aligned, 15m data unavailable"

        # Check 15m alignment
        trend_15m_bullish = self.trend_15m in ['BULLISH', 'STRONG_BULLISH']
        trend_15m_bearish = self.trend_15m in ['BEARISH', 'STRONG_BEARISH']

        alignment_15m = (signal_bullish and trend_15m_bullish) or (signal_bearish and trend_15m_bearish)

        if not alignment_15m:
            return False, 20, f"15m trend ({self.trend_15m}) against signal ({signal})"

        # If 30m available
        if self.trend_30m is None:
            return True, 75, "5m and 15m aligned ✅"

        # Check 30m alignment
        trend_30m_bullish = self.trend_30m in ['BULLISH', 'STRONG_BULLISH']
        trend_30m_bearish = self.trend_30m in ['BEARISH', 'STRONG_BEARISH']

        alignment_30m = (signal_bullish and trend_30m_bullish) or (signal_bearish and trend_30m_bearish)

        if not alignment_30m:
            return True, 60, "5m and 15m aligned, but 30m against ⚠️"

        return True, 100, "All timeframes aligned ✅✅"

    def get_trend_score(self):
        """Get overall trend score based on timeframes"""
        score = 0
        reasons = []

        if self.trend_5m:
            score += 30
            reasons.append(f"5m: {self.trend_5m}")

        if self.trend_15m:
            score += 35
            reasons.append(f"15m: {self.trend_15m}")

        if self.trend_30m:
            score += 35
            reasons.append(f"30m: {self.trend_30m}")

        return score, reasons

    def is_trend_consistent(self):
        """Check if all timeframes show consistent trend direction"""
        if self.trend_5m is None:
            return False, "No data"

        # Determine direction from 5m
        is_bullish_5m = self.trend_5m in ['BULLISH', 'STRONG_BULLISH']
        is_bearish_5m = self.trend_5m in ['BEARISH', 'STRONG_BEARISH']

        if not (is_bullish_5m or is_bearish_5m):
            return False, "5m trend unclear"

        expected_direction = "BULLISH" if is_bullish_5m else "BEARISH"

        # Check 15m
        if self.trend_15m:
            is_aligned_15m = (expected_direction == "BULLISH" and self.trend_15m in ['BULLISH', 'STRONG_BULLISH']) or \
                           (expected_direction == "BEARISH" and self.trend_15m in ['BEARISH', 'STRONG_BEARISH'])
            if not is_aligned_15m:
                return False, f"15m contradicts 5m ({expected_direction})"

        # Check 30m
        if self.trend_30m:
            is_aligned_30m = (expected_direction == "BULLISH" and self.trend_30m in ['BULLISH', 'STRONG_BULLISH']) or \
                           (expected_direction == "BEARISH" and self.trend_30m in ['BEARISH', 'STRONG_BEARISH'])
            if not is_aligned_30m:
                return False, f"30m contradicts 5m ({expected_direction})"

        return True, f"All aligned: {expected_direction}"


def calculate_trend_from_candles(candles, lookback=5):
    """
    Calculate trend direction from candles
    Returns: trend string
    """
    if len(candles) < lookback:
        return "INSUFFICIENT_DATA"

    recent = candles[-lookback:]
    first_close = recent[0].get('close', recent[0][4] if isinstance(recent[0], (list, tuple)) else 0)
    last_close = recent[-1].get('close', recent[-1][4] if isinstance(recent[-1], (list, tuple)) else 0)

    if first_close == 0:
        return "UNKNOWN"

    change = ((last_close - first_close) / first_close) * 100

    if change > 1.0:
        return "STRONG_BULLISH" if change > 2.0 else "BULLISH"
    elif change < -1.0:
        return "STRONG_BEARISH" if change < -2.0 else "BEARISH"
    return "NEUTRAL"


def quick_trend_alignment_check(trend_5m, trend_15m, signal):
    """
    Quick check if 5m and 15m trends align with signal
    Returns: (aligned, strength, reason)
    """
    analyzer = MultiTimeframeTrend()
    analyzer.update_trends(trend_5m, trend_15m)
    return analyzer.check_alignment(signal)


def get_trend_recommendation(trend_5m, trend_15m=None, trend_30m=None):
    """
    Get trading recommendation based on multi-timeframe trends
    Returns: dict with recommendation
    """
    analyzer = MultiTimeframeTrend()
    analyzer.update_trends(trend_5m, trend_15m, trend_30m)

    consistent, reason = analyzer.is_trend_consistent()

    if not consistent:
        return {
            'trade': False,
            'confidence': 'LOW',
            'reason': reason,
            'message': '❌ Timeframes not aligned - avoid trading'
        }

    score, tf_reasons = analyzer.get_trend_score()

    confidence = 'HIGH' if score >= 100 else 'MEDIUM' if score >= 65 else 'LOW'

    return {
        'trade': True,
        'confidence': confidence,
        'score': score,
        'reason': reason,
        'timeframes': tf_reasons,
        'message': f'✅ {confidence} confidence - aligned across timeframes'
    }


if __name__ == "__main__":
    print("="*60)
    print("📊 Multi-Timeframe Trend Analysis Test")
    print("="*60)

    # Test 1: All aligned bullish
    print("\n1️⃣ Test: All Timeframes Aligned Bullish")
    print("-" * 60)
    analyzer = MultiTimeframeTrend()
    analyzer.update_trends('BULLISH', 'BULLISH', 'BULLISH')
    aligned, strength, reason = analyzer.check_alignment("CE")
    print(f"Signal: CE")
    print(f"Aligned: {aligned}, Strength: {strength}%")
    print(f"Reason: {reason}")

    # Test 2: 15m against 5m
    print("\n2️⃣ Test: 15m Against 5m (Rejected)")
    print("-" * 60)
    analyzer.update_trends('BULLISH', 'BEARISH', None)
    aligned, strength, reason = analyzer.check_alignment("CE")
    print(f"Signal: CE")
    print(f"Aligned: {aligned}, Strength: {strength}%")
    print(f"Reason: {reason}")

    # Test 3: 5m only
    print("\n3️⃣ Test: 5m Only (Partial)")
    print("-" * 60)
    analyzer.update_trends('STRONG_BULLISH', None, None)
    aligned, strength, reason = analyzer.check_alignment("CE")
    print(f"Signal: CE")
    print(f"Aligned: {aligned}, Strength: {strength}%")
    print(f"Reason: {reason}")

    # Test 4: PE alignment
    print("\n4️⃣ Test: All Aligned Bearish for PE")
    print("-" * 60)
    analyzer.update_trends('BEARISH', 'STRONG_BEARISH', 'BEARISH')
    aligned, strength, reason = analyzer.check_alignment("PE")
    print(f"Signal: PE")
    print(f"Aligned: {aligned}, Strength: {strength}%")
    print(f"Reason: {reason}")

    print("\n" + "="*60)
    print("✅ Multi-Timeframe Trend ready!")
    print("   Higher TF confirms = Higher conviction")
    print("="*60)


__all__ = [
    'MultiTimeframeTrend',
    'calculate_trend_from_candles',
    'quick_trend_alignment_check',
    'get_trend_recommendation'
]
