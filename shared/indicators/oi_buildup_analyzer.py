"""
OI (Open Interest) Buildup Analyzer
For confirming institutional participation - critical for option buyers
OI + Price direction = Strong signal
"""
from shared.utils.time_utils import TimeUtils


class OIBuildupAnalyzer:
    """
    Analyze OI buildup to confirm signal strength
    OI increase + Price in direction = Strong conviction
    OI decrease + Price move = Weak (short covering/long unwinding)
    """

    def __init__(self, for_option_buyer=True):
        self.time_utils = TimeUtils()
        self.for_option_buyer = for_option_buyer
        self.oi_history = []
        self.price_history = []

        # Thresholds
        self.min_oi_change_percent = 5.0 if for_option_buyer else 3.0
        self.strong_oi_change_percent = 10.0 if for_option_buyer else 8.0

    def update(self, oi_value, price, timestamp=None):
        """Update with new OI and price data"""
        if oi_value is None or price is None:
            return None

        self.oi_history.append({
            'oi': float(oi_value),
            'price': float(price),
            'timestamp': timestamp or self.time_utils.now_ist()
        })

        # Keep last 20 data points
        if len(self.oi_history) > 20:
            self.oi_history = self.oi_history[-20:]

        return self.get_current_analysis()

    def get_current_analysis(self):
        """Get current OI buildup analysis"""
        if len(self.oi_history) < 2:
            return None

        current = self.oi_history[-1]
        lookback = min(4, len(self.oi_history))
        baseline = self.oi_history[-lookback]
        previous = self.oi_history[-2]

        # Calculate changes
        oi_change = current['oi'] - baseline['oi']
        oi_change_percent = (oi_change / baseline['oi']) * 100 if baseline['oi'] > 0 else 0

        price_change = current['price'] - baseline['price']
        price_change_percent = (price_change / baseline['price']) * 100 if baseline['price'] > 0 else 0

        last_oi_step = current['oi'] - previous['oi']
        last_price_step = current['price'] - previous['price']
        aligned_steps = 0
        for earlier, later in zip(self.oi_history[-lookback:-1], self.oi_history[-lookback + 1:]):
            oi_step = later['oi'] - earlier['oi']
            if earlier['oi'] <= 0 or earlier['price'] <= 0:
                continue
            oi_step_pct = (oi_step / earlier['oi']) * 100
            price_step = later['price'] - earlier['price']
            price_step_pct = (price_step / earlier['price']) * 100
            step_type = self._classify_buildup(oi_step_pct, price_step_pct)
            if step_type == 'FLAT':
                continue
            aligned_steps += 1 if step_type == self._classify_buildup(oi_change_percent, price_change_percent) else 0

        # Determine buildup type
        buildup_type = self._classify_buildup(oi_change_percent, price_change_percent)

        # Calculate strength score
        strength_score = self._calculate_strength_score(oi_change_percent, price_change_percent)

        return {
            'current_oi': int(current['oi']),
            'previous_oi': int(previous['oi']),
            'oi_change': int(oi_change),
            'oi_change_percent': round(oi_change_percent, 2),
            'price_change': round(price_change, 2),
            'price_change_percent': round(price_change_percent, 2),
            'last_oi_step': int(last_oi_step),
            'last_price_step': round(last_price_step, 2),
            'buildup_type': buildup_type,
            'strength_score': strength_score,
            'lookback_points': lookback,
            'aligned_steps': aligned_steps,
            'is_fresh': buildup_type in ['FRESH_LONG', 'FRESH_SHORT'],
            'is_unwinding': buildup_type in ['LONG_UNWINDING', 'SHORT_COVERING']
        }

    def _classify_buildup(self, oi_change_percent, price_change_percent):
        """Classify OI buildup type"""
        if abs(oi_change_percent) < 1.0 or abs(price_change_percent) < 0.05:
            return 'FLAT'

        oi_up = oi_change_percent > 0
        price_up = price_change_percent > 0

        if oi_up and price_up:
            return 'FRESH_LONG'  # New buying - Strong bullish
        elif oi_up and not price_up:
            return 'FRESH_SHORT'  # New shorting - Strong bearish
        elif not oi_up and price_up:
            return 'SHORT_COVERING'  # Shorts exiting - Weak bullish
        elif not oi_up and not price_up:
            return 'LONG_UNWINDING'  # Longs exiting - Weak bearish

        return 'UNKNOWN'

    def _calculate_strength_score(self, oi_change_percent, price_change_percent):
        """Calculate signal strength score based on OI and price"""
        score = 0

        # OI contribution (0-50 points)
        abs_oi_change = abs(oi_change_percent)
        if abs_oi_change >= self.strong_oi_change_percent:
            score += 50
        elif abs_oi_change >= self.min_oi_change_percent:
            score += 35
        elif abs_oi_change >= 2:
            score += 20
        else:
            score += 10

        # Alignment bonus (0-30 points)
        buildup_type = self._classify_buildup(oi_change_percent, price_change_percent)
        if buildup_type in ['FRESH_LONG', 'FRESH_SHORT']:
            score += 30  # Fresh positions = strong
        elif buildup_type in ['SHORT_COVERING', 'LONG_UNWINDING']:
            score += 15  # Unwinding = weak
        elif buildup_type == 'FLAT':
            score -= 10

        return score

    def confirm_signal(self, signal, min_score=50):
        """
        Confirm signal with OI buildup
        signal: 'CE' or 'PE'
        Returns: (confirmed, score, reason)
        """
        analysis = self.get_current_analysis()

        if analysis is None:
            return False, 0, "No OI data available"

        buildup_type = analysis['buildup_type']
        oi_change = analysis['oi_change_percent']
        score = analysis['strength_score']
        aligned_steps = int(analysis.get('aligned_steps') or 0)
        lookback_points = int(analysis.get('lookback_points') or 2)
        persistence_ok = aligned_steps >= max(1, lookback_points - 2)

        # Check alignment
        if signal == "CE":
            # For CE: Want FRESH_LONG (OI up, price up) or SHORT_COVERING (OI down, price up)
            if buildup_type == 'FRESH_LONG':
                confirmed = True
                reason = f"✅ Strong CE: Fresh long buildup (+{oi_change:.1f}% OI)"
            elif buildup_type == 'SHORT_COVERING':
                confirmed = persistence_ok and score >= max(55, min_score)
                reason = f"⚠️ Weak CE: Short covering (-{abs(oi_change):.1f}% OI)"
            elif buildup_type == 'FRESH_SHORT':
                confirmed = False
                reason = f"❌ CE rejected: Fresh short buildup (-{oi_change:.1f}% OI) - bearish"
            elif buildup_type == 'FLAT':
                confirmed = False
                reason = "❌ CE rejected: OI/price participation flat"
            else:  # LONG_UNWINDING
                confirmed = False
                reason = f"❌ CE rejected: Long unwinding (-{abs(oi_change):.1f}% OI)"

        else:  # PE
            # For PE: Want FRESH_SHORT (OI up, price down) or LONG_UNWINDING (OI down, price down)
            if buildup_type == 'FRESH_SHORT':
                confirmed = True
                reason = f"✅ Strong PE: Fresh short buildup (+{oi_change:.1f}% OI)"
            elif buildup_type == 'LONG_UNWINDING':
                confirmed = persistence_ok and score >= max(55, min_score)
                reason = f"⚠️ Weak PE: Long unwinding (-{abs(oi_change):.1f}% OI)"
            elif buildup_type == 'FRESH_LONG':
                confirmed = False
                reason = f"❌ PE rejected: Fresh long buildup (+{oi_change:.1f}% OI) - bullish"
            elif buildup_type == 'FLAT':
                confirmed = False
                reason = "❌ PE rejected: OI/price participation flat"
            else:  # SHORT_COVERING
                confirmed = False
                reason = f"❌ PE rejected: Short covering (+{abs(oi_change):.1f}% OI) - bullish"

        # Additional score check for option buyers
        if self.for_option_buyer and score < min_score:
            confirmed = False
            reason += f" | Score {score} < {min_score} (insufficient)"
        elif confirmed and not persistence_ok and buildup_type in {'SHORT_COVERING', 'LONG_UNWINDING'}:
            confirmed = False
            reason += " | Persistence weak"
        elif confirmed and persistence_ok:
            reason += f" | persistence {aligned_steps}/{max(1, lookback_points - 1)}"

        return confirmed, score, reason

    def is_strong_buildup(self, threshold=5.0):
        """Check if current OI change is strong"""
        analysis = self.get_current_analysis()
        if analysis is None:
            return False, 0

        return abs(analysis['oi_change_percent']) >= threshold, analysis['oi_change_percent']

    def get_oi_trend(self, lookback=5):
        """Get OI trend over recent periods"""
        if len(self.oi_history) < lookback:
            return "INSUFFICIENT_DATA"

        recent = self.oi_history[-lookback:]
        first_oi = recent[0]['oi']
        last_oi = recent[-1]['oi']

        change = ((last_oi - first_oi) / first_oi) * 100 if first_oi > 0 else 0

        if change > 8:
            return f"RISING_STRONG ({change:.1f}%)"
        elif change > 3:
            return f"RISING ({change:.1f}%)"
        elif change < -8:
            return f"FALLING_STRONG ({change:.1f}%)"
        elif change < -3:
            return f"FALLING ({change:.1f}%)"
        return f"STABLE ({change:.1f}%)"

    def get_pcr_analysis(self, pcr_value):
        """Analyze Put-Call Ratio"""
        if pcr_value is None:
            return None

        if pcr_value > 1.2:
            sentiment = "BEARISH_EXTREME"
            bias = "CONTRARIAN_BULLISH"
        elif pcr_value > 1.0:
            sentiment = "BEARISH"
            bias = "NEUTRAL"
        elif pcr_value > 0.8:
            sentiment = "NEUTRAL"
            bias = "NEUTRAL"
        elif pcr_value > 0.6:
            sentiment = "BULLISH"
            bias = "NEUTRAL"
        else:
            sentiment = "BULLISH_EXTREME"
            bias = "CONTRARIAN_BEARISH"

        return {
            'pcr': round(pcr_value, 2),
            'sentiment': sentiment,
            'bias': bias
        }

    def reset(self):
        """Reset analyzer"""
        self.oi_history = []
        self.price_history = []


def quick_oi_check(current_oi, previous_oi, current_price, previous_price, signal):
    """
    Quick OI buildup check
    Returns: (confirmed, score, reason)
    """
    if None in (current_oi, previous_oi, current_price, previous_price):
        return False, 0, "Missing OI/Price data"

    analyzer = OIBuildupAnalyzer(for_option_buyer=True)
    analyzer.update(previous_oi, previous_price)
    analyzer.update(current_oi, current_price)

    return analyzer.confirm_signal(signal)


def is_oi_confirming_trend(oi_change_percent, price_change_percent, direction):
    """
    Check if OI is confirming the price trend
    Returns: (is_confirming, strength, reason)
    """
    oi_up = oi_change_percent > 0
    price_up = price_change_percent > 0

    if direction == "CE":
        if oi_up and price_up:
            return True, "STRONG", "Fresh buying - strong bullish"
        elif not oi_up and price_up:
            return True, "WEAK", "Short covering - weak bullish"
        elif oi_up and not price_up:
            return False, "NONE", "Fresh shorts - bearish"
        else:
            return False, "NONE", "Long unwinding - bearish"
    else:  # PE
        if oi_up and not price_up:
            return True, "STRONG", "Fresh shorts - strong bearish"
        elif not oi_up and not price_up:
            return True, "WEAK", "Long unwinding - weak bearish"
        elif oi_up and price_up:
            return False, "NONE", "Fresh longs - bullish"
        else:
            return False, "NONE", "Short covering - bullish"


# Pre-configured analyzers
OPTION_BUYER_OI_ANALYZER = OIBuildupAnalyzer(for_option_buyer=True)
NORMAL_OI_ANALYZER = OIBuildupAnalyzer(for_option_buyer=False)

if __name__ == "__main__":
    print("="*60)
    print("📊 OI Buildup Analyzer Test")
    print("="*60)

    analyzer = OIBuildupAnalyzer(for_option_buyer=True)

    # Simulate data: Fresh long buildup (strong bullish)
    print("\n1️⃣ Test: Fresh Long Buildup (Strong CE)")
    print("-" * 60)
    for i in range(5):
        oi = 1000000 + (i * 50000)  # OI increasing
        price = 24000 + (i * 50)  # Price increasing
        analyzer.update(oi, price)

    confirmed, score, reason = analyzer.confirm_signal("CE")
    print(f"Result: {confirmed}, Score: {score}")
    print(f"Reason: {reason}")

    # Reset and test short covering (weak bullish)
    analyzer.reset()
    print("\n2️⃣ Test: Short Covering (Weak CE - Rejected for PE)")
    print("-" * 60)
    for i in range(5):
        oi = 1000000 - (i * 30000)  # OI decreasing
        price = 24000 + (i * 50)  # Price increasing
        analyzer.update(oi, price)

    confirmed, score, reason = analyzer.confirm_signal("CE")
    print(f"Result: {confirmed}, Score: {score}")
    print(f"Reason: {reason}")

    # Reset and test fresh short (strong bearish)
    analyzer.reset()
    print("\n3️⃣ Test: Fresh Short Buildup (Strong PE)")
    print("-" * 60)
    for i in range(5):
        oi = 1000000 + (i * 60000)  # OI increasing
        price = 24000 - (i * 60)  # Price decreasing
        analyzer.update(oi, price)

    confirmed, score, reason = analyzer.confirm_signal("PE")
    print(f"Result: {confirmed}, Score: {score}")
    print(f"Reason: {reason}")

    print("\n" + "="*60)
    print("✅ OI Buildup Analyzer ready!")
    print("   Fresh positions = Strong signal")
    print("   Unwinding/Covering = Weak signal")
    print("="*60)


__all__ = [
    'OIBuildupAnalyzer',
    'quick_oi_check',
    'is_oi_confirming_trend',
    'OPTION_BUYER_OI_ANALYZER',
    'NORMAL_OI_ANALYZER'
]
