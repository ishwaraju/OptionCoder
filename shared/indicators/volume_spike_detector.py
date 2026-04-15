"""
Volume Spike Detector
For confirming breakouts with volume - critical for option buyers
"""
from shared.utils.time_utils import TimeUtils


class VolumeSpikeDetector:
    """
    Detect volume spikes to confirm breakouts
    Volume > 1.5x average = Strong confirmation
    Volume < 1.2x average = Weak/breakout not confirmed
    """

    def __init__(self, period=14, spike_threshold=1.5, option_buyer_threshold=1.8):
        self.time_utils = TimeUtils()
        self.period = period
        self.spike_threshold = spike_threshold  # Normal breakout
        self.option_buyer_threshold = option_buyer_threshold  # Stricter for options
        self.volume_history = []
        self.avg_volume = 0

    def update(self, volume):
        """Update with new volume data"""
        if volume is None or volume <= 0:
            return None

        vol = float(volume)
        self.volume_history.append(vol)

        # Keep only needed history
        max_needed = self.period * 2
        if len(self.volume_history) > max_needed:
            self.volume_history = self.volume_history[-max_needed:]

        # Need minimum data
        if len(self.volume_history) < self.period:
            return {
                'current_volume': int(vol),
                'avg_volume': int(vol),  # Use current as estimate
                'ratio': 1.0,
                'is_spike': False,
                'strength': 'INSUFFICIENT_DATA'
            }

        return self._calculate_spike(vol)

    def _calculate_spike(self, current_volume):
        """Calculate volume spike metrics"""
        # Calculate average (excluding current volume for unbiased ratio)
        recent_volumes = self.volume_history[-self.period:]
        avg_volume = sum(recent_volumes) / len(recent_volumes)

        if avg_volume == 0:
            avg_volume = current_volume

        self.avg_volume = avg_volume

        # Calculate ratio
        ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

        # Determine strength
        if ratio >= self.option_buyer_threshold:
            strength = 'STRONG_SPIKE'
            is_spike = True
        elif ratio >= self.spike_threshold:
            strength = 'MODERATE_SPIKE'
            is_spike = True
        elif ratio >= 1.2:
            strength = 'SLIGHT_INCREASE'
            is_spike = False
        else:
            strength = 'NORMAL'
            is_spike = False

        return {
            'current_volume': int(current_volume),
            'avg_volume': int(avg_volume),
            'ratio': round(ratio, 2),
            'is_spike': is_spike,
            'strength': strength,
            'threshold': self.spike_threshold,
            'option_buyer_threshold': self.option_buyer_threshold
        }

    def is_breakout_confirmed(self, volume=None, for_option_buyer=True):
        """Check if breakout is confirmed by volume"""
        if volume is not None:
            result = self.update(volume)
        else:
            result = self.get_current()

        if result is None:
            return False, 0, "No volume data"

        threshold = self.option_buyer_threshold if for_option_buyer else self.spike_threshold
        confirmed = result['ratio'] >= threshold

        reason = f"Volume ratio: {result['ratio']:.2f} vs threshold {threshold}"
        if confirmed:
            reason += " ✅ Breakout confirmed"
        else:
            reason += " ❌ Volume too low"

        return confirmed, result['ratio'], reason

    def get_current(self):
        """Get current volume analysis"""
        if not self.volume_history:
            return None
        return self._calculate_spike(self.volume_history[-1])

    def get_volume_trend(self, lookback=5):
        """Get volume trend direction"""
        if len(self.volume_history) < lookback:
            return "INSUFFICIENT_DATA"

        recent = self.volume_history[-lookback:]
        first_half = sum(recent[:lookback//2]) / (lookback//2)
        second_half = sum(recent[lookback//2:]) / (lookback - lookback//2)

        if second_half > first_half * 1.3:
            return "INCREASING"
        elif second_half < first_half * 0.7:
            return "DECREASING"
        return "STABLE"

    def reset(self):
        """Reset detector"""
        self.volume_history = []
        self.avg_volume = 0

    def get_volume_percentile(self, current_volume):
        """Get what percentile current volume is in history"""
        if len(self.volume_history) < 10:
            return 50  # Default middle

        sorted_volumes = sorted(self.volume_history)
        below_current = sum(1 for v in sorted_volumes if v < current_volume)
        percentile = (below_current / len(sorted_volumes)) * 100

        return round(percentile, 1)


def detect_volume_spike(volumes, current_volume, threshold=1.5):
    """
    Quick volume spike detection
    volumes: list of recent volumes
    current_volume: current candle volume
    Returns: dict with analysis
    """
    if not volumes or current_volume is None:
        return {'is_spike': False, 'ratio': 1.0, 'reason': 'No data'}

    avg_volume = sum(volumes) / len(volumes)
    if avg_volume == 0:
        return {'is_spike': False, 'ratio': 1.0, 'reason': 'Zero average'}

    ratio = current_volume / avg_volume
    is_spike = ratio >= threshold

    return {
        'is_spike': is_spike,
        'ratio': round(ratio, 2),
        'current_volume': int(current_volume),
        'avg_volume': int(avg_volume),
        'threshold': threshold,
        'reason': f"Volume {ratio:.2f}x average" if is_spike else f"Volume only {ratio:.2f}x average"
    }


def is_volume_confirmed_for_option_buying(volumes, current_volume, min_ratio=1.8):
    """
    Strict volume check for option buyers
    Returns: (is_confirmed, ratio, reason)
    """
    result = detect_volume_spike(volumes, current_volume, min_ratio)

    if result['is_spike']:
        return True, result['ratio'], f"✅ Volume confirmed: {result['ratio']:.2f}x average"
    else:
        return False, result['ratio'], f"❌ Volume weak: {result['ratio']:.2f}x (need {min_ratio}x)"


# Pre-configured detectors
OPTION_BUYER_VOLUME_DETECTOR = VolumeSpikeDetector(
    period=14,
    spike_threshold=1.5,
    option_buyer_threshold=1.8
)

NORMAL_VOLUME_DETECTOR = VolumeSpikeDetector(
    period=14,
    spike_threshold=1.5,
    option_buyer_threshold=1.5  # Same as normal
)


class VolumeConfirmationManager:
    """
    Manager for volume confirmation with multiple checks
    """

    def __init__(self, for_option_buyer=True):
        self.detector = VolumeSpikeDetector(
            period=14,
            spike_threshold=1.5,
            option_buyer_threshold=1.8 if for_option_buyer else 1.5
        )
        self.for_option_buyer = for_option_buyer

    def confirm_signal(self, current_volume, price_change_percent=0):
        """
        Confirm signal with volume
        Returns: (confirmed, score, reason)
        """
        result = self.detector.update(current_volume)

        if result is None:
            return False, 0, "No volume data"

        ratio = result['ratio']
        threshold = 1.8 if self.for_option_buyer else 1.5

        # Score calculation
        if ratio >= 2.5:
            score = 25  # Excellent
            status = "EXCELLENT"
        elif ratio >= threshold:
            score = 20  # Good
            status = "GOOD"
        elif ratio >= 1.3:
            score = 10  # Weak
            status = "WEAK"
        else:
            score = 0
            status = "REJECT"

        confirmed = ratio >= threshold

        if confirmed:
            reason = f"✅ Volume {status}: {ratio:.2f}x (need {threshold}x)"
        else:
            reason = f"❌ Volume {status}: {ratio:.2f}x (need {threshold}x)"

        return confirmed, score, reason


if __name__ == "__main__":
    # Test
    print("="*60)
    print("📊 Volume Spike Detector Test")
    print("="*60)

    detector = VolumeSpikeDetector()

    # Simulate volume data
    test_volumes = [1000, 1100, 1050, 1200, 980, 1150, 1020, 1080, 1100, 950,
                    1000, 1050, 1100, 1080]  # Normal volumes

    for v in test_volumes:
        detector.update(v)

    # Test with spike
    spike_volume = 2200  # 2x average
    result = detector.update(spike_volume)

    print(f"\nNormal average: {result['avg_volume']}")
    print(f"Current volume: {result['current_volume']}")
    print(f"Ratio: {result['ratio']:.2f}x")
    print(f"Strength: {result['strength']}")

    confirmed, ratio, reason = detector.is_breakout_confirmed(for_option_buyer=True)
    print(f"\nOption buyer confirmation: {confirmed}")
    print(f"Reason: {reason}")

    # Test weak volume
    weak_result = detector.update(1100)  # Normal volume
    print(f"\nWeak volume test:")
    print(f"Ratio: {weak_result['ratio']:.2f}x")
    print(f"Confirmed: {weak_result['is_spike']}")

    print("\n" + "="*60)
    print("✅ Volume Spike Detector ready!")
    print(f"   Normal threshold: 1.5x")
    print(f"   Option buyer threshold: 1.8x (stricter)")
    print("="*60)


__all__ = [
    'VolumeSpikeDetector',
    'detect_volume_spike',
    'is_volume_confirmed_for_option_buying',
    'OPTION_BUYER_VOLUME_DETECTOR',
    'NORMAL_VOLUME_DETECTOR',
    'VolumeConfirmationManager'
]
