from config import Config


class OIAnalyzer:
    def __init__(self):
        self.prev_call_oi = None
        self.prev_put_oi = None
        self.prev_price = None

        self.call_oi_change = 0
        self.put_oi_change = 0
        self.price_change = 0

    def update(self, price, call_oi, put_oi):
        """
        Update OI and price data
        """

        if self.prev_price is not None:
            self.price_change = price - self.prev_price

        if self.prev_call_oi is not None:
            self.call_oi_change = call_oi - self.prev_call_oi

        if self.prev_put_oi is not None:
            self.put_oi_change = put_oi - self.prev_put_oi

        # Save current as previous
        self.prev_price = price
        self.prev_call_oi = call_oi
        self.prev_put_oi = put_oi

    def get_oi_signal(self):
        """
        Determine OI based market direction
        """

        # Price up
        if self.price_change > 0:
            if self.put_oi_change > 0:
                return "LONG_BUILDUP"  # Bullish
            elif self.call_oi_change < 0:
                return "SHORT_COVERING"  # Bullish

        # Price down
        elif self.price_change < 0:
            if self.call_oi_change > 0:
                return "SHORT_BUILDUP"  # Bearish
            elif self.put_oi_change < 0:
                return "LONG_UNWINDING"  # Bearish

        return "NO_CLEAR_SIGNAL"

    def get_bias(self):
        """
        Simple Bullish/Bearish Bias
        """

        signal = self.get_oi_signal()

        if signal in ["LONG_BUILDUP", "SHORT_COVERING"]:
            return "BULLISH"

        elif signal in ["SHORT_BUILDUP", "LONG_UNWINDING"]:
            return "BEARISH"

        return "NEUTRAL"

    def get_oi_strength(self):
        """
        OI strength based on change magnitude
        """

        total_change = abs(self.call_oi_change) + abs(self.put_oi_change)

        if total_change > 500000:
            return "STRONG"
        elif total_change > 200000:
            return "MEDIUM"
        else:
            return "WEAK"