from config import Config


class ATRCalculator:
    def __init__(self, period=None):
        self.period = period if period else Config.ATR_PERIOD
        self.tr_values = []
        self.prev_close = None
        self.atr = None

    def calculate_tr(self, candle):
        """
        True Range calculation
        """
        high = candle["high"]
        low = candle["low"]
        close = candle["close"]

        if self.prev_close is None:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - self.prev_close),
                abs(low - self.prev_close)
            )

        self.prev_close = close
        return tr

    def update(self, candle):
        """
        Update ATR with new candle
        """
        tr = self.calculate_tr(candle)
        self.tr_values.append(tr)

        # Keep only last N TR values
        if len(self.tr_values) > self.period:
            self.tr_values.pop(0)

        # Calculate ATR
        if len(self.tr_values) == self.period:
            self.atr = sum(self.tr_values) / self.period

        return self.atr

    def get_atr(self):
        return self.atr

    def get_buffer(self):
        """
        ATR based breakout buffer
        """
        if self.atr is None:
            return Config.FALLBACK_BUFFER

        buffer = self.atr * Config.ATR_MULTIPLIER

        # Clamp buffer between min & max
        buffer = max(Config.MIN_BUFFER, min(Config.MAX_BUFFER, buffer))

        return round(buffer)

    def get_volatility(self):
        """
        Market volatility level
        """
        if self.atr is None:
            return "LOW"

        if self.atr < 30:
            return "LOW"
        elif self.atr < 60:
            return "MEDIUM"
        else:
            return "HIGH"