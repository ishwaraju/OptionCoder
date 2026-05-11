from shared.utils.time_utils import TimeUtils


class VWAPCalculator:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.cumulative_pv = 0
        self.cumulative_volume = 0
        self.cumulative_price = 0
        self.price_count = 0
        self.current_day = None
        self.vwap = None

    def _get_session_day(self, candle):
        candle_dt = candle.get("time") or candle.get("datetime") or candle.get("close_time")

        if hasattr(candle_dt, "date"):
            return candle_dt.date().isoformat()

        return self.time_utils.today_str()

    def reset(self):
        """Reset VWAP for new trading day"""
        self.cumulative_pv = 0
        self.cumulative_volume = 0
        self.cumulative_price = 0
        self.price_count = 0
        self.vwap = None

    def update(self, candle):
        """
        Update VWAP with new 5-min candle
        candle format:
        {
            open,
            high,
            low,
            close,
            volume
        }
        """

        session_day = self._get_session_day(candle)

        # Reset VWAP if new day
        if self.current_day != session_day:
            self.reset()
            self.current_day = session_day

        high = candle["high"]
        low = candle["low"]
        close = candle["close"]
        volume = candle["volume"]

        typical_price = (high + low + close) / 3

        self.cumulative_price += typical_price
        self.price_count += 1

        if volume and volume > 0:
            self.cumulative_pv += typical_price * volume
            self.cumulative_volume += volume

        if self.cumulative_volume != 0:
            self.vwap = self.cumulative_pv / self.cumulative_volume
        elif self.price_count:
            # Index candles can have zero volume; use a session typical-price
            # average so VWAP-dependent structure checks can still run.
            self.vwap = self.cumulative_price / self.price_count

        return self.vwap

    def get_vwap(self):
        return self.vwap

    def get_vwap_signal(self, price):
        """
        VWAP Trend Signal
        """
        if self.vwap is None:
            return "NO_DATA"

        if price > self.vwap:
            return "BULLISH"
        elif price < self.vwap:
            return "BEARISH"
        else:
            return "NEUTRAL"
