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
        self.source = None

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
        self.source = None

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
        volume = candle.get("volume") or 0
        vwap_price = candle.get("vwap_price")
        vwap_volume = candle.get("vwap_volume")

        typical_price = (high + low + close) / 3
        price_for_vwap = float(vwap_price) if vwap_price is not None else typical_price
        volume_for_vwap = int(vwap_volume) if vwap_volume not in (None, "") else int(volume or 0)

        self.cumulative_price += typical_price
        self.price_count += 1

        if volume_for_vwap and volume_for_vwap > 0:
            self.cumulative_pv += price_for_vwap * volume_for_vwap
            self.cumulative_volume += volume_for_vwap
            self.source = "FUTURES" if vwap_price is not None and vwap_volume not in (None, 0, "") else "TRADED_VOLUME"

        if self.cumulative_volume != 0:
            self.vwap = self.cumulative_pv / self.cumulative_volume
        elif self.price_count:
            # Index candles can have zero volume; use a session typical-price
            # average so VWAP-dependent structure checks can still run.
            self.vwap = self.cumulative_price / self.price_count
            self.source = "PRICE_ONLY_FALLBACK"

        return self.vwap

    def get_vwap(self):
        return self.vwap

    def get_source(self):
        return self.source

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
