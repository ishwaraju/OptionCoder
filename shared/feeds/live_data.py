from shared.utils.time_utils import TimeUtils


class LiveData:
    """
    Stores live market data from WebSocket
    """

    def __init__(self):
        self.time_utils = TimeUtils()
        self.instrument_data = {}

    def _ensure_instrument(self, instrument):
        if instrument not in self.instrument_data:
            self.instrument_data[instrument] = {
                "price": None,
                "volume": 0,
                "oi": 0,
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "ce_oi": 0,
                "pe_oi": 0,
                "fut_volume": 0,
                "last_update_time": None,
                "last_update_dt": None,
            }
        return self.instrument_data[instrument]

    def update_index_data(self, instrument, price, open_p, high_p, low_p, volume):
        data = self._ensure_instrument(instrument)
        data["price"] = price
        data["open_price"] = open_p
        data["high_price"] = high_p
        data["low_price"] = low_p
        data["volume"] = volume
        data["last_update_time"] = self.time_utils.current_time_str()
        data["last_update_dt"] = self.time_utils.now_ist()

    def update_futures_data(self, instrument, volume, oi):
        data = self._ensure_instrument(instrument)
        data["fut_volume"] = volume
        data["oi"] = oi
        data["last_update_time"] = self.time_utils.current_time_str()
        data["last_update_dt"] = self.time_utils.now_ist()

    def update_option_data(self, instrument, ce_oi=None, pe_oi=None):
        data = self._ensure_instrument(instrument)
        if ce_oi is not None:
            data["ce_oi"] = ce_oi
        if pe_oi is not None:
            data["pe_oi"] = pe_oi
        data["last_update_time"] = self.time_utils.current_time_str()
        data["last_update_dt"] = self.time_utils.now_ist()

    def get_snapshot(self, instrument=None):
        if instrument is None:
            if not self.instrument_data:
                return {
                    "time": None,
                    "last_update_dt": None,
                    "data_age_seconds": None,
                    "price": None,
                    "open": None,
                    "high": None,
                    "low": None,
                    "volume": 0,
                    "futures_volume": 0,
                    "oi": 0,
                    "ce_oi": 0,
                    "pe_oi": 0,
                }
            instrument = next(iter(self.instrument_data.keys()))

        data = self._ensure_instrument(instrument)
        now = self.time_utils.now_ist()
        data_age_seconds = None
        if data["last_update_dt"] is not None:
            data_age_seconds = max(0.0, (now - data["last_update_dt"]).total_seconds())

        return {
            "instrument": instrument,
            "time": data["last_update_time"],
            "last_update_dt": data["last_update_dt"],
            "data_age_seconds": data_age_seconds,
            "price": data["price"],
            "open": data["open_price"],
            "high": data["high_price"],
            "low": data["low_price"],
            "volume": data["volume"],
            "futures_volume": data["fut_volume"],
            "oi": data["oi"],
            "ce_oi": data["ce_oi"],
            "pe_oi": data["pe_oi"],
        }

    def get_all_snapshots(self):
        return {
            instrument: self.get_snapshot(instrument)
            for instrument in self.instrument_data.keys()
        }
