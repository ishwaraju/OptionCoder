from shared.utils.time_utils import TimeUtils


class LiveData:
    """
    Stores live market data from WebSocket
    """

    def __init__(self):
        self.time_utils = TimeUtils()

        # Live values
        self.price = None
        self.volume = 0
        self.oi = 0

        self.open_price = None
        self.high_price = None
        self.low_price = None

        # Option OI
        self.ce_oi = 0
        self.pe_oi = 0

        # Futures volume
        self.fut_volume = 0

        # Timestamp
        self.last_update_time = None
        self.last_update_dt = None

    def update_index_data(self, price, open_p, high_p, low_p, volume):
        """Update NIFTY index data"""
        self.price = price
        self.open_price = open_p
        self.high_price = high_p
        self.low_price = low_p
        self.volume = volume
        self.last_update_time = self.time_utils.current_time_str()
        self.last_update_dt = self.time_utils.now_ist()

    def update_futures_data(self, volume, oi):
        """Update Futures data"""
        self.fut_volume = volume
        self.oi = oi
        self.last_update_time = self.time_utils.current_time_str()
        self.last_update_dt = self.time_utils.now_ist()

    def update_option_data(self, ce_oi=None, pe_oi=None):
        """Update Option OI data"""
        if ce_oi is not None:
            self.ce_oi = ce_oi

        if pe_oi is not None:
            self.pe_oi = pe_oi

        self.last_update_time = self.time_utils.current_time_str()
        self.last_update_dt = self.time_utils.now_ist()

    def get_snapshot(self):
        """Return current market snapshot"""
        now = self.time_utils.now_ist()
        data_age_seconds = None
        if self.last_update_dt is not None:
            data_age_seconds = max(0.0, (now - self.last_update_dt).total_seconds())

        return {
            "time": self.last_update_time,
            "last_update_dt": self.last_update_dt,
            "data_age_seconds": data_age_seconds,
            "price": self.price,
            "open": self.open_price,
            "high": self.high_price,
            "low": self.low_price,
            "volume": self.volume,
            "futures_volume": self.fut_volume,
            "oi": self.oi,
            "ce_oi": self.ce_oi,
            "pe_oi": self.pe_oi
        }
