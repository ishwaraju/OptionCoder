import random
from dhanhq import dhanhq
from config import Config
from utils.time_utils import TimeUtils


class MarketData:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.dhan = dhanhq(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)

        self.nifty_security_id = Config.SECURITY_IDS["NIFTY"]
        self.exchange_index = "IDX_I"
        self.exchange_fno = "NSE_FNO"

        # Mock price start
        self.mock_price = 22400

        # Cache
        self.cached_option_chain = None
        self.last_oi_fetch_time = 0

        self.fut_security_id = None

    # ---------------- MOCK DATA ---------------- #
    def get_mock_price(self):
        move = random.randint(-20, 20)
        self.mock_price += move
        return self.mock_price

    def get_mock_oi(self):
        return {
            "call_oi": random.randint(1000000, 1200000),
            "put_oi": random.randint(1200000, 1500000),
        }

    def get_mock_option_data(self, price):
        atm = self.get_atm_strike(price)
        return {
            "atm_strike": atm,
            "ce_oi": random.randint(200000, 400000),
            "pe_oi": random.randint(300000, 500000),
            "ce_ltp": random.randint(80, 150),
            "pe_ltp": random.randint(80, 150),
            "ce_volume": random.randint(100000, 300000),
            "pe_volume": random.randint(100000, 300000),
        }

    def get_mock_futures_volume(self):
        return random.randint(50000, 150000)

    # ---------------- LIVE DATA ---------------- #
    def get_live_price(self):
        today = self.time_utils.today_str()

        data = self.dhan.intraday_minute_data(
            security_id=self.nifty_security_id,
            exchange_segment=self.exchange_index,
            instrument_type="INDEX",
            from_date=today,
            to_date=today
        )

        if data['status'] == 'success':
            candles = data['data']
            if candles:
                return candles[-1]['close']

        return None

    def get_expiry_list(self):
        data = self.dhan.expiry_list(
            under_security_id=self.nifty_security_id,
            under_exchange_segment=self.exchange_index
        )

        if data['status'] == 'success':
            return data['data']

        return []

    def get_nearest_expiry(self):
        expiries = self.get_expiry_list()
        if expiries:
            return expiries[0]
        return None

    def get_option_chain(self):
        """Fetch option chain with caching"""
        import time

        if Config.USE_MOCK_DATA:
            return None

        current_time = time.time()

        if current_time - self.last_oi_fetch_time < Config.OI_FETCH_INTERVAL:
            return self.cached_option_chain

        expiry = self.get_nearest_expiry()
        if expiry is None:
            return None

        data = self.dhan.option_chain(
            under_security_id=self.nifty_security_id,
            under_exchange_segment=self.exchange_index,
            expiry=expiry
        )

        if data['status'] == 'success':
            self.cached_option_chain = data['data']
            self.last_oi_fetch_time = current_time
            return self.cached_option_chain

        return None

    # ---------------- FUTURES VOLUME ---------------- #
    def get_nifty_futures_security_id(self):
        """Find NIFTY Futures security ID dynamically"""
        if self.fut_security_id:
            return self.fut_security_id

        instruments = self.dhan.fetch_security_list("compact")

        if instruments['status'] != 'success':
            return None

        for inst in instruments['data']:
            if (
                    inst['SEM_TRADING_SYMBOL'].startswith("NIFTY") and
                    inst['SEM_INSTRUMENT_NAME'] == "FUTIDX"
            ):
                self.fut_security_id = inst['SEM_SMST_SECURITY_ID']
                return self.fut_security_id

        return None

    def get_futures_volume(self):
        """Get NIFTY Futures volume"""
        if Config.USE_MOCK_DATA:
            return self.get_mock_futures_volume()

        today = self.time_utils.today_str()
        fut_id = self.get_nifty_futures_security_id()

        if fut_id is None:
            return 0

        data = self.dhan.intraday_minute_data(
            security_id=fut_id,
            exchange_segment=self.exchange_fno,
            instrument_type="FUTIDX",
            from_date=today,
            to_date=today
        )

        if data['status'] == 'success':
            candles = data['data']
            if candles:
                return candles[-1]['volume']

        return 0

    # ---------------- COMMON ---------------- #
    def get_nifty_price(self):
        if Config.USE_MOCK_DATA:
            return self.get_mock_price()
        return self.get_live_price()

    def get_total_oi(self):
        if Config.USE_MOCK_DATA:
            oi = self.get_mock_oi()
            return oi["call_oi"], oi["put_oi"]

        option_chain = self.get_option_chain()
        if option_chain is None:
            return None, None

        total_call_oi = 0
        total_put_oi = 0

        for strike in option_chain['oc']:
            if strike['ce']:
                total_call_oi += strike['ce']['oi']
            if strike['pe']:
                total_put_oi += strike['pe']['oi']

        return total_call_oi, total_put_oi

    def get_pcr(self):
        call_oi, put_oi = self.get_total_oi()

        if call_oi is None or call_oi == 0:
            return None

        return round(put_oi / call_oi, 2)

    def get_atm_strike(self, price):
        strike_step = Config.STRIKE_GAP
        return round(price / strike_step) * strike_step

    def get_atm_option_data(self, price):
        if Config.USE_MOCK_DATA:
            return self.get_mock_option_data(price)

        atm = self.get_atm_strike(price)
        option_chain = self.get_option_chain()

        if option_chain is None:
            return None

        for strike in option_chain['oc']:
            if strike['strikePrice'] == atm:
                ce_data = strike['ce']
                pe_data = strike['pe']

                return {
                    "atm_strike": atm,
                    "ce_oi": ce_data['oi'] if ce_data else 0,
                    "pe_oi": pe_data['oi'] if pe_data else 0,
                    "ce_ltp": ce_data['lastPrice'] if ce_data else 0,
                    "pe_ltp": pe_data['lastPrice'] if pe_data else 0,
                    "ce_volume": ce_data['volume'] if ce_data else 0,
                    "pe_volume": pe_data['volume'] if pe_data else 0,
                }

        return None

    def get_all_data(self):
        """Main function used by trading bot"""
        price = self.get_nifty_price()
        call_oi, put_oi = self.get_total_oi()
        futures_volume = self.get_futures_volume()
        atm_data = self.get_atm_option_data(price) if price else None
        pcr = self.get_pcr()

        return {
            "price": price,
            "call_oi": call_oi,
            "put_oi": put_oi,
            "pcr": pcr,
            "futures_volume": futures_volume,
            "atm_data": atm_data
        }

    # ---------------- TIME ---------------- #
    def is_market_open(self):
        return self.time_utils.is_market_open()

    def can_trade(self):
        return self.time_utils.can_trade()

    def is_orb_time(self):
        return self.time_utils.is_orb_time()