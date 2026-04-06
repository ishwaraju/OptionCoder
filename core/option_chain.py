import requests
import time
from config import Config
from utils.time_utils import TimeUtils


class OptionChain:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.base_url = "https://api.dhan.co/v2/optionchain"
        self.expiry_url = "https://api.dhan.co/v2/optionchain/expirylist"

        self.client_id = Config.DHAN_CLIENT_ID
        self.access_token = Config.DHAN_ACCESS_TOKEN

        self.headers = {
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id
        }

        self.cached_data = None
        self.last_fetch_time = 0
        self.cached_expiry = None
        self.option_chain_data = None
        self.underlying_price = None

    # -------------------------------------------------
    # Get nearest weekly expiry
    # -------------------------------------------------
    def get_expiry(self):
        if self.cached_expiry:
            return self.cached_expiry

        payload = {
            "UnderlyingScrip": 13,
            "UnderlyingSeg": "IDX_I"
        }

        try:
            response = requests.post(self.expiry_url, headers=self.headers, json=payload)

            if response.status_code != 200:
                print("ERROR: Expiry API Failed:", response.status_code)
                return None

            data = response.json()["data"]
            data = sorted(data)

            self.cached_expiry = data[0]
            print("Using Expiry:", self.cached_expiry)

            return self.cached_expiry

        except Exception as e:
            print("ERROR: Expiry API Exception:", e)
            return None

    # -------------------------------------------------
    # ATM Strike
    # -------------------------------------------------
    def get_atm_strike(self, price):
        step = Config.STRIKE_GAP
        return int(round(price / step) * step)

    # -------------------------------------------------
    # Fetch Option Chain
    # -------------------------------------------------
    def fetch_option_chain(self):

        # Rate limit protection (Dhan limit: 1 request / 3 sec)
        if time.time() - self.last_fetch_time < 5:
            return self.cached_data

        # Market closed check
        if not self.time_utils.is_market_open():
            return self.cached_data

        expiry = self.get_expiry()
        if expiry is None:
            return None

        payload = {
            "UnderlyingScrip": 13,
            "UnderlyingSeg": "IDX_I",
            "Expiry": expiry
        }

        try:
            response = requests.post(self.base_url, headers=self.headers, json=payload)

            if response.status_code != 200:
                print("ERROR: Option Chain API Failed:", response.status_code)
                return None

            data = response.json()["data"]
            oc = data["oc"]

            # Save OC for security ID lookup later
            self.option_chain_data = oc

            # Underlying price
            self.underlying_price = data["last_price"]

            atm = self.get_atm_strike(self.underlying_price)

            # ATM ±5 strikes
            strikes_needed = [atm + i for i in range(-250, 251, 50)]

            ce_oi_ladder = {}
            pe_oi_ladder = {}

            total_call_oi = 0
            total_put_oi = 0
            total_call_volume = 0
            total_put_volume = 0

            max_call_oi = 0
            max_put_oi = 0
            max_call_strike = None
            max_put_strike = None

            atm_ce = None
            atm_pe = None

            for strike in strikes_needed:
                strike_key = f"{strike:.6f}"

                if strike_key not in oc:
                    continue

                ce = oc[strike_key]["ce"]
                pe = oc[strike_key]["pe"]

                ce_oi_ladder[strike] = ce["oi"]
                pe_oi_ladder[strike] = pe["oi"]

                total_call_oi += ce["oi"]
                total_put_oi += pe["oi"]
                total_call_volume += ce.get("volume", 0)
                total_put_volume += pe.get("volume", 0)

                if ce["oi"] > max_call_oi:
                    max_call_oi = ce["oi"]
                    max_call_strike = strike

                if pe["oi"] > max_put_oi:
                    max_put_oi = pe["oi"]
                    max_put_strike = strike

                if strike == atm:
                    atm_ce = ce
                    atm_pe = pe

            pcr = total_put_oi / total_call_oi if total_call_oi != 0 else 0

            result = {
                "time": self.time_utils.current_time(),
                "underlying_price": self.underlying_price,
                "atm": atm,
                "pcr": round(pcr, 2),

                # OI Ladder
                "ce_oi_ladder": ce_oi_ladder,
                "pe_oi_ladder": pe_oi_ladder,

                # Support / Resistance
                "max_call_oi_strike": max_call_strike,
                "max_put_oi_strike": max_put_strike,

                # ATM data
                "atm_ce_security_id": atm_ce["security_id"] if atm_ce else None,
                "atm_pe_security_id": atm_pe["security_id"] if atm_pe else None,

                "ce_ltp": atm_ce.get("last_price", 0) if atm_ce else 0,
                "pe_ltp": atm_pe.get("last_price", 0) if atm_pe else 0,

                "ce_oi": atm_ce.get("oi", 0) if atm_ce else 0,
                "pe_oi": atm_pe.get("oi", 0) if atm_pe else 0,
                "ce_volume": atm_ce.get("volume", 0) if atm_ce else 0,
                "pe_volume": atm_pe.get("volume", 0) if atm_pe else 0,
                "ce_volume_band": total_call_volume,
                "pe_volume_band": total_put_volume,

                "ce_iv": atm_ce.get("implied_volatility", 0) if atm_ce else 0,
                "pe_iv": atm_pe.get("implied_volatility", 0) if atm_pe else 0,
            }

            self.cached_data = result
            self.last_fetch_time = time.time()

            return result

        except Exception as e:
            print("ERROR: Option Chain Exception:", e)
            return None

    # -------------------------------------------------
    # Get Security ID by Strike
    # -------------------------------------------------
    def get_security_id_by_strike(self, strike):
        if not self.option_chain_data:
            return None

        strike_str = f"{strike:.6f}"

        if strike_str not in self.option_chain_data:
            return None

        strike_data = self.option_chain_data[strike_str]

        ce_id = strike_data["ce"]["security_id"] if strike_data.get("ce") else None
        pe_id = strike_data["pe"]["security_id"] if strike_data.get("pe") else None

        return {
            "ce": ce_id,
            "pe": pe_id
        }
