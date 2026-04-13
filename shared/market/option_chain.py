import requests
import time
from datetime import timedelta
from config import Config
from shared.utils.time_utils import TimeUtils


class OptionChain:
    def __init__(self, instrument=None):
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

        # Get security ID for instrument
        self.instrument = (instrument or Config.SYMBOL).upper()
        self.security_id = Config.SECURITY_IDS.get(self.instrument, 13)

        self.cached_data = None
        self.last_fetch_time = 0
        self.cached_expiry = None
        self.option_chain_data = None
        self.underlying_price = None
        self.mock_step = 0

    @staticmethod
    def _safe_spread(bid_price, ask_price):
        if bid_price in (None, 0) or ask_price in (None, 0):
            return None
        return round(float(ask_price) - float(bid_price), 2)

    def _post_json(self, url, payload, label):
        last_error = None

        for attempt in range(1, Config.OPTION_CHAIN_RETRIES + 1):
            try:
                response = requests.post(
                    url,
                    headers=self.headers,
                    json=payload,
                    timeout=Config.OPTION_CHAIN_TIMEOUT,
                )

                if response.status_code == 200:
                    return response

                last_error = f"{label} API Failed: {response.status_code}"
                print("ERROR:", last_error, f"(attempt {attempt}/{Config.OPTION_CHAIN_RETRIES})")
            except Exception as e:
                last_error = e
                print(
                    f"ERROR: {label} API Exception (attempt {attempt}/{Config.OPTION_CHAIN_RETRIES}):",
                    e,
                )

            if attempt < Config.OPTION_CHAIN_RETRIES:
                time.sleep(attempt)

        return None

    def _build_mock_option_chain(self):
        self.mock_step += 1

        base_price = self.underlying_price if self.underlying_price is not None else 20000
        price_drift = ((self.mock_step % 12) - 6) * 8
        self.underlying_price = base_price + price_drift
        atm = self.get_atm_strike(self.underlying_price)
        expiry = self.cached_expiry or (
            self.time_utils.now_ist().date() + timedelta(days=7)
        ).isoformat()
        self.cached_expiry = expiry

        bullish_cycle = (self.mock_step // 4) % 2 == 0
        ce_oi_ladder = {}
        pe_oi_ladder = {}
        band_snapshots = []
        option_chain_data = {}
        total_call_oi = 0
        total_put_oi = 0
        total_call_volume = 0
        total_put_volume = 0

        for strike in [atm + i for i in range(-250, 251, 50)]:
            distance_steps = abs(strike - atm) // Config.STRIKE_GAP
            base_oi = max(150000, 420000 - (distance_steps * 35000))
            base_volume = max(800, 3200 - (distance_steps * 250))

            if bullish_cycle:
                ce_oi = int(base_oi * (0.92 - (distance_steps * 0.01)))
                pe_oi = int(base_oi * (1.08 + (0.03 if strike <= atm else 0)))
                ce_volume = int(base_volume * 0.85)
                pe_volume = int(base_volume * (1.35 if strike >= atm - Config.STRIKE_GAP else 1.15))
            else:
                ce_oi = int(base_oi * (1.08 + (0.03 if strike >= atm else 0)))
                pe_oi = int(base_oi * (0.92 - (distance_steps * 0.01)))
                ce_volume = int(base_volume * (1.35 if strike <= atm + Config.STRIKE_GAP else 1.15))
                pe_volume = int(base_volume * 0.85)

            distance_from_atm = int((strike - atm) / Config.STRIKE_GAP)
            ce_price = max(5, 180 - (distance_steps * 18))
            pe_price = max(5, 180 - (distance_steps * 18))

            ce = {
                "security_id": 100000 + strike,
                "oi": ce_oi,
                "volume": ce_volume,
                "last_price": ce_price,
                "implied_volatility": 12.5,
                "top_bid_price": round(max(1, ce_price - 0.8), 2),
                "top_ask_price": round(ce_price + 0.8, 2),
                "top_bid_quantity": 200,
                "top_ask_quantity": 200,
                "average_price": ce_price,
                "previous_oi": int(ce_oi * 0.96),
                "previous_volume": int(ce_volume * 0.9),
                "greeks": {"delta": 0.45, "theta": -8.0, "gamma": 0.02, "vega": 0.11},
            }
            pe = {
                "security_id": 200000 + strike,
                "oi": pe_oi,
                "volume": pe_volume,
                "last_price": pe_price,
                "implied_volatility": 12.8,
                "top_bid_price": round(max(1, pe_price - 0.8), 2),
                "top_ask_price": round(pe_price + 0.8, 2),
                "top_bid_quantity": 200,
                "top_ask_quantity": 200,
                "average_price": pe_price,
                "previous_oi": int(pe_oi * 0.96),
                "previous_volume": int(pe_volume * 0.9),
                "greeks": {"delta": -0.45, "theta": -8.0, "gamma": 0.02, "vega": 0.11},
            }

            option_chain_data[f"{strike:.6f}"] = {"ce": ce, "pe": pe}
            ce_oi_ladder[strike] = ce_oi
            pe_oi_ladder[strike] = pe_oi
            total_call_oi += ce_oi
            total_put_oi += pe_oi
            total_call_volume += ce_volume
            total_put_volume += pe_volume

            band_snapshots.extend([
                {
                    "atm_strike": atm,
                    "strike": strike,
                    "distance_from_atm": distance_from_atm,
                    "option_type": "CE",
                    "security_id": ce["security_id"],
                    "oi": ce_oi,
                    "volume": ce_volume,
                    "ltp": ce_price,
                    "iv": ce["implied_volatility"],
                    "top_bid_price": ce["top_bid_price"],
                    "top_bid_quantity": ce["top_bid_quantity"],
                    "top_ask_price": ce["top_ask_price"],
                    "top_ask_quantity": ce["top_ask_quantity"],
                    "spread": self._safe_spread(ce["top_bid_price"], ce["top_ask_price"]),
                    "average_price": ce["average_price"],
                    "previous_oi": ce["previous_oi"],
                    "previous_volume": ce["previous_volume"],
                    "delta": ce["greeks"]["delta"],
                    "theta": ce["greeks"]["theta"],
                    "gamma": ce["greeks"]["gamma"],
                    "vega": ce["greeks"]["vega"],
                },
                {
                    "atm_strike": atm,
                    "strike": strike,
                    "distance_from_atm": distance_from_atm,
                    "option_type": "PE",
                    "security_id": pe["security_id"],
                    "oi": pe_oi,
                    "volume": pe_volume,
                    "ltp": pe_price,
                    "iv": pe["implied_volatility"],
                    "top_bid_price": pe["top_bid_price"],
                    "top_bid_quantity": pe["top_bid_quantity"],
                    "top_ask_price": pe["top_ask_price"],
                    "top_ask_quantity": pe["top_ask_quantity"],
                    "spread": self._safe_spread(pe["top_bid_price"], pe["top_ask_price"]),
                    "average_price": pe["average_price"],
                    "previous_oi": pe["previous_oi"],
                    "previous_volume": pe["previous_volume"],
                    "delta": pe["greeks"]["delta"],
                    "theta": pe["greeks"]["theta"],
                    "gamma": pe["greeks"]["gamma"],
                    "vega": pe["greeks"]["vega"],
                },
            ])

        self.option_chain_data = option_chain_data
        atm_data = option_chain_data[f"{atm:.6f}"]
        pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else 0
        result = {
            "time": self.time_utils.current_time(),
            "expiry": expiry,
            "underlying_price": self.underlying_price,
            "atm": atm,
            "pcr": pcr,
            "ce_oi_ladder": ce_oi_ladder,
            "pe_oi_ladder": pe_oi_ladder,
            "band_snapshots": band_snapshots,
            "max_call_oi_strike": max(ce_oi_ladder, key=ce_oi_ladder.get),
            "max_put_oi_strike": max(pe_oi_ladder, key=pe_oi_ladder.get),
            "atm_ce_security_id": atm_data["ce"]["security_id"],
            "atm_pe_security_id": atm_data["pe"]["security_id"],
            "ce_ltp": atm_data["ce"]["last_price"],
            "pe_ltp": atm_data["pe"]["last_price"],
            "ce_oi": atm_data["ce"]["oi"],
            "pe_oi": atm_data["pe"]["oi"],
            "ce_volume": atm_data["ce"]["volume"],
            "pe_volume": atm_data["pe"]["volume"],
            "ce_volume_band": total_call_volume,
            "pe_volume_band": total_put_volume,
            "ce_iv": atm_data["ce"]["implied_volatility"],
            "pe_iv": atm_data["pe"]["implied_volatility"],
            "ce_top_bid_price": atm_data["ce"]["top_bid_price"],
            "ce_top_ask_price": atm_data["ce"]["top_ask_price"],
            "pe_top_bid_price": atm_data["pe"]["top_bid_price"],
            "pe_top_ask_price": atm_data["pe"]["top_ask_price"],
            "ce_spread": self._safe_spread(atm_data["ce"]["top_bid_price"], atm_data["ce"]["top_ask_price"]),
            "pe_spread": self._safe_spread(atm_data["pe"]["top_bid_price"], atm_data["pe"]["top_ask_price"]),
            "ce_delta": atm_data["ce"]["greeks"]["delta"],
            "pe_delta": atm_data["pe"]["greeks"]["delta"],
            "ce_theta": atm_data["ce"]["greeks"]["theta"],
            "pe_theta": atm_data["pe"]["greeks"]["theta"],
        }
        self.cached_data = result
        self.last_fetch_time = time.time()
        return result

    # -------------------------------------------------
    # Get nearest weekly expiry
    # -------------------------------------------------
    def get_expiry(self):
        if Config.TEST_MODE or Config.USE_MOCK_DATA:
            if not self.cached_expiry:
                self.cached_expiry = (
                    self.time_utils.now_ist().date() + timedelta(days=7)
                ).isoformat()
            return self.cached_expiry

        if self.cached_expiry:
            return self.cached_expiry

        payload = {
            "UnderlyingScrip": self.security_id,
            "UnderlyingSeg": "IDX_I"
        }

        try:
            response = self._post_json(self.expiry_url, payload, "Expiry")
            if response is None:
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
        if Config.TEST_MODE or Config.USE_MOCK_DATA:
            return self._build_mock_option_chain()

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
            "UnderlyingScrip": self.security_id,
            "UnderlyingSeg": "IDX_I",
            "Expiry": expiry
        }

        try:
            response = self._post_json(self.base_url, payload, "Option Chain")
            if response is None:
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
            band_snapshots = []

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

                distance_from_atm = int((strike - atm) / Config.STRIKE_GAP)
                band_snapshots.extend([
                    {
                        "atm_strike": atm,
                        "strike": strike,
                        "distance_from_atm": distance_from_atm,
                        "option_type": "CE",
                        "security_id": ce.get("security_id"),
                        "oi": ce.get("oi", 0),
                        "volume": ce.get("volume", 0),
                        "ltp": ce.get("last_price", 0),
                        "iv": ce.get("implied_volatility", 0),
                        "top_bid_price": ce.get("top_bid_price", 0),
                        "top_bid_quantity": ce.get("top_bid_quantity", 0),
                        "top_ask_price": ce.get("top_ask_price", 0),
                        "top_ask_quantity": ce.get("top_ask_quantity", 0),
                        "spread": self._safe_spread(ce.get("top_bid_price"), ce.get("top_ask_price")),
                        "average_price": ce.get("average_price", 0),
                        "previous_oi": ce.get("previous_oi", 0),
                        "previous_volume": ce.get("previous_volume", 0),
                        "delta": (ce.get("greeks") or {}).get("delta"),
                        "theta": (ce.get("greeks") or {}).get("theta"),
                        "gamma": (ce.get("greeks") or {}).get("gamma"),
                        "vega": (ce.get("greeks") or {}).get("vega"),
                    },
                    {
                        "atm_strike": atm,
                        "strike": strike,
                        "distance_from_atm": distance_from_atm,
                        "option_type": "PE",
                        "security_id": pe.get("security_id"),
                        "oi": pe.get("oi", 0),
                        "volume": pe.get("volume", 0),
                        "ltp": pe.get("last_price", 0),
                        "iv": pe.get("implied_volatility", 0),
                        "top_bid_price": pe.get("top_bid_price", 0),
                        "top_bid_quantity": pe.get("top_bid_quantity", 0),
                        "top_ask_price": pe.get("top_ask_price", 0),
                        "top_ask_quantity": pe.get("top_ask_quantity", 0),
                        "spread": self._safe_spread(pe.get("top_bid_price"), pe.get("top_ask_price")),
                        "average_price": pe.get("average_price", 0),
                        "previous_oi": pe.get("previous_oi", 0),
                        "previous_volume": pe.get("previous_volume", 0),
                        "delta": (pe.get("greeks") or {}).get("delta"),
                        "theta": (pe.get("greeks") or {}).get("theta"),
                        "gamma": (pe.get("greeks") or {}).get("gamma"),
                        "vega": (pe.get("greeks") or {}).get("vega"),
                    },
                ])

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
                "expiry": expiry,
                "underlying_price": self.underlying_price,
                "atm": atm,
                "pcr": round(pcr, 2),

                # OI Ladder
                "ce_oi_ladder": ce_oi_ladder,
                "pe_oi_ladder": pe_oi_ladder,
                "band_snapshots": band_snapshots,

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
                "ce_top_bid_price": atm_ce.get("top_bid_price", 0) if atm_ce else 0,
                "ce_top_ask_price": atm_ce.get("top_ask_price", 0) if atm_ce else 0,
                "pe_top_bid_price": atm_pe.get("top_bid_price", 0) if atm_pe else 0,
                "pe_top_ask_price": atm_pe.get("top_ask_price", 0) if atm_pe else 0,
                "ce_spread": self._safe_spread(
                    atm_ce.get("top_bid_price", 0) if atm_ce else 0,
                    atm_ce.get("top_ask_price", 0) if atm_ce else 0,
                ),
                "pe_spread": self._safe_spread(
                    atm_pe.get("top_bid_price", 0) if atm_pe else 0,
                    atm_pe.get("top_ask_price", 0) if atm_pe else 0,
                ),
                "ce_delta": (atm_ce.get("greeks") or {}).get("delta") if atm_ce else None,
                "pe_delta": (atm_pe.get("greeks") or {}).get("delta") if atm_pe else None,
                "ce_theta": (atm_ce.get("greeks") or {}).get("theta") if atm_ce else None,
                "pe_theta": (atm_pe.get("greeks") or {}).get("theta") if atm_pe else None,
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
