from config import Config
from shared.utils.future_id_cache import FutureIdCache


def get_instrument_profile(instrument=None):
    symbol = (instrument or Config.SYMBOL or "NIFTY").upper()
    future_id_cache = FutureIdCache()
    return {
        "instrument": symbol,
        "security_id": Config.SECURITY_IDS.get(symbol),
        "strike_step": Config.STRIKE_STEP.get(symbol, 50),
        "lot_size": Config.LOT_SIZE.get(symbol, 50),
        "future_id": future_id_cache.get(symbol),
    }
