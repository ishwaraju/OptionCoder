from config import Config


def get_instrument_profile(instrument=None):
    symbol = (instrument or Config.SYMBOL or "NIFTY").upper()
    return {
        "instrument": symbol,
        "security_id": Config.SECURITY_IDS.get(symbol),
        "strike_step": Config.STRIKE_STEP.get(symbol, 50),
        "lot_size": Config.LOT_SIZE.get(symbol, 50),
    }
