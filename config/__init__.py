"""
Configuration Package
"""

from .main_config import MainConfig
from .nifty_config import NiftyConfig
from .sensex_config import SensexConfig
from .banknifty_config import BankNiftyConfig
from .db_config import DBConfig

# For backward compatibility
Config = MainConfig


def get_config_for_instrument(instrument):
    """
    Get the appropriate config class for the given instrument
    
    Args:
        instrument (str): Instrument symbol (NIFTY, SENSEX, BANKNIFTY, etc.)
    
    Returns:
        Config class: Specific config class for the instrument
    """
    instrument = instrument.upper()
    
    if instrument == "NIFTY":
        return NiftyConfig
    elif instrument == "SENSEX":
        return SensexConfig
    elif instrument == "BANKNIFTY":
        return BankNiftyConfig
    else:
        # Fallback to main config for other instruments
        return MainConfig
