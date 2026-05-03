"""
Configuration Package
"""

from .main_config import MainConfig
from .nifty_config import NiftyConfig
from .sensex_config import SensexConfig
from .banknifty_config import BankNiftyConfig
from .db_config import DBConfig
from .risk_profiles import get_risk_profile_matrix

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


def get_scalp_config_for_instrument(instrument):
    """
    Get instrument-specific scalping configuration
    
    Args:
        instrument (str): Instrument symbol (NIFTY, SENSEX, BANKNIFTY, etc.)
    
    Returns:
        dict: Scalping config with target, stop, min_atr for the instrument
    """
    from .main_config import MainConfig
    
    instrument = instrument.upper()
    
    if instrument == "NIFTY":
        return {
            'target': MainConfig.NIFTY_SCALP_TARGET,
            'stop': MainConfig.NIFTY_SCALP_STOP,
            'min_atr': MainConfig.NIFTY_SCALP_MIN_ATR,
            'min_score': MainConfig.SCALP_MIN_SCORE,
            'cooldown': MainConfig.SCALP_COOLDOWN_SECONDS,
            'max_hold': MainConfig.SCALP_MAX_HOLD_MINUTES
        }
    elif instrument == "BANKNIFTY":
        return {
            'target': MainConfig.BANKNIFTY_SCALP_TARGET,
            'stop': MainConfig.BANKNIFTY_SCALP_STOP,
            'min_atr': MainConfig.BANKNIFTY_SCALP_MIN_ATR,
            'min_score': MainConfig.SCALP_MIN_SCORE,
            'cooldown': MainConfig.SCALP_COOLDOWN_SECONDS,
            'max_hold': MainConfig.SCALP_MAX_HOLD_MINUTES
        }
    elif instrument == "SENSEX":
        return {
            'target': MainConfig.SENSEX_SCALP_TARGET,
            'stop': MainConfig.SENSEX_SCALP_STOP,
            'min_atr': MainConfig.SENSEX_SCALP_MIN_ATR,
            'min_score': MainConfig.SCALP_MIN_SCORE,
            'cooldown': MainConfig.SCALP_COOLDOWN_SECONDS,
            'max_hold': MainConfig.SCALP_MAX_HOLD_MINUTES
        }
    else:
        # Default to NIFTY settings for unknown instruments
        return {
            'target': MainConfig.NIFTY_SCALP_TARGET,
            'stop': MainConfig.NIFTY_SCALP_STOP,
            'min_atr': MainConfig.NIFTY_SCALP_MIN_ATR,
            'min_score': MainConfig.SCALP_MIN_SCORE,
            'cooldown': MainConfig.SCALP_COOLDOWN_SECONDS,
            'max_hold': MainConfig.SCALP_MAX_HOLD_MINUTES
        }
