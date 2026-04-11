"""
NIFTY Specific Configuration
"""

import os
from dotenv import load_dotenv

load_dotenv()


class NiftyConfig:
    """NIFTY specific configuration"""

    # ==============================
    # NIFTY SYMBOL SETTINGS
    # ==============================
    SYMBOL = "NIFTY"
    SECURITY_ID = 13
    LOT_SIZE = 65
    STRIKE_STEP = 50
    DEFAULT_QUANTITY = 65

    # ==============================
    # NIFTY TRADING PARAMETERS
    # ==============================
    # Risk Management
    MAX_RISK_PERCENT = float(os.getenv('NIFTY_MAX_RISK_PERCENT', '2.0'))
    STOP_LOSS_PERCENT = float(os.getenv('NIFTY_STOP_LOSS_PERCENT', '25.0'))
    TARGET_PERCENT = float(os.getenv('NIFTY_TARGET_PERCENT', '50.0'))
    TRAIL_PERCENT = float(os.getenv('NIFTY_TRAIL_PERCENT', '15.0'))

    # Trading Limits
    MAX_TRADES_PER_DAY = int(os.getenv('NIFTY_MAX_TRADES_PER_DAY', '2'))
    MAX_LOSS_STREAK = int(os.getenv('NIFTY_MAX_LOSS_STREAK', '2'))
    DAILY_LOSS_LIMIT = float(os.getenv('NIFTY_DAILY_LOSS_LIMIT', '2000'))

    # ==============================
    # NIFTY STRATEGY PARAMETERS
    # ==============================
    # ATR Settings
    USE_ATR = os.getenv('NIFTY_USE_ATR', 'True').lower() == 'true'
    ATR_PERIOD = int(os.getenv('NIFTY_ATR_PERIOD', '5'))
    ATR_MULTIPLIER = float(os.getenv('NIFTY_ATR_MULTIPLIER', '0.3'))
    MIN_BUFFER = int(os.getenv('NIFTY_MIN_BUFFER', '10'))
    MAX_BUFFER = int(os.getenv('NIFTY_MAX_BUFFER', '25'))
    FALLBACK_BUFFER = int(os.getenv('NIFTY_FALLBACK_BUFFER', '15'))

    # Signal Parameters
    MIN_SCORE_THRESHOLD = float(os.getenv('NIFTY_MIN_SCORE_THRESHOLD', '55.0'))
    MIN_HIGH_QUALITY_SCORE = float(os.getenv('NIFTY_MIN_HIGH_QUALITY_SCORE', '65.0'))
    AGGRESSIVE_MODE = os.getenv('NIFTY_AGGRESSIVE_MODE', 'False').lower() == 'true'

    # ==============================
    # NIFTY SPREAD FILTER SETTINGS
    # ==============================
    MAX_SPREAD_PERCENT = float(os.getenv('NIFTY_MAX_SPREAD_PERCENT', '7.0'))
    MIN_BID_QUANTITY = int(os.getenv('NIFTY_MIN_BID_QUANTITY', '50'))
    MIN_ASK_QUANTITY = int(os.getenv('NIFTY_MIN_ASK_QUANTITY', '50'))
    MIN_SPREAD_RUPEES = float(os.getenv('NIFTY_MIN_SPREAD_RUPEES', '0.3'))

    # ==============================
    # NIFTY OI/QUOTE CONFIRMATION SETTINGS
    # ==============================
    MIN_OI_CHANGE_PERCENT = float(os.getenv('NIFTY_MIN_OI_CHANGE_PERCENT', '8.0'))
    MIN_VOLUME_THRESHOLD = int(os.getenv('NIFTY_MIN_VOLUME_THRESHOLD', '800'))
    MAX_QUOTE_AGE_SECONDS = int(os.getenv('NIFTY_MAX_QUOTE_AGE_SECONDS', '45'))
    OI_CONFIRMATION_WINDOW = int(os.getenv('NIFTY_OI_CONFIRMATION_WINDOW', '5'))

    # ==============================
    # NIFTY TRADING TIME (IST)
    # ==============================
    TRADE_START_TIME = os.getenv('NIFTY_TRADE_START_TIME', '09:30')
    TRADE_END_TIME = os.getenv('NIFTY_TRADE_END_TIME', '15:30')
    FORCE_EXIT_TIME = os.getenv('NIFTY_FORCE_EXIT_TIME', '14:59')

    # No Trade Zone
    NO_TRADE_START = os.getenv('NIFTY_NO_TRADE_START', '14:59')
    NO_TRADE_END = os.getenv('NIFTY_NO_TRADE_END', '15:30')

    # ORB Timing
    ORB_START = os.getenv('NIFTY_ORB_START', '09:15')
    ORB_END = os.getenv('NIFTY_ORB_END', '09:30')

    @classmethod
    def get_instrument_config(cls):
        """Get NIFTY instrument configuration"""
        return {
            "symbol": cls.SYMBOL,
            "security_id": cls.SECURITY_ID,
            "lot_size": cls.LOT_SIZE,
            "strike_step": cls.STRIKE_STEP,
            "default_quantity": cls.DEFAULT_QUANTITY,
            "max_risk_percent": cls.MAX_RISK_PERCENT,
            "stop_loss_percent": cls.STOP_LOSS_PERCENT,
            "target_percent": cls.TARGET_PERCENT,
            "min_score_threshold": cls.MIN_SCORE_THRESHOLD,
        }
