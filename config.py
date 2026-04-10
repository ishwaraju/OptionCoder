"""
Configuration file for Trading Bot
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration class for trading bot"""

    # ==============================
    # DHAN API CREDENTIALS
    # ==============================
    DHAN_CLIENT_ID = os.getenv('DHAN_CLIENT_ID', '')
    DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', '')

    # ==============================
    # DATA MODE
    # ==============================
    USE_MOCK_DATA = os.getenv('USE_MOCK_DATA', 'True').lower() == 'true'
    PAPER_TRADE = os.getenv('PAPER_TRADE', 'True').lower() == 'true'
    TEST_MODE = os.getenv('TEST_MODE', 'True').lower() == 'true'

    # ==============================
    # SYMBOL SETTINGS
    # ==============================
    SYMBOL = os.getenv('SYMBOL', 'NIFTY')

    LOT_SIZE = {
        "NIFTY": 65,
        "BANKNIFTY": 30,
        "FINNIFTY": 40,
        "MIDCPNIFTY": 75
    }

    STRIKE_STEP = {
        "NIFTY": 50,
        "BANKNIFTY": 100,
        "FINNIFTY": 50,
        "MIDCPNIFTY": 25
    }

    DEFAULT_QUANTITY = LOT_SIZE.get(SYMBOL, 50)
    STRIKE_GAP = STRIKE_STEP.get(SYMBOL, 50)

    # ==============================
    # DHAN SECURITY IDS
    # ==============================
    SECURITY_IDS = {
        "NIFTY": 13,
        "BANKNIFTY": 25,
        "FINNIFTY": 27,
        "MIDCPNIFTY": 442  # verify from instrument list
    }

    # ==============================
    # ATR BUFFER SETTINGS
    # ==============================
    USE_ATR = os.getenv('USE_ATR', 'True').lower() == 'true'
    ATR_PERIOD = int(os.getenv('ATR_PERIOD', '5'))
    ATR_MULTIPLIER = float(os.getenv('ATR_MULTIPLIER', '0.3'))
    MIN_BUFFER = int(os.getenv('MIN_BUFFER', '10'))
    MAX_BUFFER = int(os.getenv('MAX_BUFFER', '25'))

    # Fallback buffer if ATR not ready
    FALLBACK_BUFFER = int(os.getenv('FALLBACK_BUFFER', '15'))

    # ==============================
    # RISK MANAGEMENT
    # ==============================
    MAX_RISK_PERCENT = float(os.getenv('MAX_RISK_PERCENT', '2.0'))
    STOP_LOSS_PERCENT = float(os.getenv('STOP_LOSS_PERCENT', '25.0'))
    TARGET_PERCENT = float(os.getenv('TARGET_PERCENT', '50.0'))
    TRAIL_PERCENT = float(os.getenv('TRAIL_PERCENT', '15.0'))

    MAX_TRADES_PER_DAY = int(os.getenv('MAX_TRADES_PER_DAY', '2'))
    MAX_LOSS_STREAK = int(os.getenv('MAX_LOSS_STREAK', '2'))
    DAILY_LOSS_LIMIT = float(os.getenv('DAILY_LOSS_LIMIT', '2000'))

    # ==============================
    # TRADING TIME (IST)
    # ==============================
    TRADE_START_TIME = os.getenv('TRADE_START_TIME', '09:30')
    TRADE_END_TIME = os.getenv('TRADE_END_TIME', '15:30')
    FORCE_EXIT_TIME = os.getenv('FORCE_EXIT_TIME', '14:59')

    # No Trade Zone
    NO_TRADE_START = os.getenv('NO_TRADE_START', '14:59')
    NO_TRADE_END = os.getenv('NO_TRADE_END', '15:30')

    # ORB Timing
    ORB_START = os.getenv('ORB_START', '09:15')
    ORB_END = os.getenv('ORB_END', '09:30')

    # ==============================
    # BOT SHUTDOWN SETTINGS (IST)
    # ==============================
    BOT_SHUTDOWN_TIME = os.getenv('BOT_SHUTDOWN_TIME', '15:40')  # 3:40 PM IST
    AUTO_SYSTEM_SHUTDOWN = os.getenv('AUTO_SYSTEM_SHUTDOWN', 'True').lower() == 'true'
    SHUTDOWN_GRACE_PERIOD = int(os.getenv('SHUTDOWN_GRACE_PERIOD', '300'))  # 5 minutes grace period
    SYSTEM_SHUTDOWN_DELAY = int(os.getenv('SYSTEM_SHUTDOWN_DELAY', '60'))  # 1 minute after bot shutdown

    # ==============================
    # DATA FETCH SETTINGS
    # ==============================
    VWAP_TIMEFRAME = int(os.getenv('VWAP_TIMEFRAME', '5'))
    OI_FETCH_INTERVAL = int(os.getenv('OI_FETCH_INTERVAL', '300'))  # seconds
    PRICE_FETCH_INTERVAL = int(os.getenv('PRICE_FETCH_INTERVAL', '5'))
    LIVE_DATA_STALE_SECONDS = int(os.getenv('LIVE_DATA_STALE_SECONDS', '8'))
    STALE_FEED_FORCE_RECONNECT_SECONDS = int(os.getenv('STALE_FEED_FORCE_RECONNECT_SECONDS', '30'))
    AUTO_SWITCH_TO_MOCK_AFTER_CLOSE = os.getenv('AUTO_SWITCH_TO_MOCK_AFTER_CLOSE', 'False').lower() == 'true'
    SIGNAL_COOLDOWN_BARS = int(os.getenv('SIGNAL_COOLDOWN_BARS', '2'))
    RECONNECT_COOLDOWN_BARS = int(os.getenv('RECONNECT_COOLDOWN_BARS', '2'))
    OPTION_CHAIN_TIMEOUT = int(os.getenv('OPTION_CHAIN_TIMEOUT', '6'))
    OPTION_CHAIN_RETRIES = int(os.getenv('OPTION_CHAIN_RETRIES', '3'))
    STATE_RECOVERY_5M_BARS = int(os.getenv('STATE_RECOVERY_5M_BARS', '24'))
    SIGNAL_VALIDITY_MINUTES = int(os.getenv('SIGNAL_VALIDITY_MINUTES', '4'))
    ENABLE_1M_TRIGGER = os.getenv('ENABLE_1M_TRIGGER', 'True').lower() == 'true'
    ENTRY_TRIGGER_VALIDITY_MINUTES = int(os.getenv('ENTRY_TRIGGER_VALIDITY_MINUTES', '2'))
    ENTRY_TRIGGER_MIN_BODY = int(os.getenv('ENTRY_TRIGGER_MIN_BODY', '5'))
    ALLOW_CONTINUATION_ENTRY = os.getenv('ALLOW_CONTINUATION_ENTRY', 'False').lower() == 'true'

    # ==============================
    # SPREAD FILTER SETTINGS (Tuned for Option Buyer)
    # ==============================
    MAX_SPREAD_PERCENT = float(os.getenv('MAX_SPREAD_PERCENT', '7.0'))  # Increased from 5.0 to 7.0
    MIN_BID_QUANTITY = int(os.getenv('MIN_BID_QUANTITY', '50'))  # Reduced from 100 to 50
    MIN_ASK_QUANTITY = int(os.getenv('MIN_ASK_QUANTITY', '50'))  # Reduced from 100 to 50
    MIN_SPREAD_RUPEES = float(os.getenv('MIN_SPREAD_RUPEES', '0.3'))  # Reduced from 0.5 to 0.3

    # ==============================
    # OI/QUOTE CONFIRMATION SETTINGS (Tuned for Option Buyer)
    # ==============================
    MIN_OI_CHANGE_PERCENT = float(os.getenv('MIN_OI_CHANGE_PERCENT', '8.0'))  # Reduced from 10.0 to 8.0
    MIN_VOLUME_THRESHOLD = int(os.getenv('MIN_VOLUME_THRESHOLD', '800'))  # Reduced from 1000 to 800
    MAX_QUOTE_AGE_SECONDS = int(os.getenv('MAX_QUOTE_AGE_SECONDS', '45'))  # Increased from 30 to 45
    OI_CONFIRMATION_WINDOW = int(os.getenv('OI_CONFIRMATION_WINDOW', '5'))  # Increased from 3 to 5

    # ==============================
    # MINIMUM SCORE THRESHOLD (Tuned for Option Buyer)
    # ==============================
    MIN_SCORE_THRESHOLD = float(os.getenv('MIN_SCORE_THRESHOLD', '55.0'))  # Reduced from 60 to 55
    MIN_HIGH_QUALITY_SCORE = float(os.getenv('MIN_HIGH_QUALITY_SCORE', '65.0'))  # Reduced from 70 to 65
    AGGRESSIVE_MODE = os.getenv('AGGRESSIVE_MODE', 'False').lower() == 'true'

    # ==============================
    # DATABASE SETTINGS (PostgreSQL)
    # ==============================
    DB_ENABLED = os.getenv('DB_ENABLED', 'True').lower() == 'true'
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))
    DB_NAME = os.getenv('DB_NAME', 'optioncoder')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', '5'))

    # sslmode examples: disable / prefer / require
    DB_SSLMODE = os.getenv('DB_SSLMODE', 'prefer')

    # ==============================
    # APP SETTINGS
    # ==============================
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    CONSOLE_MODE = os.getenv('CONSOLE_MODE', 'DETAILED').upper()
    ENABLE_ALERTS = os.getenv('ENABLE_ALERTS', 'True').lower() == 'true'
    ENABLE_SOUND_ALERT = os.getenv('ENABLE_SOUND_ALERT', 'True').lower() == 'true'
    TELEGRAM_ENABLED = os.getenv('TELEGRAM_ENABLED', 'False').lower() == 'true'
    TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    ENABLE_WATCHDOG = os.getenv('ENABLE_WATCHDOG', 'True').lower() == 'true'
    WATCHDOG_STALE_SECONDS = int(os.getenv('WATCHDOG_STALE_SECONDS', '90'))
    WATCHDOG_CHECK_INTERVAL = int(os.getenv('WATCHDOG_CHECK_INTERVAL', '10'))
    WATCHDOG_MAX_RESTARTS = int(os.getenv('WATCHDOG_MAX_RESTARTS', '5'))
    WATCHDOG_RESTART_WINDOW_SECONDS = int(os.getenv('WATCHDOG_RESTART_WINDOW_SECONDS', '1800'))

    # ==============================
    # VALIDATION
    # ==============================
    @classmethod
    def validate_credentials(cls):
        """Validate if API credentials are set"""
        if not cls.DHAN_CLIENT_ID or not cls.DHAN_ACCESS_TOKEN:
            return False
        return True

    @classmethod
    def get_db_dsn(cls):
        """
        Build PostgreSQL DSN string.
        Example:
        host=localhost port=5432 dbname=optioncoder user=postgres password=... sslmode=prefer
        """
        dsn_parts = [
            f"host={cls.DB_HOST}",
            f"port={cls.DB_PORT}",
            f"dbname={cls.DB_NAME}",
            f"user={cls.DB_USER}",
            f"connect_timeout={cls.DB_CONNECT_TIMEOUT}",
            f"sslmode={cls.DB_SSLMODE}",
        ]

        if cls.DB_PASSWORD:
            dsn_parts.append(f"password={cls.DB_PASSWORD}")

        return " ".join(dsn_parts)

    @classmethod
    def print_config(cls):
        """Print important config values"""
        print("\n========== BOT CONFIG ==========")
        print("Symbol:", cls.SYMBOL)
        print("Lot Size:", cls.DEFAULT_QUANTITY)
        print("Strike Gap:", cls.STRIKE_GAP)
        print("Mock Mode:", cls.USE_MOCK_DATA)
        print("Paper Trade:", cls.PAPER_TRADE)
        print("ATR Enabled:", cls.USE_ATR)
        print("Max Trades/Day:", cls.MAX_TRADES_PER_DAY)
        print("SL %:", cls.STOP_LOSS_PERCENT)
        print("Target %:", cls.TARGET_PERCENT)
        print("DB Enabled:", cls.DB_ENABLED)
        print("DB Name:", cls.DB_NAME)
        print("DB Host:", cls.DB_HOST)
        print("================================\n")
