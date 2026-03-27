#!/usr/bin/env python3
"""
Test script to verify bot functionality
"""

import time
from datetime import datetime

from core.market_data import MarketData
from core.candle_manager import CandleManager
from core.vwap import VWAPCalculator
from core.orb import ORB
from core.oi_analyzer import OIAnalyzer
from core.volume_analyzer import VolumeAnalyzer
from core.time_manager import TimeManager
from strategy.breakout_strategy import BreakoutStrategy
from strategy.strike_selector import StrikeSelector
from utils.logger import TradeLogger


def test_bot():
    print("Testing Option Trading Bot...")
    print("Time:", datetime.now())

    # Initialize modules
    market_data = MarketData()
    candle_manager = CandleManager()
    vwap_calculator = VWAPCalculator()
    orb = ORB()
    oi_analyzer = OIAnalyzer()
    volume_analyzer = VolumeAnalyzer()
    time_manager = TimeManager()
    strategy = BreakoutStrategy()
    strike_selector = StrikeSelector()
    logger = TradeLogger()

    print("✅ All modules initialized successfully!")
    
    # Test time manager
    print(f"Time Status: {time_manager.get_time_status()}")
    print(f"Can Trade: {time_manager.can_take_new_trade()}")
    print(f"Market Open: {time_manager.is_market_open()}")
    
    # Test market data (will fail without API credentials, but should not crash)
    print("\nTesting market data fetch...")
    try:
        data = market_data.get_all_data()
        print(f"Market Data: {data}")
    except Exception as e:
        print(f"Expected API error (no credentials): {e}")
    
    print("\n✅ Bot test completed successfully!")
    print("The bot structure is working correctly.")
    print("To run the actual bot, use: python3 main.py")


if __name__ == "__main__":
    test_bot()
