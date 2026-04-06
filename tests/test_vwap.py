import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.vwap import VWAPCalculator
from datetime import datetime

vwap = VWAPCalculator()

# Mock 5-min candles
candles = [
    {"time": datetime(2026, 4, 6, 9, 15), "high": 100, "low": 90, "close": 95, "volume": 1000},
    {"time": datetime(2026, 4, 6, 9, 20), "high": 105, "low": 95, "close": 100, "volume": 1200},
    {"time": datetime(2026, 4, 6, 9, 25), "high": 110, "low": 100, "close": 108, "volume": 900},
    {"time": datetime(2026, 4, 6, 9, 30), "high": 115, "low": 105, "close": 110, "volume": 800},
]

for c in candles:
    v = vwap.update(c)
    print("VWAP:", v)

print("Signal:", vwap.get_vwap_signal(price=112))
