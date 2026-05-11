import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.indicators.vwap import VWAPCalculator
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


def test_vwap_falls_back_to_typical_price_average_when_volume_is_zero():
    calc = VWAPCalculator()

    first = calc.update({"time": datetime(2026, 5, 11, 10, 10), "high": 110, "low": 100, "close": 105, "volume": 0})
    second = calc.update({"time": datetime(2026, 5, 11, 10, 15), "high": 120, "low": 108, "close": 114, "volume": 0})

    assert first == 105
    assert round(second, 2) == 109.5
