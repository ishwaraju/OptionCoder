import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.indicators.orb import ORB
from datetime import datetime

orb = ORB()

candles = [
    {"time": datetime(2026, 3, 25, 9, 15), "high": 22450, "low": 22380},
    {"time": datetime(2026, 3, 25, 9, 20), "high": 22480, "low": 22400},
    {"time": datetime(2026, 3, 25, 9, 25), "high": 22470, "low": 22410},
]

for c in candles:
    orb.add_candle(c)

high, low = orb.calculate_orb()

print("ORB High:", high)
print("ORB Low:", low)

# Test breakout
price = 22500
print("Breakout:", orb.check_breakout(price, buffer=10))