import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.volume_analyzer import VolumeAnalyzer
from datetime import datetime

va = VolumeAnalyzer()

# Mock 5-min volumes
volumes = [
    {"time": datetime(2026, 4, 6, 9, 15), "volume": 1000},
    {"time": datetime(2026, 4, 6, 9, 20), "volume": 1200},
    {"time": datetime(2026, 4, 6, 9, 25), "volume": 900},
    {"time": datetime(2026, 4, 6, 9, 30), "volume": 1500},
    {"time": datetime(2026, 4, 6, 9, 35), "volume": 2000},
    {"time": datetime(2026, 4, 6, 9, 40), "volume": 1800},
    {"time": datetime(2026, 4, 6, 9, 45), "volume": 2200},
]

for candle in volumes:
    avg = va.update(candle)
    signal = va.get_volume_signal(candle["volume"])
    ratio = va.get_volume_ratio(candle["volume"])

    print("Volume:", candle["volume"])
    print("Avg Volume:", avg)
    print("Signal:", signal)
    print("Ratio:", ratio)
    print("--------------------")
