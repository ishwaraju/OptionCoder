import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.volume_analyzer import VolumeAnalyzer

va = VolumeAnalyzer()

# Mock 5-min volumes
volumes = [1000, 1200, 900, 1500, 2000, 1800, 2200]

for v in volumes:
    avg = va.update({"volume": v})
    signal = va.get_volume_signal(v)
    ratio = va.get_volume_ratio(v)

    print("Volume:", v)
    print("Avg Volume:", avg)
    print("Signal:", signal)
    print("Ratio:", ratio)
    print("--------------------")