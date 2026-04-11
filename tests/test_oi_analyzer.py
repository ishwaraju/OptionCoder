import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.market.oi_analyzer import OIAnalyzer

oi = OIAnalyzer()

# Mock data
data = [
    (22400, 1000000, 1200000),
    (22450, 1050000, 1300000),  # Price up, Put OI up → Long buildup
    (22480, 1040000, 1280000),  # Price up, Call OI down → Short covering
    (22420, 1100000, 1250000),  # Price down, Call OI up → Short buildup
]

for price, call_oi, put_oi in data:
    oi.update(price, call_oi, put_oi)

    print("Price:", price)
    print("Call OI:", call_oi)
    print("Put OI:", put_oi)
    print("Signal:", oi.get_oi_signal())
    print("Bias:", oi.get_bias())
    print("Strength:", oi.get_oi_strength())
    print("-------------------------")