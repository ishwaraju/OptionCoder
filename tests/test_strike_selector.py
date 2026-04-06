import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategy.strike_selector import StrikeSelector

ss = StrikeSelector()

price = 22485
strong_bullish_pressure = {
    "pressure_bias": "BULLISH",
    "near_call_pressure_ratio": 1.35,
    "near_put_pressure_ratio": 0.74,
    "strongest_ce_strike": 22500,
    "strongest_pe_strike": 22450,
}
strong_bearish_pressure = {
    "pressure_bias": "BEARISH",
    "near_call_pressure_ratio": 0.71,
    "near_put_pressure_ratio": 1.41,
    "strongest_ce_strike": 22450,
    "strongest_pe_strike": 22500,
}
weak_pressure = {
    "pressure_bias": "NEUTRAL",
    "near_call_pressure_ratio": 1.02,
    "near_put_pressure_ratio": 0.98,
    "strongest_ce_strike": 22550,
    "strongest_pe_strike": 22400,
}

print("High Score CE:", ss.select_strike(price, "CE", "STRONG", 90, strong_bullish_pressure))
print("High Score PE:", ss.select_strike(price, "PE", "STRONG", 90, strong_bearish_pressure))
print("Weak Volume CE:", ss.select_strike(price, "CE", "WEAK", 68, weak_pressure))
print("Weak Pressure PE:", ss.select_strike(price, "PE", "NORMAL", 72, weak_pressure))
