import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategy.strike_selector import StrikeSelector

ss = StrikeSelector()

price = 22485

print("ATM CE:", ss.select_strike(price, "CE", "STRONG"))
print("ATM PE:", ss.select_strike(price, "PE", "STRONG"))

print("Low Volume CE:", ss.select_strike(price, "CE", "WEAK"))
print("Low Volume PE:", ss.select_strike(price, "PE", "WEAK"))