import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.market_data import MarketData

md = MarketData()

price = md.get_nifty_price()
print("NIFTY:", price)

if price is not None:
    print("ATM Strike:", md.get_atm_strike(price))
    print("PCR:", md.get_pcr())
    atm_data = md.get_atm_option_data(price)
    print("ATM Data:", atm_data)
else:
    print("Could not get NIFTY price - market might be closed or API issue")
    print("Testing with sample price...")
    sample_price = 20000
    print("Sample ATM Strike:", md.get_atm_strike(sample_price))
