import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategy.breakout_strategy import BreakoutStrategy

strategy = BreakoutStrategy()

# Mock values
price = 22500
orb_high = 22450
orb_low = 22380
vwap = 22420
volume_signal = "STRONG"
oi_bias = "BULLISH"
can_trade = True
buffer = 10

signal, reason = strategy.generate_signal(
    price,
    orb_high,
    orb_low,
    vwap,
    volume_signal,
    oi_bias,
    can_trade,
    buffer
)

print("Signal:", signal)
print("Reason:", reason)