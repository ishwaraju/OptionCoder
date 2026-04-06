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
oi_trend = "BULLISH"
build_up = "LONG_BUILDUP"
can_trade = True
buffer = 10
pressure_metrics = {
    "pressure_bias": "BULLISH",
    "atm_ce_concentration": 0.25,
    "atm_pe_concentration": 0.12,
}

signal, reason = strategy.generate_signal(
    price=price,
    orb_high=orb_high,
    orb_low=orb_low,
    vwap=vwap,
    volume_signal=volume_signal,
    oi_bias=oi_bias,
    oi_trend=oi_trend,
    build_up=build_up,
    can_trade=can_trade,
    buffer=buffer,
    pressure_metrics=pressure_metrics,
)

print("Signal:", signal)
print("Reason:", reason)
print("Score:", strategy.last_score)
