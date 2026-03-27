import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.atr import ATRCalculator

atr = ATRCalculator(period=5)

# Mock 5-min candles
candles = [
    {"high": 22450, "low": 22380, "close": 22400},
    {"high": 22480, "low": 22400, "close": 22470},
    {"high": 22520, "low": 22460, "close": 22500},
    {"high": 22540, "low": 22480, "close": 22510},
    {"high": 22560, "low": 22500, "close": 22540},
    {"high": 22580, "low": 22520, "close": 22560},
]

for c in candles:
    value = atr.update(c)
    print("ATR:", value)
    print("Buffer:", atr.get_buffer())
    print("Volatility:", atr.get_volatility())
    print("----------------------")