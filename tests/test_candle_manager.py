import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.candle_manager import CandleManager
import random
from datetime import datetime, timedelta

cm = CandleManager()

# Mock start time (IST 9:15)
start_time = datetime.strptime("2026-03-25 09:15:00", "%Y-%m-%d %H:%M:%S")

price = 22400

print("Starting Mock Candle Test...\n")

for i in range(20):  # 20 minutes data
    current_time = start_time + timedelta(minutes=i)

    # Simulate price movement
    open_price = price
    high_price = price + random.randint(5, 20)
    low_price = price - random.randint(5, 20)
    close_price = random.randint(low_price, high_price)
    volume = random.randint(1000, 5000)

    price = close_price

    minute_candle = {
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "datetime": current_time.strftime("%Y-%m-%d %H:%M:%S")
    }

    completed = cm.add_minute_candle(minute_candle)

    print(f"1-min: {minute_candle['datetime']} O:{open_price} H:{high_price} L:{low_price} C:{close_price}")

    if completed:
        print("\n>>> 5-MIN CANDLE COMPLETED <<<")
        print(completed)
        print("--------------------------------\n")

print("\nAll 5-min Candles:")
for c in cm.get_all_candles():
    print(c)