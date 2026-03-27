from utils.time_utils import TimeUtils
from core.live_feed import LiveFeed
from core.option_chain import OptionChain
from engine.event_engine import EventEngine
from config import Config
from dhanhq import dhanhq
import time
import pandas as pd
import warnings
import os

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

FUTURE_ID_FILE = "data/nifty_future_id.txt"


# ===============================
# Get Nearest NIFTY Future ID
# ===============================
def get_nifty_future_security_id():
    if os.path.exists(FUTURE_ID_FILE):
        with open(FUTURE_ID_FILE, "r") as f:
            print("Loaded FUT ID from file")
            return int(f.read().strip())

    print("Fetching FUT ID from Dhan...")
    dhan = dhanhq(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)
    df = dhan.fetch_security_list("compact")

    fut = df[df["SEM_INSTRUMENT_NAME"] == "FUTIDX"]
    fut = fut[fut["SEM_TRADING_SYMBOL"].str.contains("NIFTY")]
    fut = fut.sort_values("SEM_EXPIRY_DATE")

    nearest = fut.iloc[0]
    fut_id = int(nearest["SEM_SMST_SECURITY_ID"])

    os.makedirs("data", exist_ok=True)
    with open(FUTURE_ID_FILE, "w") as f:
        f.write(str(fut_id))

    print("Saved FUT ID:", fut_id)
    return fut_id


# ===============================
# Generate ATM ± Strikes
# ===============================
def generate_strikes(atm, step=50, count=5):
    return [atm + i * step for i in range(-count, count + 1)]


# ===============================
# MAIN
# ===============================
def main():
    time_utils = TimeUtils()

    print("====================================")
    print("      OPTION TRADING BOT STARTED    ")
    print("      Time (IST):", time_utils.current_time())
    print("====================================")

    option_chain = OptionChain()

    print("Fetching Option Chain...")
    option_data = None

    while option_data is None:
        option_data = option_chain.fetch_option_chain()
        time.sleep(2)

    atm = option_data["atm"]
    print("ATM Strike:", atm)

    # Generate ATM ± 5 strikes
    strike_list = generate_strikes(atm)
    print("Subscribing Strikes:", strike_list)

    ce_ids = []
    pe_ids = []

    for strike in strike_list:
        data = option_chain.get_security_id_by_strike(strike)
        if data:
            if data["ce"]:
                ce_ids.append(data["ce"])
            if data["pe"]:
                pe_ids.append(data["pe"])
        else:
            print("Security ID not found for strike:", strike)

    print("Total CE Instruments:", len(ce_ids))
    print("Total PE Instruments:", len(pe_ids))

    # Step 2 - Get Futures ID
    print("Fetching NIFTY Futures Security ID...")
    fut_id = get_nifty_future_security_id()
    print("FUT Security ID:", fut_id)

    if fut_id is None:
        print("Error: Futures ID not found")
        return

    # ===============================
    # Save mapping in Config
    # ===============================
    Config.NIFTY_FUTURE_ID = fut_id
    Config.STRIKE_MAP = {}

    for strike, ce, pe in zip(strike_list, ce_ids, pe_ids):
        Config.STRIKE_MAP[int(ce)] = {"strike": strike, "type": "CE"}
        Config.STRIKE_MAP[int(pe)] = {"strike": strike, "type": "PE"}

    # ===============================
    # WebSocket Instruments
    # ===============================
    instruments = []

    # Index
    instruments.append({"ExchangeSegment": "IDX_I", "SecurityId": "13"})

    # Futures
    instruments.append({"ExchangeSegment": "NSE_FNO", "SecurityId": str(fut_id)})

    # CE
    for ce in ce_ids:
        instruments.append({"ExchangeSegment": "NSE_FNO", "SecurityId": str(ce)})

    # PE
    for pe in pe_ids:
        instruments.append({"ExchangeSegment": "NSE_FNO", "SecurityId": str(pe)})

    print("Total Instruments Subscribed:", len(instruments))

    # Step 3 - Start WebSocket
    print("Starting WebSocket...")
    live_feed = LiveFeed(instruments)
    live_feed.connect()

    time.sleep(5)

    # Step 4 - Start Event Engine
    engine = EventEngine(live_feed)
    engine.run()


if __name__ == "__main__":
    main()