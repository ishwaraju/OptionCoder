from utils.time_utils import TimeUtils
from core.live_feed import LiveFeed
from core.option_chain import OptionChain
from engine.event_engine import EventEngine
from config import Config
from dhanhq import dhanhq
from utils.runtime_watchdog import RuntimeWatchdog
import time
import pandas as pd
import warnings
import os
import sys
import requests

warnings.simplefilter(action='ignore', category=pd.errors.DtypeWarning)

FUTURE_ID_FILE = "data/nifty_future_id.txt"
HEARTBEAT_FILE = "data/runtime_heartbeat.json"
WATCHDOG_STATE_FILE = "data/watchdog_state.json"


# ===============================
# Get Nearest NIFTY Future ID
# ===============================
def get_nifty_future_security_id():
    def _fetch_nearest_future_id(dhan_client):
        raw = dhan_client.fetch_security_list("compact")

        # Support both direct DataFrame response and dict-style response
        if isinstance(raw, pd.DataFrame):
            df = raw
        elif isinstance(raw, dict) and "data" in raw:
            df = pd.DataFrame(raw["data"])
        else:
            raise ValueError("Unexpected response format from fetch_security_list")

        fut = df[df["SEM_INSTRUMENT_NAME"] == "FUTIDX"]
        fut = fut[fut["SEM_TRADING_SYMBOL"].astype(str).str.contains("NIFTY", na=False)]
        fut = fut.sort_values("SEM_EXPIRY_DATE")

        if fut.empty:
            raise ValueError("No NIFTY FUTIDX instruments found")

        nearest = fut.iloc[0]
        return int(nearest["SEM_SMST_SECURITY_ID"])

    print("Fetching/Validating FUT ID from Dhan...")
    dhan = dhanhq(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)
    latest_fut_id = _fetch_nearest_future_id(dhan)

    # Use cached ID only if it matches latest nearest futures ID
    if os.path.exists(FUTURE_ID_FILE):
        try:
            with open(FUTURE_ID_FILE, "r") as f:
                cached_id = int(f.read().strip())

            if cached_id == latest_fut_id:
                print("Loaded FUT ID from file (validated):", cached_id)
                return cached_id

            print(f"Cached FUT ID {cached_id} is stale. Updating to {latest_fut_id}.")
        except Exception:
            print("FUT ID cache invalid/corrupt. Rebuilding cache.")

    os.makedirs("data", exist_ok=True)
    with open(FUTURE_ID_FILE, "w") as f:
        f.write(str(latest_fut_id))

    print("Saved FUT ID:", latest_fut_id)
    return latest_fut_id


# ===============================
# Generate ATM ± Strikes
# ===============================
def generate_strikes(atm, step=50, count=5):
    return [atm + i * step for i in range(-count, count + 1)]


def send_watchdog_telegram(message):
    if not (
            Config.ENABLE_ALERTS
            and Config.TELEGRAM_ENABLED
            and Config.TELEGRAM_BOT_TOKEN
            and Config.TELEGRAM_CHAT_ID
    ):
        return

    try:
        requests.post(
            f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": Config.TELEGRAM_CHAT_ID,
                "text": message,
            },
            timeout=5,
        )
    except Exception as exc:
        print(f"[WATCHDOG] Telegram alert failed: {exc}")


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
    max_option_chain_attempts = 5

    for attempt in range(1, max_option_chain_attempts + 1):
        option_data = option_chain.fetch_option_chain()
        if option_data is not None:
            break

        print(f"Option Chain fetch failed (attempt {attempt}/{max_option_chain_attempts})")
        time.sleep(2)

    strike_list = []
    ce_ids = []
    pe_ids = []

    if option_data is not None:
        atm = option_data["atm"]
        print("ATM Strike:", atm)

        # Generate ATM ± 5 strikes
        strike_list = generate_strikes(atm)
        print("Subscribing Strikes:", strike_list)

        for strike in strike_list:
            data = option_chain.get_security_id_by_strike(strike)
            if data:
                if data["ce"]:
                    ce_ids.append(data["ce"])
                if data["pe"]:
                    pe_ids.append(data["pe"])
            else:
                print("Security ID not found for strike:", strike)
    else:
        print("Starting without option subscriptions; candles/DB should still run.")

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

    watchdog = RuntimeWatchdog(
        heartbeat_file=HEARTBEAT_FILE,
        stale_after_seconds=Config.WATCHDOG_STALE_SECONDS,
        check_interval=Config.WATCHDOG_CHECK_INTERVAL,
        state_file=WATCHDOG_STATE_FILE,
        max_restarts=Config.WATCHDOG_MAX_RESTARTS,
        restart_window_seconds=Config.WATCHDOG_RESTART_WINDOW_SECONDS,
    )

    def restart_process(age, payload):
        can_restart, recent_restarts = watchdog.can_restart_now()
        message = (
            f"[WATCHDOG] Heartbeat stale for {age:.1f}s. "
            f"Recent restarts in window: {recent_restarts}/{Config.WATCHDOG_MAX_RESTARTS}. "
            f"Last payload: {payload}"
        )
        send_watchdog_telegram(message)

        if not can_restart:
            print("\n[WATCHDOG] Restart limit reached. Not restarting automatically.")
            print(f"[WATCHDOG] Last payload: {payload}")
            watchdog.touch({"phase": "watchdog_restart_limit_reached", "payload": payload})
            return

        restart_count = watchdog.record_restart({"age_seconds": age, "payload": payload})
        print(
            f"\n[WATCHDOG] Heartbeat stale for {age:.1f}s. Restarting process."
        )
        print(f"[WATCHDOG] Restart count in active window: {restart_count}/{Config.WATCHDOG_MAX_RESTARTS}")
        print(f"[WATCHDOG] Last payload: {payload}")
        time.sleep(2)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    if Config.ENABLE_WATCHDOG:
        watchdog.touch({"phase": "boot"})
        watchdog.start(restart_process)

    # Step 3 - Start WebSocket
    print("Starting WebSocket...")
    live_feed = LiveFeed(instruments)
    live_feed.connect()

    time.sleep(5)

    # Step 4 - Start Event Engine
    print("Initializing Event Engine...")
    engine = EventEngine(live_feed, watchdog=watchdog if Config.ENABLE_WATCHDOG else None)
    print("Running Event Engine...")
    engine.run()


if __name__ == "__main__":
    main()
