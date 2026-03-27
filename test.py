from dhanhq import dhanhq
from config import Config
from datetime import datetime

dhan = dhanhq(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)

today = datetime.today().strftime('%Y-%m-%d')

data = dhan.ledger_report("2025-03-02", "2025-03-25")

print(data)