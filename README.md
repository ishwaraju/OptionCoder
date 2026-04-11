# Dhan Options Trading Application

A comprehensive Python trading application for options buying using the Dhan API. This application is specifically designed for option buyers with features for market analysis, position management, and risk management.

## Features

- **Option Chain Analysis**: View and analyze option chains with OI, volume, and Greeks data
- **Market Data Analysis**: Get insights on market sentiment with OI analysis
- **Option Trading**: Buy and sell options with risk management
- **Position Tracking**: Monitor current positions with real-time P&L
- **Order Management**: View order book and manage orders
- **Risk Management**: Built-in risk controls and stop-loss functionality
- **CLI Interface**: User-friendly command-line interface with colored output

## Requirements

- Python 3.7 or higher
- Dhan API credentials (Client ID and Access Token)
- Active Dhan trading account

## Installation

1. Clone or download the project
2. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Copy the example environment file:
```bash
cp .env .env
```

2. Edit the `.env` file with your Dhan API credentials:
```
DHAN_CLIENT_ID=your_client_id_here
DHAN_ACCESS_TOKEN=your_access_token_here
DEFAULT_QUANTITY=25
MAX_RISK_PERCENT=2.0
STOP_LOSS_PERCENT=5.0
DEBUG=False
LOG_LEVEL=INFO
```

## Usage

Run the application:
```bash
python3 main.py
```

Replay the latest decision from the audit log:
```bash
python3 /Users/ishwar/Documents/OptionCoder/tools/replay_decision.py
```

Replay a specific decision by time match:
```bash
python3 /Users/ishwar/Documents/OptionCoder/tools/replay_decision.py "2026-04-06 12:25"
```

Reload one trading day of intraday candles from Dhan historical API into `candles_1m` and `candles_5m`:
```bash
.venv/bin/python /Users/ishwar/Documents/OptionCoder/tools/reload_intraday_day.py --date YYYY-MM-DD --replace-day
```

Example:
```bash
.venv/bin/python /Users/ishwar/Documents/OptionCoder/tools/reload_intraday_day.py --date 2026-04-08 --replace-day
```

Check service heartbeat/watchdog status:
```bash
python3 /Users/ishwar/Documents/OptionCoder/tools/runtime_status.py
```

Compact one-line status:
```bash
python3 /Users/ishwar/Documents/OptionCoder/tools/runtime_status.py --one-line
```

Start `data_collector` and `oi_collector` together for all supported instruments:
```bash
python3 /Users/ishwar/Documents/OptionCoder/tools/run_collectors.py
```

This launcher starts collectors for:
- `NIFTY`
- `BANKNIFTY`
- `SENSEX`

`signal_service` is intentionally separate so you can start or stop it independently.

Telegram read-only command listener:
```bash
python3 /Users/ishwar/Documents/OptionCoder/services/telegram_bot_service.py
```

Shortcut launcher:
```bash
python3 /Users/ishwar/Documents/OptionCoder/tools/run_telegram.py
```

Supported Telegram commands:
- `/status`
- `/health`
- `/signals`
- `/stop`
- `/shutdown`

## Decision Operations

Use compact console output for live trading:
```bash
CONSOLE_MODE=COMPACT python3 main.py
```

Use detailed console output for debugging:
```bash
CONSOLE_MODE=DETAILED python3 main.py
```

Decision audit trail is written to:
```text
data/decision_audit.csv
```

Database tables:
```text
strategy_decisions_5m  -> full 5-minute decision audit (signals + no-trade decisions)
signals_issued         -> only actual fired actionable signals
trade_monitor_events_1m -> per-minute post-signal hold/exit guidance
```

End-of-day session summary is written to files like:
```text
data/session_summary_YYYYMMDD.txt
```

Telegram alert setup:
```text
Set TELEGRAM_ENABLED=true
Set TELEGRAM_BOT_TOKEN in .env
Set TELEGRAM_CHAT_ID in .env after messaging your bot once
```

### Main Menu Options

1. **View Option Chain**: Display current option chain data with strike prices, premiums, volume, and open interest
2. **Analyze Market Data**: Get market insights including OI analysis and top options
3. **Buy Option**: Place option orders with symbol, strike, and type selection
4. **View Positions**: Monitor current positions with real-time P&L
5. **View Order Book**: Check order status and history
6. **Account Balance**: View account balance and margin details
7. **Risk Settings**: Display current risk management parameters
8. **Exit**: Close the application

## Project Structure

```
.
├── main.py              # Main application entry point
├── config.py            # Configuration and settings
├── dhan_client.py       # Dhan API client wrapper
├── option_trader.py     # Option analysis and trading logic
├── trading_engine.py    # Trading engine with order management
├── requirements.txt     # Python dependencies
├── .env.example        # Environment variables template
└── README.md           # Project documentation
```

## Key Components

### DhanClient
Handles authentication and API communication with Dhan HQ.

### OptionTrader
Provides option chain analysis, market data processing, and option selection strategies.

### TradingEngine
Manages order placement, position tracking, and risk management.

## Risk Management

The application includes built-in risk management features:

- **Position Sizing**: Limits position size based on account balance
- **Stop Loss**: Automatic stop-loss placement to limit downside
- **Risk Percentage**: Maximum risk per trade (configurable)
- **Order Validation**: Pre-trade risk checks

## Supported Symbols

- NIFTY (Nifty 50 Index Options)
- BANKNIFTY (Nifty Bank Index Options)
- Other index options supported by Dhan

## API Endpoints Used

- Market Data & Quotes
- Option Chain
- Order Placement & Management
- Position & Portfolio
- Account & Margin Details

## Error Handling

The application includes comprehensive error handling for:
- API connection issues
- Invalid order parameters
- Network timeouts
- Data parsing errors

## Logging

Configurable logging levels (DEBUG, INFO, WARNING, ERROR) for monitoring and debugging.

## Disclaimer

This application is for educational and demonstration purposes. Trading options involves significant risk and may not be suitable for all investors. Always do your own research and consider consulting with a financial advisor before making investment decisions.

## License

This project is open source and available under the MIT License.
