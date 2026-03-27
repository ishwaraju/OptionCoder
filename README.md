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
python main.py
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
