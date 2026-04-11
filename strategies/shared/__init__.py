"""
Strategy modules for OptionCoder
"""

from .breakout_strategy import BreakoutStrategy
from .strike_selector import StrikeSelector
from .option_trader import OptionTrader

__all__ = [
    'BreakoutStrategy',
    'StrikeSelector',
    'OptionTrader'
]
