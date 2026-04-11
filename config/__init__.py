"""
Configuration Package
"""

from .main_config import MainConfig
from .nifty_config import NiftyConfig
from .db_config import DBConfig

# For backward compatibility
Config = MainConfig
