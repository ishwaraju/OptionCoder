"""
Database Configuration
"""

import os
from dotenv import load_dotenv

load_dotenv()


class DBConfig:
    """Database configuration"""

    # ==============================
    # DATABASE SETTINGS (PostgreSQL)
    # ==============================
    DB_ENABLED = os.getenv('DB_ENABLED', 'True').lower() == 'true'
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))
    DB_NAME = os.getenv('DB_NAME', 'optioncoder')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    DB_CONNECT_TIMEOUT = int(os.getenv('DB_CONNECT_TIMEOUT', '5'))

    # sslmode examples: disable / prefer / require
    DB_SSLMODE = os.getenv('DB_SSLMODE', 'prefer')

    # ==============================
    # STATE RECOVERY SETTINGS
    # ==============================
    STATE_RECOVERY_5M_BARS = int(os.getenv('STATE_RECOVERY_5M_BARS', '24'))

    @classmethod
    def get_db_dsn(cls):
        """
        Build PostgreSQL DSN string.
        Example:
        host=localhost port=5432 dbname=optioncoder user=postgres password=... sslmode=prefer
        """
        dsn_parts = [
            f"host={cls.DB_HOST}",
            f"port={cls.DB_PORT}",
            f"dbname={cls.DB_NAME}",
            f"user={cls.DB_USER}",
            f"connect_timeout={cls.DB_CONNECT_TIMEOUT}",
            f"sslmode={cls.DB_SSLMODE}",
        ]

        if cls.DB_PASSWORD:
            dsn_parts.append(f"password={cls.DB_PASSWORD}")

        return " ".join(dsn_parts)

    @classmethod
    def print_db_config(cls):
        """Print database configuration"""
        print("\n========== DATABASE CONFIG ==========")
        print("DB Enabled:", cls.DB_ENABLED)
        print("DB Host:", cls.DB_HOST)
        print("DB Port:", cls.DB_PORT)
        print("DB Name:", cls.DB_NAME)
        print("DB User:", cls.DB_USER)
        print("SSL Mode:", cls.DB_SSLMODE)
        print("=====================================\n")
