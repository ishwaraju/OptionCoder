import psycopg2
from config import Config


class DBWriter:
    def __init__(self):
        self.enabled = Config.DB_ENABLED
        self.conn = None

        if not self.enabled:
            print("DBWriter disabled (DB_ENABLED=False)")
            return

        try:
            print("Connecting to DB...")
            self.conn = psycopg2.connect(Config.get_db_dsn())
            self.conn.autocommit = True
            with self.conn.cursor() as cur:
                # Keep DB session rendering and DB-side timestamp functions in IST.
                cur.execute("SET TIME ZONE 'Asia/Kolkata'")
            print("DB connected successfully")
        except Exception as e:
            self.enabled = False
            print("DB connection failed:", e)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _execute(self, query, params):
        if not self.enabled or not self.conn:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
        except Exception as e:
            print("DB execute error:", e)

    def insert_candle_1m(self, row):
        """
        row = (ts, instrument, open, high, low, close, volume)
        """
        query = """
        INSERT INTO candles_1m (ts, instrument, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """
        self._execute(query, row)

    def insert_candle_5m(self, row):
        """
        row = (ts, instrument, open, high, low, close, volume)
        """
        query = """
        INSERT INTO candles_5m (ts, instrument, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument) DO UPDATE
        SET open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """
        self._execute(query, row)

    def insert_oi_1m(self, row):
        """
        row = (
            ts, instrument, underlying_price, ce_oi, pe_oi,
            ce_volume, pe_volume, ce_volume_band, pe_volume_band, pcr
        )
        """
        query = """
        INSERT INTO oi_snapshots_1m
        (
            ts, instrument, underlying_price, ce_oi, pe_oi,
            ce_volume, pe_volume, ce_volume_band, pe_volume_band, pcr
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument) DO UPDATE
        SET underlying_price = EXCLUDED.underlying_price,
            ce_oi = EXCLUDED.ce_oi,
            pe_oi = EXCLUDED.pe_oi,
            ce_volume = EXCLUDED.ce_volume,
            pe_volume = EXCLUDED.pe_volume,
            ce_volume_band = EXCLUDED.ce_volume_band,
            pe_volume_band = EXCLUDED.pe_volume_band,
            pcr = EXCLUDED.pcr;
        """
        self._execute(query, row)
