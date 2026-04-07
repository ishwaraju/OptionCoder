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

    def _execute_many(self, query, rows):
        if not self.enabled or not self.conn or not rows:
            return

        try:
            with self.conn.cursor() as cur:
                cur.executemany(query, rows)
        except Exception as e:
            print("DB bulk execute error:", e)

    def _fetch_all(self, query, params=None):
        if not self.enabled or not self.conn:
            return []

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params or ())
                return cur.fetchall()
        except Exception as e:
            print("DB fetch error:", e)
            return []

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

    def insert_option_band_snapshots_1m(self, rows):
        """
        rows = [
            (
                ts, instrument, atm_strike, strike, distance_from_atm,
                option_type, security_id, oi, volume, ltp, iv
            ),
            ...
        ]
        """
        query = """
        INSERT INTO option_band_snapshots_1m
        (
            ts, instrument, atm_strike, strike, distance_from_atm,
            option_type, security_id, oi, volume, ltp, iv
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument, strike, option_type) DO UPDATE
        SET atm_strike = EXCLUDED.atm_strike,
            distance_from_atm = EXCLUDED.distance_from_atm,
            security_id = EXCLUDED.security_id,
            oi = EXCLUDED.oi,
            volume = EXCLUDED.volume,
            ltp = EXCLUDED.ltp,
            iv = EXCLUDED.iv;
        """
        self._execute_many(query, rows)

    def insert_strategy_decision_5m(self, row):
        """
        row = (
            ts, instrument, price, signal, reason, strategy_score, score_factors,
            volume_signal, oi_bias, oi_trend, build_up, pressure_bias,
            ce_delta_total, pe_delta_total, pcr,
            orb_high, orb_low, vwap, atr, strike
        )
        """
        query = """
        INSERT INTO strategy_decisions_5m
        (
            ts, instrument, price, signal, reason, strategy_score, score_factors,
            volume_signal, oi_bias, oi_trend, build_up, pressure_bias,
            ce_delta_total, pe_delta_total, pcr,
            orb_high, orb_low, vwap, atr, strike
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument) DO UPDATE
        SET price = EXCLUDED.price,
            signal = EXCLUDED.signal,
            reason = EXCLUDED.reason,
            strategy_score = EXCLUDED.strategy_score,
            score_factors = EXCLUDED.score_factors,
            volume_signal = EXCLUDED.volume_signal,
            oi_bias = EXCLUDED.oi_bias,
            oi_trend = EXCLUDED.oi_trend,
            build_up = EXCLUDED.build_up,
            pressure_bias = EXCLUDED.pressure_bias,
            ce_delta_total = EXCLUDED.ce_delta_total,
            pe_delta_total = EXCLUDED.pe_delta_total,
            pcr = EXCLUDED.pcr,
            orb_high = EXCLUDED.orb_high,
            orb_low = EXCLUDED.orb_low,
            vwap = EXCLUDED.vwap,
            atr = EXCLUDED.atr,
            strike = EXCLUDED.strike;
        """
        self._execute(query, row)

    def fetch_recent_candles_5m(self, instrument, limit=24):
        query = """
        SELECT ts, open, high, low, close, volume
        FROM candles_5m
        WHERE instrument = %s
          AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = CURRENT_DATE
        ORDER BY ts ASC
        LIMIT %s;
        """
        rows = self._fetch_all(query, (instrument, limit))
        return [
            {
                "time": row[0],
                "close_time": None,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": int(row[5]),
            }
            for row in rows
        ]
