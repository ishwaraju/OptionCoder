"""Database Reader - Common database access for reading data"""

from config import Config
from shared.db.pool import DBPool


class DBReader:
    def __init__(self):
        self.enabled = DBPool.initialize()
        self.conn = None
        if not self.enabled:
            print("DBReader disabled (DB_ENABLED=False)")
            return
        print("DBReader using pooled connections")

    def close(self):
        self.conn = None

    def _execute(self, query, params=None):
        if not self.enabled:
            return []

        try:
            with DBPool.connection() as conn:
                if conn is None:
                    return []
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    return cur.fetchall()
        except Exception as e:
            print("DBReader execute error:", e)
            return []

    def fetch_recent_candles_5m(self, instrument, limit=24):
        """Fetch recent 5-minute candles for indicator warmup"""
        query = """
        SELECT ts, open, high, low, close, volume
        FROM candles_5m
        WHERE instrument = %s
          AND DATE(ts AT TIME ZONE 'Asia/Kolkata') = CURRENT_DATE
        ORDER BY ts ASC
        LIMIT %s;
        """
        rows = self._execute(query, (instrument, limit))
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

    def fetch_recent_candles_1m(self, instrument, limit=15):
        """Fetch recent 1-minute candles."""
        query = """
        SELECT ts, open, high, low, close, volume
        FROM candles_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT %s;
        """
        rows = self._execute(query, (instrument, limit))
        rows = list(reversed(rows))
        return [
            {
                "time": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": int(row[5]),
            }
            for row in rows
        ]

    def fetch_candles_in_range(self, instrument, start_time, end_time, timeframe="5m"):
        """Fetch candles in a specific time range"""
        table = "candles_5m" if timeframe == "5m" else "candles_1m"
        
        query = f"""
        SELECT ts, open, high, low, close, volume
        FROM {table}
        WHERE instrument = %s
          AND ts >= %s
          AND ts <= %s
        ORDER BY ts ASC;
        """
        
        rows = self._execute(query, (instrument, start_time, end_time))
        return [
            {
                "time": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": int(row[5]),
            }
            for row in rows
        ]

    def fetch_latest_candle(self, instrument, timeframe="5m"):
        """Fetch the latest candle"""
        table = "candles_5m" if timeframe == "5m" else "candles_1m"
        
        query = f"""
        SELECT ts, open, high, low, close, volume
        FROM {table}
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 1;
        """
        
        rows = self._execute(query, (instrument,))
        if not rows:
            return None
            
        row = rows[0]
        return {
            "time": row[0],
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": int(row[5]),
        }

    def fetch_strategy_decisions(self, instrument, limit=10):
        """Fetch recent strategy decisions"""
        query = """
        SELECT ts, signal, reason, strategy_score, confidence, strike
        FROM strategy_decisions_5m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT %s;
        """
        
        rows = self._execute(query, (instrument, limit))
        return [
            {
                "time": row[0],
                "signal": row[1],
                "reason": row[2],
                "score": row[3],
                "confidence": row[4],
                "strike": row[5],
            }
            for row in rows
        ]

    def fetch_latest_oi_snapshot(self, instrument, before_ts=None):
        """Fetch latest OI snapshot for an instrument, optionally at or before a timestamp."""
        if before_ts is None:
            query = """
            SELECT
                ts, underlying_price, ce_oi, pe_oi, pcr,
                ce_oi_change, pe_oi_change, oi_sentiment, oi_trend,
                support_level, resistance_level, volume_pcr, data_quality
            FROM oi_snapshots_1m
            WHERE instrument = %s
            ORDER BY ts DESC
            LIMIT 1;
            """
            rows = self._execute(query, (instrument,))
        else:
            query = """
            SELECT
                ts, underlying_price, ce_oi, pe_oi, pcr,
                ce_oi_change, pe_oi_change, oi_sentiment, oi_trend,
                support_level, resistance_level, volume_pcr, data_quality
            FROM oi_snapshots_1m
            WHERE instrument = %s
              AND ts <= %s
            ORDER BY ts DESC
            LIMIT 1;
            """
            rows = self._execute(query, (instrument, before_ts))

        if not rows:
            return None

        row = rows[0]
        return {
            "ts": row[0],
            "underlying_price": float(row[1]) if row[1] is not None else None,
            "ce_oi": int(row[2]) if row[2] is not None else 0,
            "pe_oi": int(row[3]) if row[3] is not None else 0,
            "pcr": float(row[4]) if row[4] is not None else 0.0,
            "ce_oi_change": int(row[5]) if row[5] is not None else 0,
            "pe_oi_change": int(row[6]) if row[6] is not None else 0,
            "oi_sentiment": row[7],
            "oi_trend": row[8],
            "support_level": float(row[9]) if row[9] is not None else None,
            "resistance_level": float(row[10]) if row[10] is not None else None,
            "volume_pcr": float(row[11]) if row[11] is not None else 0.0,
            "data_quality": row[12],
        }

    def get_candle_count(self, instrument, start_time, end_time, timeframe="5m"):
        """Get count of candles in a range"""
        table = "candles_5m" if timeframe == "5m" else "candles_1m"
        
        query = f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE instrument = %s
          AND ts >= %s
          AND ts <= %s;
        """
        
        rows = self._execute(query, (instrument, start_time, end_time))
        return rows[0][0] if rows else 0

    def check_data_gaps(self, instrument, start_time, end_time, timeframe="5m"):
        """Check for data gaps in a time range"""
        table = "candles_5m" if timeframe == "5m" else "candles_1m"
        
        query = f"""
        SELECT ts
        FROM {table}
        WHERE instrument = %s
          AND ts >= %s
          AND ts <= %s
        ORDER BY ts ASC;
        """
        
        rows = self._execute(query, (instrument, start_time, end_time))
        return [row[0] for row in rows]

    def fetch_latest_signal_issued(self, instrument):
        """Fetch latest fired signal for an instrument."""
        query = """
        SELECT
            ts, signal, price, strike, strategy_score,
            signal_quality, setup_type, tradability, time_regime, oi_mode, reason
        FROM signals_issued
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 1;
        """
        rows = self._execute(query, (instrument,))
        if not rows:
            return None

        row = rows[0]
        return {
            "time": row[0],
            "signal": row[1],
            "price": float(row[2]) if row[2] is not None else None,
            "strike": row[3],
            "score": float(row[4]) if row[4] is not None else None,
            "quality": row[5],
            "setup_type": row[6],
            "tradability": row[7],
            "time_regime": row[8],
            "oi_mode": row[9],
            "reason": row[10],
        }

    def fetch_latest_trade_monitor_event(self, instrument):
        """Fetch latest trade monitor event for an instrument."""
        query = """
        SELECT
            ts, signal, entry_ts, entry_price, current_price,
            pnl_points, guidance, reason, structure_state, quality, time_regime
        FROM trade_monitor_events_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 1;
        """
        rows = self._execute(query, (instrument,))
        if not rows:
            return None

        row = rows[0]
        return {
            "time": row[0],
            "signal": row[1],
            "entry_ts": row[2],
            "entry_price": float(row[3]) if row[3] is not None else None,
            "current_price": float(row[4]) if row[4] is not None else None,
            "pnl_points": float(row[5]) if row[5] is not None else None,
            "guidance": row[6],
            "reason": row[7],
            "structure_state": row[8],
            "quality": row[9],
            "time_regime": row[10],
        }
