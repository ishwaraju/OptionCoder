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
        ORDER BY ts DESC
        LIMIT %s;
        """
        rows = self._execute(query, (instrument, limit))
        # Reverse to maintain chronological order (oldest first)
        rows = list(reversed(rows))
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
        SELECT ts, signal, reason, strategy_score, signal_quality, strike
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
                "signal_quality": row[4],
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

    def fetch_intraday_candle_health(self, instrument, session_start, end_time, timeframe="5m"):
        """Summarize intraday candle coverage and largest gap for a session slice."""
        table = "candles_5m" if timeframe == "5m" else "candles_1m"
        expected_step_seconds = 300 if timeframe == "5m" else 60
        query = f"""
        WITH ordered AS (
            SELECT
                ts,
                LAG(ts) OVER (ORDER BY ts) AS prev_ts
            FROM {table}
            WHERE instrument = %s
              AND ts >= %s
              AND ts <= %s
        )
        SELECT
            COUNT(*)::int,
            MAX(EXTRACT(EPOCH FROM (ts - prev_ts)))::int
        FROM ordered;
        """
        rows = self._execute(query, (instrument, session_start, end_time))
        count = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        max_gap_seconds = int(rows[0][1]) if rows and rows[0][1] is not None else 0
        elapsed_seconds = max(0.0, (end_time - session_start).total_seconds())
        expected_count = int(elapsed_seconds // expected_step_seconds) + 1
        coverage_pct = round((count / expected_count) * 100, 2) if expected_count > 0 else 0.0
        return {
            "count": count,
            "expected_count": expected_count,
            "coverage_pct": coverage_pct,
            "max_gap_seconds": max_gap_seconds,
        }

    def fetch_intraday_oi_health(self, instrument, session_start, end_time):
        """Summarize intraday OI snapshot coverage, distinct minutes, and largest gap."""
        query = """
        WITH ordered AS (
            SELECT
                ts,
                LAG(ts) OVER (ORDER BY ts) AS prev_ts
            FROM oi_snapshots_1m
            WHERE instrument = %s
              AND ts >= %s
              AND ts <= %s
        ),
        minute_counts AS (
            SELECT
                COUNT(*)::int AS row_count,
                COUNT(DISTINCT date_trunc('minute', ts))::int AS distinct_minutes,
                MAX(EXTRACT(EPOCH FROM (ts - prev_ts)))::int AS max_gap_seconds,
                COUNT(*) FILTER (WHERE data_quality <> 'GOOD')::int AS non_good_rows
            FROM oi_snapshots_1m
            LEFT JOIN ordered USING (ts)
            WHERE instrument = %s
              AND ts >= %s
              AND ts <= %s
        )
        SELECT row_count, distinct_minutes, max_gap_seconds, non_good_rows
        FROM minute_counts;
        """
        rows = self._execute(query, (instrument, session_start, end_time, instrument, session_start, end_time))
        row_count = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        distinct_minutes = int(rows[0][1]) if rows and rows[0][1] is not None else 0
        max_gap_seconds = int(rows[0][2]) if rows and rows[0][2] is not None else 0
        non_good_rows = int(rows[0][3]) if rows and rows[0][3] is not None else 0
        elapsed_seconds = max(0.0, (end_time - session_start).total_seconds())
        expected_minutes = int(elapsed_seconds // 60) + 1
        coverage_pct = round((distinct_minutes / expected_minutes) * 100, 2) if expected_minutes > 0 else 0.0
        return {
            "row_count": row_count,
            "distinct_minutes": distinct_minutes,
            "expected_minutes": expected_minutes,
            "coverage_pct": coverage_pct,
            "max_gap_seconds": max_gap_seconds,
            "non_good_rows": non_good_rows,
        }

    def fetch_latest_option_band_snapshot(self, instrument, before_ts=None):
        """Fetch the latest full option-band snapshot for an instrument."""
        if before_ts is None:
            query = """
            SELECT
                ts, atm_strike, strike, distance_from_atm, option_type, security_id,
                oi, volume, ltp, iv,
                top_bid_price, top_bid_quantity, top_ask_price, top_ask_quantity,
                spread, average_price, previous_oi, previous_volume,
                delta, theta, gamma, vega
            FROM option_band_snapshots_1m
            WHERE instrument = %s
              AND ts = (
                  SELECT MAX(ts)
                  FROM option_band_snapshots_1m
                  WHERE instrument = %s
              )
            ORDER BY strike ASC, option_type ASC;
            """
            rows = self._execute(query, (instrument, instrument))
        else:
            query = """
            SELECT
                ts, atm_strike, strike, distance_from_atm, option_type, security_id,
                oi, volume, ltp, iv,
                top_bid_price, top_bid_quantity, top_ask_price, top_ask_quantity,
                spread, average_price, previous_oi, previous_volume,
                delta, theta, gamma, vega
            FROM option_band_snapshots_1m
            WHERE instrument = %s
              AND ts = (
                  SELECT MAX(ts)
                  FROM option_band_snapshots_1m
                  WHERE instrument = %s
                    AND ts <= %s
              )
            ORDER BY strike ASC, option_type ASC;
            """
            rows = self._execute(query, (instrument, instrument, before_ts))

        if not rows:
            return []

        snapshots = []
        for row in rows:
            snapshots.append(
                {
                    "ts": row[0],
                    "atm_strike": int(row[1]) if row[1] is not None else None,
                    "strike": int(row[2]) if row[2] is not None else None,
                    "distance_from_atm": int(row[3]) if row[3] is not None else None,
                    "option_type": row[4],
                    "security_id": row[5],
                    "oi": int(row[6]) if row[6] is not None else 0,
                    "volume": int(row[7]) if row[7] is not None else 0,
                    "ltp": float(row[8]) if row[8] is not None else 0.0,
                    "iv": float(row[9]) if row[9] is not None else 0.0,
                    "top_bid_price": float(row[10]) if row[10] is not None else None,
                    "top_bid_quantity": int(row[11]) if row[11] is not None else None,
                    "top_ask_price": float(row[12]) if row[12] is not None else None,
                    "top_ask_quantity": int(row[13]) if row[13] is not None else None,
                    "spread": float(row[14]) if row[14] is not None else None,
                    "average_price": float(row[15]) if row[15] is not None else None,
                    "previous_oi": int(row[16]) if row[16] is not None else None,
                    "previous_volume": int(row[17]) if row[17] is not None else None,
                    "delta": float(row[18]) if row[18] is not None else None,
                    "theta": float(row[19]) if row[19] is not None else None,
                    "gamma": float(row[20]) if row[20] is not None else None,
                    "vega": float(row[21]) if row[21] is not None else None,
                }
            )

        return snapshots

    def fetch_option_contract_snapshot(self, instrument, strike, option_type, before_ts=None):
        """Fetch the latest snapshot row for one option contract."""
        if strike is None or option_type not in {"CE", "PE"}:
            return None

        if before_ts is None:
            query = """
            SELECT
                ts, atm_strike, strike, distance_from_atm, option_type, security_id,
                oi, volume, ltp, iv,
                top_bid_price, top_bid_quantity, top_ask_price, top_ask_quantity,
                spread, average_price, previous_oi, previous_volume,
                delta, theta, gamma, vega
            FROM option_band_snapshots_1m
            WHERE instrument = %s
              AND strike = %s
              AND option_type = %s
            ORDER BY ts DESC
            LIMIT 1;
            """
            rows = self._execute(query, (instrument, strike, option_type))
        else:
            query = """
            SELECT
                ts, atm_strike, strike, distance_from_atm, option_type, security_id,
                oi, volume, ltp, iv,
                top_bid_price, top_bid_quantity, top_ask_price, top_ask_quantity,
                spread, average_price, previous_oi, previous_volume,
                delta, theta, gamma, vega
            FROM option_band_snapshots_1m
            WHERE instrument = %s
              AND strike = %s
              AND option_type = %s
              AND ts <= %s
            ORDER BY ts DESC
            LIMIT 1;
            """
            rows = self._execute(query, (instrument, strike, option_type, before_ts))

        if not rows:
            return None

        row = rows[0]
        return {
            "ts": row[0],
            "atm_strike": int(row[1]) if row[1] is not None else None,
            "strike": int(row[2]) if row[2] is not None else None,
            "distance_from_atm": int(row[3]) if row[3] is not None else None,
            "option_type": row[4],
            "security_id": row[5],
            "oi": int(row[6]) if row[6] is not None else 0,
            "volume": int(row[7]) if row[7] is not None else 0,
            "ltp": float(row[8]) if row[8] is not None else None,
            "iv": float(row[9]) if row[9] is not None else None,
            "top_bid_price": float(row[10]) if row[10] is not None else None,
            "top_bid_quantity": int(row[11]) if row[11] is not None else 0,
            "top_ask_price": float(row[12]) if row[12] is not None else None,
            "top_ask_quantity": int(row[13]) if row[13] is not None else 0,
            "spread": float(row[14]) if row[14] is not None else None,
            "average_price": float(row[15]) if row[15] is not None else None,
            "previous_oi": int(row[16]) if row[16] is not None else 0,
            "previous_volume": int(row[17]) if row[17] is not None else 0,
            "delta": float(row[18]) if row[18] is not None else None,
            "theta": float(row[19]) if row[19] is not None else None,
            "gamma": float(row[20]) if row[20] is not None else None,
            "vega": float(row[21]) if row[21] is not None else None,
        }

    def fetch_recent_atm_iv_series(self, instrument, option_type, before_ts=None, limit=20):
        """Fetch recent ATM IV observations for one option side."""
        if option_type not in {"CE", "PE"}:
            return []

        if before_ts is None:
            query = """
            SELECT ts, iv
            FROM option_band_snapshots_1m
            WHERE instrument = %s
              AND option_type = %s
              AND distance_from_atm = 0
              AND iv IS NOT NULL
              AND iv > 0
            ORDER BY ts DESC
            LIMIT %s;
            """
            rows = self._execute(query, (instrument, option_type, limit))
        else:
            query = """
            SELECT ts, iv
            FROM option_band_snapshots_1m
            WHERE instrument = %s
              AND option_type = %s
              AND distance_from_atm = 0
              AND ts <= %s
              AND iv IS NOT NULL
              AND iv > 0
            ORDER BY ts DESC
            LIMIT %s;
            """
            rows = self._execute(query, (instrument, option_type, before_ts, limit))

        return [
            {
                "ts": row[0],
                "iv": float(row[1]) if row[1] is not None else None,
            }
            for row in rows
            if row[1] is not None
        ]

    def fetch_option_outcome_summary(self, signal_ts, instrument, signal, strike):
        """Fetch aggregated premium outcome summary for one fired signal."""
        query = """
        SELECT
            COUNT(*)::int,
            MAX(minutes_since_signal)::int,
            MAX(max_favorable_ltp),
            MIN(max_adverse_ltp),
            MAX(pnl_points),
            MIN(pnl_points),
            (
                SELECT pnl_points
                FROM option_signal_outcomes_1m o2
                WHERE o2.signal_ts = %s
                  AND o2.instrument = %s
                  AND o2.signal = %s
                  AND o2.strike = %s
                ORDER BY observed_ts DESC
                LIMIT 1
            ) AS final_pnl
        FROM option_signal_outcomes_1m o
        WHERE o.signal_ts = %s
          AND o.instrument = %s
          AND o.signal = %s
          AND o.strike = %s;
        """
        rows = self._execute(
            query,
            (
                signal_ts, instrument, signal, strike,
                signal_ts, instrument, signal, strike,
            ),
        )
        if not rows:
            return None
        row = rows[0]
        if row[0] in (None, 0):
            return None
        return {
            "samples": int(row[0]),
            "minutes_tracked": int(row[1]) if row[1] is not None else 0,
            "max_favorable_ltp": float(row[2]) if row[2] is not None else None,
            "max_adverse_ltp": float(row[3]) if row[3] is not None else None,
            "max_pnl_points": float(row[4]) if row[4] is not None else None,
            "min_pnl_points": float(row[5]) if row[5] is not None else None,
            "final_pnl_points": float(row[6]) if row[6] is not None else None,
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
            signal_quality, setup_type, tradability, time_regime, oi_mode, reason,
            confidence_summary, entry_above, entry_below, invalidate_price, first_target_price
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
            "confidence_summary": row[11],
            "entry_above": float(row[12]) if row[12] is not None else None,
            "entry_below": float(row[13]) if row[13] is not None else None,
            "invalidate_price": float(row[14]) if row[14] is not None else None,
            "first_target_price": float(row[15]) if row[15] is not None else None,
        }

    def fetch_latest_scalp_signal(self, instrument):
        """Fetch latest scalp signal for an instrument."""
        query = """
        SELECT
            ts, signal, entry_price, target_price, stop_loss,
            score, reason, status, exit_ts, exit_price, pnl
        FROM scalp_signals_1m
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
            "target_price": float(row[3]) if row[3] is not None else None,
            "stop_loss": float(row[4]) if row[4] is not None else None,
            "score": float(row[5]) if row[5] is not None else None,
            "reason": row[6],
            "status": row[7],
            "exit_ts": row[8],
            "exit_price": float(row[9]) if row[9] is not None else None,
            "pnl": float(row[10]) if row[10] is not None else None,
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
