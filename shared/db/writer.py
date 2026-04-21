import json

from config import Config
from shared.db.pool import DBPool


class DBWriter:
    def __init__(self):
        self.enabled = DBPool.initialize()
        self.conn = None
        if not self.enabled:
            print("DBWriter disabled (DB_ENABLED=False)")
            return
        print("DBWriter using pooled connections")
        self._strategy_decision_has_structured_columns = None

    def close(self):
        self.conn = None

    def _execute(self, query, params):
        if not self.enabled:
            return

        try:
            with DBPool.connection() as conn:
                if conn is None:
                    return
                with conn.cursor() as cur:
                    cur.execute(query, params)
        except Exception as e:
            print("DB execute error:", e)

    def _execute_many(self, query, rows):
        if not self.enabled or not rows:
            return

        try:
            with DBPool.connection() as conn:
                if conn is None:
                    return
                with conn.cursor() as cur:
                    cur.executemany(query, rows)
        except Exception as e:
            print("DB bulk execute error:", e)

    def _fetch_all(self, query, params=None):
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
            print("DB fetch error:", e)
            return []

    def _table_has_columns(self, table_name, column_names):
        if not self.enabled:
            return False

        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s;
        """
        rows = self._fetch_all(query, (table_name,))
        existing = {row[0] for row in rows}
        return all(column in existing for column in column_names)

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
            ce_volume, pe_volume, ce_volume_band, pe_volume_band, pcr,
            ce_oi_change, pe_oi_change, total_oi_change,
            oi_sentiment, oi_bias_strength,
            total_volume, volume_change, volume_pcr,
            max_ce_oi_strike, max_pe_oi_strike, oi_concentration,
            oi_trend, trend_strength,
            support_level, resistance_level, oi_range_width,
            previous_ts, data_age_seconds, data_quality,
            max_ce_oi_amount, max_pe_oi_amount, oi_spread, liquidity_score
        )
        """
        query = """
        INSERT INTO oi_snapshots_1m
        (
            ts, instrument, underlying_price, ce_oi, pe_oi,
            ce_volume, pe_volume, ce_volume_band, pe_volume_band, pcr,
            ce_oi_change, pe_oi_change, total_oi_change,
            oi_sentiment, oi_bias_strength,
            total_volume, volume_change, volume_pcr,
            max_ce_oi_strike, max_pe_oi_strike, oi_concentration,
            oi_trend, trend_strength,
            support_level, resistance_level, oi_range_width,
            previous_ts, data_age_seconds, data_quality,
            max_ce_oi_amount, max_pe_oi_amount, oi_spread, liquidity_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument) DO UPDATE
        SET underlying_price = EXCLUDED.underlying_price,
            ce_oi = EXCLUDED.ce_oi,
            pe_oi = EXCLUDED.pe_oi,
            ce_volume = EXCLUDED.ce_volume,
            pe_volume = EXCLUDED.pe_volume,
            ce_volume_band = EXCLUDED.ce_volume_band,
            pe_volume_band = EXCLUDED.pe_volume_band,
            pcr = EXCLUDED.pcr,
            ce_oi_change = EXCLUDED.ce_oi_change,
            pe_oi_change = EXCLUDED.pe_oi_change,
            total_oi_change = EXCLUDED.total_oi_change,
            oi_sentiment = EXCLUDED.oi_sentiment,
            oi_bias_strength = EXCLUDED.oi_bias_strength,
            total_volume = EXCLUDED.total_volume,
            volume_change = EXCLUDED.volume_change,
            volume_pcr = EXCLUDED.volume_pcr,
            max_ce_oi_strike = EXCLUDED.max_ce_oi_strike,
            max_pe_oi_strike = EXCLUDED.max_pe_oi_strike,
            oi_concentration = EXCLUDED.oi_concentration,
            oi_trend = EXCLUDED.oi_trend,
            trend_strength = EXCLUDED.trend_strength,
            support_level = EXCLUDED.support_level,
            resistance_level = EXCLUDED.resistance_level,
            oi_range_width = EXCLUDED.oi_range_width,
            previous_ts = EXCLUDED.previous_ts,
            data_age_seconds = EXCLUDED.data_age_seconds,
            data_quality = EXCLUDED.data_quality,
            max_ce_oi_amount = EXCLUDED.max_ce_oi_amount,
            max_pe_oi_amount = EXCLUDED.max_pe_oi_amount,
            oi_spread = EXCLUDED.oi_spread,
            liquidity_score = EXCLUDED.liquidity_score;
        """
        self._execute(query, row)

    def insert_option_band_snapshots_1m(self, rows):
        """
        rows = [
            (
                ts, instrument, atm_strike, strike, distance_from_atm,
                option_type, security_id, oi, volume, ltp, iv,
                top_bid_price, top_bid_quantity, top_ask_price, top_ask_quantity,
                spread, average_price, previous_oi, previous_volume,
                delta, theta, gamma, vega
            ),
            ...
        ]
        """
        normalized_rows = []
        for row in rows:
            base = list(row)
            if len(base) < 11:
                continue
            while len(base) < 23:
                base.append(None)
            normalized_rows.append(tuple(base[:23]))

        query = """
        INSERT INTO option_band_snapshots_1m
        (
            ts, instrument, atm_strike, strike, distance_from_atm,
            option_type, security_id, oi, volume, ltp, iv,
            top_bid_price, top_bid_quantity, top_ask_price, top_ask_quantity,
            spread, average_price, previous_oi, previous_volume,
            delta, theta, gamma, vega
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument, strike, option_type) DO UPDATE
        SET atm_strike = EXCLUDED.atm_strike,
            distance_from_atm = EXCLUDED.distance_from_atm,
            security_id = EXCLUDED.security_id,
            oi = EXCLUDED.oi,
            volume = EXCLUDED.volume,
            ltp = EXCLUDED.ltp,
            iv = EXCLUDED.iv,
            top_bid_price = EXCLUDED.top_bid_price,
            top_bid_quantity = EXCLUDED.top_bid_quantity,
            top_ask_price = EXCLUDED.top_ask_price,
            top_ask_quantity = EXCLUDED.top_ask_quantity,
            spread = EXCLUDED.spread,
            average_price = EXCLUDED.average_price,
            previous_oi = EXCLUDED.previous_oi,
            previous_volume = EXCLUDED.previous_volume,
            delta = EXCLUDED.delta,
            theta = EXCLUDED.theta,
            gamma = EXCLUDED.gamma,
            vega = EXCLUDED.vega;
        """
        self._execute_many(query, normalized_rows)

    def insert_strategy_decision_5m(self, row):
        """
        row = (
            ts, instrument, price, signal, reason, strategy_score, score_factors,
            volume_signal, oi_bias, oi_trend, build_up, pressure_bias,
            ce_delta_total, pe_delta_total, pcr,
            orb_high, orb_low, vwap, atr, strike,
            base_bias, setup_type, signal_quality, tradability, time_regime, oi_mode,
            blockers_json, cautions_json, candidate_signal_type, candidate_signal_grade,
            candidate_confidence, actionable_block_reason
        )
        """
        structured_columns = (
            "blockers_json",
            "cautions_json",
            "candidate_signal_type",
            "candidate_signal_grade",
            "candidate_confidence",
            "actionable_block_reason",
            "watch_bucket",
            "pressure_conflict_level",
            "confidence_summary",
            "entry_above",
            "entry_below",
            "invalidate_price",
            "first_target_price",
        )
        if self._strategy_decision_has_structured_columns is None:
            self._strategy_decision_has_structured_columns = self._table_has_columns(
                "strategy_decisions_5m",
                structured_columns,
            )

        if self._strategy_decision_has_structured_columns:
            row = list(row)
            row[26] = json.dumps(row[26] or [])
            row[27] = json.dumps(row[27] or [])
            query = """
            INSERT INTO strategy_decisions_5m
            (
                ts, instrument, price, signal, reason, strategy_score, score_factors,
                volume_signal, oi_bias, oi_trend, build_up, pressure_bias,
                ce_delta_total, pe_delta_total, pcr,
                orb_high, orb_low, vwap, atr, strike,
                base_bias, setup_type, signal_quality, tradability, time_regime, oi_mode,
                blockers_json, cautions_json, candidate_signal_type, candidate_signal_grade,
                candidate_confidence, actionable_block_reason, watch_bucket,
                pressure_conflict_level, confidence_summary, entry_above, entry_below,
                invalidate_price, first_target_price
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                strike = EXCLUDED.strike,
                base_bias = EXCLUDED.base_bias,
                setup_type = EXCLUDED.setup_type,
                signal_quality = EXCLUDED.signal_quality,
                tradability = EXCLUDED.tradability,
                time_regime = EXCLUDED.time_regime,
                oi_mode = EXCLUDED.oi_mode,
                blockers_json = EXCLUDED.blockers_json,
                cautions_json = EXCLUDED.cautions_json,
                candidate_signal_type = EXCLUDED.candidate_signal_type,
                candidate_signal_grade = EXCLUDED.candidate_signal_grade,
                candidate_confidence = EXCLUDED.candidate_confidence,
                actionable_block_reason = EXCLUDED.actionable_block_reason,
                watch_bucket = EXCLUDED.watch_bucket,
                pressure_conflict_level = EXCLUDED.pressure_conflict_level,
                confidence_summary = EXCLUDED.confidence_summary,
                entry_above = EXCLUDED.entry_above,
                entry_below = EXCLUDED.entry_below,
                invalidate_price = EXCLUDED.invalidate_price,
                first_target_price = EXCLUDED.first_target_price;
            """
            self._execute(query, tuple(row))
            return

        legacy_row = row[:26]
        query = """
        INSERT INTO strategy_decisions_5m
        (
            ts, instrument, price, signal, reason, strategy_score, score_factors,
            volume_signal, oi_bias, oi_trend, build_up, pressure_bias,
            ce_delta_total, pe_delta_total, pcr,
            orb_high, orb_low, vwap, atr, strike,
            base_bias, setup_type, signal_quality, tradability, time_regime, oi_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            strike = EXCLUDED.strike,
            base_bias = EXCLUDED.base_bias,
            setup_type = EXCLUDED.setup_type,
            signal_quality = EXCLUDED.signal_quality,
            tradability = EXCLUDED.tradability,
            time_regime = EXCLUDED.time_regime,
            oi_mode = EXCLUDED.oi_mode;
        """
        self._execute(query, legacy_row)

    def insert_signal_issued(self, row):
        """
        row = (
            ts, instrument, signal, price, strike, strategy_score,
            signal_quality, setup_type, tradability, time_regime, oi_mode,
            reason, confidence_summary, entry_above, entry_below, invalidate_price,
            first_target_price, telegram_sent, monitor_started, entry_window_end
        )
        """
        query = """
        INSERT INTO signals_issued
        (
            ts, instrument, signal, price, strike, strategy_score,
            signal_quality, setup_type, tradability, time_regime, oi_mode,
            reason, confidence_summary, entry_above, entry_below, invalidate_price,
            first_target_price, telegram_sent, monitor_started, entry_window_end
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument, signal, strike) DO UPDATE
        SET price = EXCLUDED.price,
            strategy_score = EXCLUDED.strategy_score,
            signal_quality = EXCLUDED.signal_quality,
            setup_type = EXCLUDED.setup_type,
            tradability = EXCLUDED.tradability,
            time_regime = EXCLUDED.time_regime,
            oi_mode = EXCLUDED.oi_mode,
            reason = EXCLUDED.reason,
            confidence_summary = EXCLUDED.confidence_summary,
            entry_above = EXCLUDED.entry_above,
            entry_below = EXCLUDED.entry_below,
            invalidate_price = EXCLUDED.invalidate_price,
            first_target_price = EXCLUDED.first_target_price,
            telegram_sent = EXCLUDED.telegram_sent,
            monitor_started = EXCLUDED.monitor_started,
            entry_window_end = EXCLUDED.entry_window_end;
        """
        self._execute(query, row)

    def insert_alert_review_5m(self, row):
        """
        row = (
            alert_ts, instrument, alert_kind, direction, setup_type, watch_bucket,
            usefulness, outcome_tag, lookahead_minutes, max_favorable_points,
            max_adverse_points, close_move_points, blockers_json, cautions_json, notes
        )
        """
        serialized = list(row)
        serialized[12] = json.dumps(serialized[12] or [])
        serialized[13] = json.dumps(serialized[13] or [])
        query = """
        INSERT INTO alert_reviews_5m
        (
            alert_ts, instrument, alert_kind, direction, setup_type, watch_bucket,
            usefulness, outcome_tag, lookahead_minutes, max_favorable_points,
            max_adverse_points, close_move_points, blockers_json, cautions_json, notes
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (alert_ts, instrument, alert_kind, direction, setup_type) DO UPDATE
        SET watch_bucket = EXCLUDED.watch_bucket,
            usefulness = EXCLUDED.usefulness,
            outcome_tag = EXCLUDED.outcome_tag,
            lookahead_minutes = EXCLUDED.lookahead_minutes,
            max_favorable_points = EXCLUDED.max_favorable_points,
            max_adverse_points = EXCLUDED.max_adverse_points,
            close_move_points = EXCLUDED.close_move_points,
            blockers_json = EXCLUDED.blockers_json,
            cautions_json = EXCLUDED.cautions_json,
            notes = EXCLUDED.notes;
        """
        self._execute(query, tuple(serialized))

    def insert_trade_monitor_event_1m(self, row):
        """
        row = (
            ts, instrument, signal, entry_ts, entry_price, current_price,
            pnl_points, guidance, reason, structure_state, quality, time_regime
        )
        """
        query = """
        INSERT INTO trade_monitor_events_1m
        (
            ts, instrument, signal, entry_ts, entry_price, current_price,
            pnl_points, guidance, reason, structure_state, quality, time_regime
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument, entry_ts, signal) DO UPDATE
        SET current_price = EXCLUDED.current_price,
            pnl_points = EXCLUDED.pnl_points,
            guidance = EXCLUDED.guidance,
            reason = EXCLUDED.reason,
            structure_state = EXCLUDED.structure_state,
            quality = EXCLUDED.quality,
            time_regime = EXCLUDED.time_regime;
        """
        self._execute(query, row)

    def insert_scalp_signal(self, row):
        """
        Insert scalp signal to scalp_signals_1m table
        row = (
            ts, instrument, signal, entry_price, target_price, stop_loss,
            score, reason, status, exit_ts, exit_price, pnl
        )
        """
        query = """
        INSERT INTO scalp_signals_1m
        (
            ts, instrument, signal, entry_price, target_price, stop_loss,
            score, reason, status, exit_ts, exit_price, pnl
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ts, instrument) DO UPDATE
        SET status = EXCLUDED.status,
            exit_ts = EXCLUDED.exit_ts,
            exit_price = EXCLUDED.exit_price,
            pnl = EXCLUDED.pnl;
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
