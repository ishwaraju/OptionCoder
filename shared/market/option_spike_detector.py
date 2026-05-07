"""Standalone 1m option spike detector for broad cross-strike sweeps."""


class OptionSpikeDetector:
    def __init__(self, strike_step=50):
        self.strike_step = int(strike_step or 50)
        self.strike_offsets = tuple(range(-5, 6))

    @staticmethod
    def _row_key(row):
        return (row.get("strike"), row.get("option_type"))

    @staticmethod
    def _body_ratio(candle):
        high = candle.get("high")
        low = candle.get("low")
        open_price = candle.get("open")
        close = candle.get("close")
        if None in {high, low, open_price, close}:
            return 0.0
        span = float(high) - float(low)
        if span <= 0:
            return 0.0
        return abs(float(close) - float(open_price)) / span

    @staticmethod
    def _close_strength(candle, direction):
        high = candle.get("high")
        low = candle.get("low")
        close = candle.get("close")
        if None in {high, low, close}:
            return 0.0
        span = float(high) - float(low)
        if span <= 0:
            return 0.0
        if direction == "CE":
            return (float(close) - float(low)) / span
        return (float(high) - float(close)) / span

    def _derive_structure_context(self, direction, recent_candles_5m, oi_ladder_data=None):
        if not recent_candles_5m or len(recent_candles_5m) < 3:
            return {
                "trend_15m": None,
                "support": (oi_ladder_data or {}).get("support"),
                "resistance": (oi_ladder_data or {}).get("resistance"),
                "five_min_ready": True,
                "alignment": "NEUTRAL",
                "summary": "structure limited",
            }

        closed_5m = recent_candles_5m[-6:]
        latest_5m = closed_5m[-1]
        prev_5m = closed_5m[-2] if len(closed_5m) >= 2 else latest_5m
        prior_block = closed_5m[-6:-3] if len(closed_5m) >= 6 else closed_5m[:-1]
        latest_block = closed_5m[-3:]

        block_support = min(c["low"] for c in prior_block) if prior_block else min(c["low"] for c in latest_block)
        block_resistance = max(c["high"] for c in prior_block) if prior_block else max(c["high"] for c in latest_block)
        latest_close = float(latest_5m.get("close") or 0.0)
        prev_close = float(prev_5m.get("close") or 0.0)
        latest_block_close = float(latest_block[-1].get("close") or latest_close)
        latest_block_open = float(latest_block[0].get("open") or latest_close)
        trend_15m = "UP" if latest_block_close > latest_block_open else "DOWN" if latest_block_close < latest_block_open else "FLAT"

        oi_support = (oi_ladder_data or {}).get("support")
        oi_resistance = (oi_ladder_data or {}).get("resistance")
        support = oi_support if oi_support is not None else block_support
        resistance = oi_resistance if oi_resistance is not None else block_resistance

        if direction == "CE":
            alignment = "SUPPORTIVE" if trend_15m == "UP" and latest_close >= prev_close else "AGAINST" if trend_15m == "DOWN" and latest_close < prev_close else "NEUTRAL"
            five_min_ready = latest_close >= prev_close and latest_5m.get("high") >= prev_5m.get("high")
        else:
            alignment = "SUPPORTIVE" if trend_15m == "DOWN" and latest_close <= prev_close else "AGAINST" if trend_15m == "UP" and latest_close > prev_close else "NEUTRAL"
            five_min_ready = latest_close <= prev_close and latest_5m.get("low") <= prev_5m.get("low")

        return {
            "trend_15m": trend_15m,
            "support": support,
            "resistance": resistance,
            "five_min_ready": five_min_ready,
            "alignment": alignment,
            "summary": f"15m {trend_15m} | 5m {'ready' if five_min_ready else 'watch'} | S {support} | R {resistance}",
        }

    def detect(self, recent_1m_candles, snapshot_groups, recent_candles_5m=None, oi_ladder_data=None):
        if not recent_1m_candles or len(recent_1m_candles) < 3 or not snapshot_groups or len(snapshot_groups) < 2:
            return None

        latest_candle = recent_1m_candles[-1]
        previous_candle = recent_1m_candles[-2]
        prior_window = recent_1m_candles[-4:-1] if len(recent_1m_candles) >= 4 else recent_1m_candles[:-1]

        latest_rows = snapshot_groups[-1]
        previous_rows = snapshot_groups[-2]
        if not latest_rows or not previous_rows:
            return None

        latest_map = {self._row_key(row): row for row in latest_rows}
        previous_map = {self._row_key(row): row for row in previous_rows}
        atm_strike = next((row.get("atm_strike") for row in latest_rows if row.get("atm_strike") is not None), None)
        if atm_strike is None:
            return None

        scoped_strikes = [int(atm_strike + (offset * self.strike_step)) for offset in self.strike_offsets]
        strike_count = len(scoped_strikes)

        def summarize(direction):
            same_side = "CE" if direction == "CE" else "PE"
            opposite_side = "PE" if direction == "CE" else "CE"
            price_breadth = 0
            volume_breadth = 0
            opposite_collapse = 0
            same_volume_total = 0
            impulse_examples = []

            for strike in scoped_strikes:
                same_now = latest_map.get((strike, same_side))
                same_prev = previous_map.get((strike, same_side))
                opp_now = latest_map.get((strike, opposite_side))
                opp_prev = previous_map.get((strike, opposite_side))
                if not all([same_now, same_prev, opp_now, opp_prev]):
                    continue

                same_price_delta = float((same_now.get("ltp") or 0.0) - (same_prev.get("ltp") or 0.0))
                opp_price_delta = float((opp_now.get("ltp") or 0.0) - (opp_prev.get("ltp") or 0.0))
                same_volume_delta = int((same_now.get("volume") or 0) - (same_prev.get("volume") or 0))
                opp_volume_delta = int((opp_now.get("volume") or 0) - (opp_prev.get("volume") or 0))
                directional_price_ok = same_price_delta > 0 and opp_price_delta < 0

                if directional_price_ok:
                    price_breadth += 1
                if same_volume_delta > 0:
                    volume_breadth += 1
                    same_volume_total += same_volume_delta
                if opp_price_delta < 0:
                    opposite_collapse += 1
                if directional_price_ok and same_volume_delta > 0 and len(impulse_examples) < 3:
                    impulse_examples.append(
                        f"{strike}{same_side}:{round(same_price_delta, 1)}/{same_volume_delta}/{opp_volume_delta}"
                    )

            return {
                "price_breadth": price_breadth,
                "volume_breadth": volume_breadth,
                "opposite_collapse": opposite_collapse,
                "same_volume_total": same_volume_total,
                "examples": impulse_examples,
            }

        ce_summary = summarize("CE")
        pe_summary = summarize("PE")

        bullish_impulse = (
            float(latest_candle.get("close") or 0.0) > float(previous_candle.get("close") or 0.0)
            and self._close_strength(latest_candle, "CE") >= 0.58
            and self._body_ratio(latest_candle) >= 0.22
        )
        bearish_impulse = (
            float(latest_candle.get("close") or 0.0) < float(previous_candle.get("close") or 0.0)
            and self._close_strength(latest_candle, "PE") >= 0.58
            and self._body_ratio(latest_candle) >= 0.22
        )
        latest_volume = int(latest_candle.get("volume") or 0)
        baseline_volume = sum(int(row.get("volume") or 0) for row in prior_window) / max(len(prior_window), 1)
        volume_ratio = (latest_volume / baseline_volume) if baseline_volume > 0 else 1.0

        direction = None
        summary = None
        if bullish_impulse and ce_summary["price_breadth"] >= 7 and ce_summary["volume_breadth"] >= 6 and ce_summary["opposite_collapse"] >= 7:
            direction = "CE"
            summary = ce_summary
        elif bearish_impulse and pe_summary["price_breadth"] >= 7 and pe_summary["volume_breadth"] >= 6 and pe_summary["opposite_collapse"] >= 7:
            direction = "PE"
            summary = pe_summary

        if not direction or not summary:
            return None

        structure = self._derive_structure_context(
            direction=direction,
            recent_candles_5m=recent_candles_5m,
            oi_ladder_data=oi_ladder_data,
        )
        if structure.get("alignment") == "AGAINST":
            return None

        quality = "MODERATE"
        if (
            summary["price_breadth"] >= 8
            and summary["volume_breadth"] >= 8
            and summary["same_volume_total"] >= 300000
        ):
            quality = "STRONG"
        elif summary["price_breadth"] >= 7 and summary["volume_breadth"] >= 7 and volume_ratio >= 1.1:
            quality = "STRONG"
        if structure.get("alignment") == "SUPPORTIVE" and structure.get("five_min_ready") and quality == "MODERATE":
            quality = "STRONG"

        return {
            "direction": direction,
            "quality": quality,
            "stage": "15M_STRUCTURE_5M_WATCH_1M_ACTIVE",
            "price_breadth": summary["price_breadth"],
            "volume_breadth": summary["volume_breadth"],
            "opposite_collapse": summary["opposite_collapse"],
            "same_volume_total": summary["same_volume_total"],
            "underlying_volume_ratio": round(volume_ratio, 2),
            "trigger_price": float(latest_candle.get("high") if direction == "CE" else latest_candle.get("low")),
            "invalidate_price": float(latest_candle.get("low") if direction == "CE" else latest_candle.get("high")),
            "entry_reference": float(latest_candle.get("close") or 0.0),
            "summary": (
                f"{direction} 1m spike | breadth {summary['price_breadth']}/{strike_count}"
                f" | vol {summary['volume_breadth']}/{strike_count} | 1m vol x{round(volume_ratio, 2)}"
            ),
            "examples": summary["examples"],
            "structure": structure,
        }
