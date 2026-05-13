from shared.utils.time_utils import TimeUtils
from config import Config
from shared.utils.instrument_profile import get_instrument_profile


class StrikeSelector:
    def __init__(self, instrument=None):
        self.time_utils = TimeUtils()
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.strike_gap = self.profile["strike_step"]

    def _instrument_strike_gap(self):
        return self.strike_gap or Config.STRIKE_STEP.get(self.instrument, 50)

    def get_atm_strike(self, price):
        strike_gap = self._instrument_strike_gap()
        return round(price / strike_gap) * strike_gap

    def get_itm_strike(self, price, option_type):
        strike_gap = self._instrument_strike_gap()
        atm = self.get_atm_strike(price)

        if option_type == "CE":
            return atm - strike_gap
        else:
            return atm + strike_gap

    def get_deeper_itm_strike(self, price, option_type, steps=2):
        strike_gap = self._instrument_strike_gap() * steps
        atm = self.get_atm_strike(price)

        if option_type == "CE":
            return atm - strike_gap
        return atm + strike_gap

    @staticmethod
    def _spread_percent(row):
        if not row:
            return None
        ltp = row.get("ltp")
        spread = row.get("spread")
        if not ltp or spread is None:
            return None
        try:
            return round((float(spread) / float(ltp)) * 100.0, 4)
        except Exception:
            return None

    def _target_delta(self, strategy_score, expiry_mode, setup_type, time_regime):
        if expiry_mode or time_regime in {"LATE_DAY", "ENDGAME"} or setup_type in {"REVERSAL", "TRAP_REVERSAL"}:
            return 0.62
        if strategy_score >= 84:
            return 0.48
        if strategy_score >= 72:
            return 0.54
        return 0.6

    def _base_preferred_strike(self, price, signal, strategy_score, volume_signal, aligned_pressure, strongest_nearby, expiry_mode, premium_noise, setup_type, time_regime, candle_time=None):
        atm = self.get_atm_strike(price)

        if strategy_score >= 85 and volume_signal == "STRONG" and aligned_pressure and strongest_nearby:
            return atm, "ATM because score, volume, and nearby pressure are strongly aligned"

        if expiry_mode and premium_noise:
            return self.get_deeper_itm_strike(price, signal, steps=2), "Deeper ITM because expiry premium is noisy and tighter spreads matter more"

        if expiry_mode or time_regime in {"LATE_DAY", "ENDGAME"} or setup_type in {"REVERSAL", "TRAP_REVERSAL"}:
            return self.get_itm_strike(price, signal), "ITM because expiry/late-day or reversal setups benefit from cleaner premium behavior"

        current_clock = candle_time.time() if hasattr(candle_time, "time") else candle_time
        if current_clock is None:
            current_clock = self.time_utils.current_time()
        if current_clock.hour >= 13 or strategy_score < 60:
            return self.get_deeper_itm_strike(price, signal, steps=2), "Deeper ITM because session is late or conviction is weak"

        if volume_signal == "WEAK" or not aligned_pressure:
            return self.get_itm_strike(price, signal), "ITM because volume or pressure confirmation is not strong enough"

        if strategy_score < 75:
            return self.get_itm_strike(price, signal), "ITM because score is moderate and setup is not top-tier"

        return atm, "ATM because conviction is good and pressure context is acceptable"

    def _score_option_candidate(self, row, preferred_strike, signal, strategy_score, expiry_mode, setup_type, time_regime, option_chain_data, candle_time=None):
        strike_gap = max(self._instrument_strike_gap(), 1)
        target_delta = self._target_delta(strategy_score, expiry_mode, setup_type, time_regime)
        ltp = float(row.get("ltp") or 0.0)
        if ltp <= 0:
            return -999.0, "ltp_missing"

        spread_pct = self._spread_percent(row)
        spread_score = 22.0
        if spread_pct is not None:
            spread_score = max(0.0, 22.0 - min(spread_pct, 10.0) * 3.0)

        bid_qty = float(row.get("top_bid_quantity") or 0.0)
        ask_qty = float(row.get("top_ask_quantity") or 0.0)
        depth_score = min(12.0, min(bid_qty, ask_qty) / 18.0)
        volume_score = min(18.0, float(row.get("volume") or 0.0) / 320.0)
        oi_score = min(10.0, float(row.get("oi") or 0.0) / 22000.0)

        delta_abs = abs(float(row.get("delta") or 0.0))
        delta_score = 0.0
        if delta_abs > 0:
            delta_score = max(0.0, 16.0 * (1.0 - min(abs(delta_abs - target_delta) / 0.38, 1.0)))

        theta_abs = abs(float(row.get("theta") or 0.0))
        theta_threshold = 10.0 if expiry_mode or time_regime in {"LATE_DAY", "ENDGAME"} else 14.0
        theta_penalty = max(0.0, theta_abs - theta_threshold) * (0.8 if expiry_mode else 0.45)

        distance_steps = abs(int((int(row.get("strike") or preferred_strike) - int(preferred_strike or row.get("strike") or 0)) / strike_gap))
        proximity_score = max(0.0, 12.0 - (distance_steps * 3.5))

        atm_row = None
        band_rows = (option_chain_data or {}).get("band_snapshots") or []
        atm = (option_chain_data or {}).get("atm")
        if atm is not None:
            atm_row = next(
                (
                    item for item in band_rows
                    if item.get("strike") == atm and item.get("option_type") == signal
                ),
                None,
            )
        iv_penalty = 0.0
        row_iv = row.get("iv")
        atm_iv = atm_row.get("iv") if atm_row else None
        if row_iv is not None and atm_iv not in (None, 0):
            iv_markup = (float(row_iv) - float(atm_iv)) / float(atm_iv)
            if iv_markup > 0.14:
                iv_penalty = min(7.0, iv_markup * 18.0)

        candidate_score = round(
            spread_score + depth_score + volume_score + oi_score + delta_score + proximity_score - theta_penalty - iv_penalty,
            2,
        )
        details = [
            f"score={candidate_score}",
            f"delta={delta_abs:.2f}",
            f"spread%={spread_pct if spread_pct is not None else '-'}",
            f"theta={theta_abs:.2f}",
        ]
        return candidate_score, " | ".join(details)

    def _best_chain_candidate(self, option_chain_data, signal, preferred_strike, strategy_score, expiry_mode, setup_type, time_regime, candle_time=None):
        band_rows = (option_chain_data or {}).get("band_snapshots") or []
        if not band_rows:
            return None

        max_distance = 3 if self.instrument == "NIFTY" else 4
        candidates = [
            row for row in band_rows
            if row.get("option_type") == signal and abs(int(row.get("distance_from_atm") or 99)) <= max_distance
        ]
        if not candidates:
            return None

        ranked = []
        for row in candidates:
            score, details = self._score_option_candidate(
                row=row,
                preferred_strike=preferred_strike,
                signal=signal,
                strategy_score=strategy_score,
                expiry_mode=expiry_mode,
                setup_type=setup_type,
                time_regime=time_regime,
                option_chain_data=option_chain_data,
                candle_time=candle_time,
            )
            ranked.append((score, details, row))
        ranked.sort(key=lambda item: item[0], reverse=True)
        best_score, details, best_row = ranked[0]
        return {
            "strike": best_row.get("strike"),
            "score": best_score,
            "details": details,
        }

    def select_strike(self, price, signal, volume_signal, strategy_score=0, pressure_metrics=None, cautions=None, option_chain_data=None, setup_type=None, time_regime=None, candle_time=None):
        """
        Decide which strike to trade
        """
        strike, _ = self.select_strike_with_reason(
            price=price,
            signal=signal,
            volume_signal=volume_signal,
            strategy_score=strategy_score,
            pressure_metrics=pressure_metrics,
            cautions=cautions,
            option_chain_data=option_chain_data,
            setup_type=setup_type,
            time_regime=time_regime,
            candle_time=candle_time,
        )
        return strike

    def select_strike_with_reason(self, price, signal, volume_signal, strategy_score=0, pressure_metrics=None, cautions=None, option_chain_data=None, setup_type=None, time_regime=None, candle_time=None):
        """
        Decide which strike to trade and explain why that strike was chosen.
        """

        cautions = {str(item).lower() for item in (cautions or []) if item}
        setup_type = (setup_type or "").upper()
        time_regime = (time_regime or "").upper()
        pressure_bias = pressure_metrics.get("pressure_bias") if pressure_metrics else None
        near_call_ratio = pressure_metrics.get("near_call_pressure_ratio", 0) if pressure_metrics else 0
        near_put_ratio = pressure_metrics.get("near_put_pressure_ratio", 0) if pressure_metrics else 0
        strongest_ce_strike = pressure_metrics.get("strongest_ce_strike") if pressure_metrics else None
        strongest_pe_strike = pressure_metrics.get("strongest_pe_strike") if pressure_metrics else None
        atm = self.get_atm_strike(price)
        strike_gap = self._instrument_strike_gap()
        expiry_mode = "expiry_day_mode" in cautions
        premium_noise = any(
            flag in cautions for flag in {
                "expiry_fast_decay",
                "participation_spread_wide",
                "participation_weak",
                "opposite_pressure",
                "pressure_conflict",
            }
        )

        if signal == "CE":
            aligned_pressure = pressure_bias == "BULLISH" and near_put_ratio >= 1.2
            strongest_nearby = strongest_pe_strike in {atm - strike_gap, atm, atm + strike_gap}
        else:
            aligned_pressure = pressure_bias == "BEARISH" and near_call_ratio >= 1.2
            strongest_nearby = strongest_ce_strike in {atm - strike_gap, atm, atm + strike_gap}

        if option_chain_data and option_chain_data.get("band_snapshots"):
            atm_rows = [
                row for row in option_chain_data.get("band_snapshots") or []
                if row.get("option_type") == signal and row.get("strike") == atm
            ]
            if atm_rows:
                atm_row = atm_rows[0]
                atm_ltp = float(atm_row.get("ltp") or 0)
                atm_spread = float(atm_row.get("spread") or 0)
                atm_spread_pct = (atm_spread / atm_ltp) * 100 if atm_ltp > 0 else 999
                if atm_spread_pct >= 4.0:
                    premium_noise = True

        preferred_strike, base_reason = self._base_preferred_strike(
            price=price,
            signal=signal,
            strategy_score=strategy_score,
            volume_signal=volume_signal,
            aligned_pressure=aligned_pressure,
            strongest_nearby=strongest_nearby,
            expiry_mode=expiry_mode,
            premium_noise=premium_noise,
            setup_type=setup_type,
            time_regime=time_regime,
            candle_time=candle_time,
        )

        chain_pick = self._best_chain_candidate(
            option_chain_data=option_chain_data,
            signal=signal,
            preferred_strike=preferred_strike,
            strategy_score=strategy_score,
            expiry_mode=expiry_mode,
            setup_type=setup_type,
            time_regime=time_regime,
            candle_time=candle_time,
        )
        if chain_pick and chain_pick["strike"] is not None:
            base_reason = f"{base_reason} | buyer-quality pick {chain_pick['details']}"
            return int(chain_pick["strike"]), base_reason

        return preferred_strike, base_reason
