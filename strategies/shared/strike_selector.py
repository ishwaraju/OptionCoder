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

    def select_strike(self, price, signal, volume_signal, strategy_score=0, pressure_metrics=None, cautions=None, option_chain_data=None, setup_type=None, time_regime=None):
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
        )
        return strike

    def select_strike_with_reason(self, price, signal, volume_signal, strategy_score=0, pressure_metrics=None, cautions=None, option_chain_data=None, setup_type=None, time_regime=None):
        """
        Decide which strike to trade and explain why that strike was chosen.
        """

        current_time = self.time_utils.current_time()
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

        if strategy_score >= 85 and volume_signal == "STRONG" and aligned_pressure and strongest_nearby:
            return atm, "ATM because score, volume, and nearby pressure are strongly aligned"

        if expiry_mode and premium_noise:
            return self.get_deeper_itm_strike(price, signal, steps=2), "Deeper ITM because expiry premium is noisy and tighter spreads matter more"

        if expiry_mode or time_regime in {"LATE_DAY", "ENDGAME"} or setup_type in {"REVERSAL", "TRAP_REVERSAL"}:
            return self.get_itm_strike(price, signal), "ITM because expiry/late-day or reversal setups benefit from cleaner premium behavior"

        if current_time.hour >= 13 or strategy_score < 60:
            return self.get_deeper_itm_strike(price, signal, steps=2), "Deeper ITM because session is late or conviction is weak"

        if volume_signal == "WEAK" or not aligned_pressure:
            return self.get_itm_strike(price, signal), "ITM because volume or pressure confirmation is not strong enough"

        if strategy_score < 75:
            return self.get_itm_strike(price, signal), "ITM because score is moderate and setup is not top-tier"

        return atm, "ATM because conviction is good and pressure context is acceptable"
