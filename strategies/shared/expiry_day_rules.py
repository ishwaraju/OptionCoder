from datetime import datetime, time
from config import Config
from strategies.shared.expiry_context import ExpirySessionContext, get_expiry_profile


class ExpiryDayRules:
    """
    Expiry-day specific filters and guidance.
    Keeps expiry logic separate from the base strategy so normal sessions stay clean.
    """

    def __init__(self, time_utils, instrument=None):
        self.time_utils = time_utils
        self.instrument = (instrument or Config.SYMBOL or "NIFTY").upper()
        self.profile = get_expiry_profile(self.instrument)

    @staticmethod
    def _parse_expiry(expiry_value):
        if not expiry_value:
            return None

        if isinstance(expiry_value, datetime):
            return expiry_value.date()

        if isinstance(expiry_value, str):
            try:
                return datetime.strptime(expiry_value, "%Y-%m-%d").date()
            except ValueError:
                return None

        return None

    def is_expiry_day(self, expiry_value):
        expiry_date = self._parse_expiry(expiry_value)
        if expiry_date is None:
            return False
        return self.time_utils.now_ist().date() == expiry_date

    def _profile_time(self, key, default_clock):
        value = self.profile.get(key)
        if not value:
            return time.fromisoformat(default_clock)
        if isinstance(value, time):
            return value
        return time.fromisoformat(str(value))

    def _profile_int(self, key, default_value):
        return int(self.profile.get(key, default_value))

    def _expiry_vwap_distance_limit(self):
        return self._profile_int("expiry_vwap_distance_limit", 45)

    def _opening_whipsaw_cutoff(self):
        return self._profile_time("opening_whipsaw_cutoff", "09:45")

    def _midday_score_floor(self):
        return self._profile_int("midday_score_floor", 78)

    def _late_session_score_floor(self):
        return self._profile_int("late_session_score_floor", 76)

    def evaluate(
            self,
            expiry_value,
            score,
            confidence,
            price,
            vwap,
            volume_signal,
            pressure_metrics,
            current_signal,
            blockers,
            cautions,
            entry_score=None,
            day_state=None,
    ):
        expiry_date = self._parse_expiry(expiry_value)
        session_context = ExpirySessionContext(
            current_date=self.time_utils.now_ist().date(),
            instrument=self.instrument,
            expiry_date=expiry_date,
        )
        session_mode = session_context.session_mode()

        if session_mode == "NORMAL":
            return {
                "is_expiry_day": False,
                "session_mode": "NORMAL",
                "allow_trade": True,
                "blockers": list(blockers),
                "cautions": list(cautions),
                "score_floor": 60,
                "adaptive_continuation_mode": False,
                "soften_build_up_requirement": False,
                "soften_pressure_conflict": False,
            }

        now = self.time_utils.current_time()
        blockers = list(blockers)
        cautions = list(cautions)
        allow_trade = True
        score_floor = 72
        pressure_bias = pressure_metrics["pressure_bias"] if pressure_metrics else "NEUTRAL"
        opposite_pressure = (
            (current_signal == "CE" and pressure_bias == "BEARISH")
            or (current_signal == "PE" and pressure_bias == "BULLISH")
        )
        day_state = day_state or {}
        active_day_state = (day_state.get("state") or "").upper()
        day_state_direction = (day_state.get("direction") or "").upper()
        aligned_day_state = (
            active_day_state in {"REVERSAL_UNDERWAY", "BULL_TREND_ACTIVE", "BEAR_TREND_ACTIVE"}
            and current_signal in {"CE", "PE"}
            and current_signal == day_state_direction
        )
        entry_score = float(entry_score or score or 0)
        high_conviction_expiry_trend = (
            current_signal in {"CE", "PE"}
            and score >= self._profile_int("high_conviction_expiry_score", 76)
            and entry_score >= self._profile_int("high_conviction_expiry_entry_score", 72)
            and confidence in {"MEDIUM", "HIGH"}
            and volume_signal in {"NORMAL", "STRONG"}
            and not opposite_pressure
        )
        if aligned_day_state and confidence in {"MEDIUM", "HIGH"} and volume_signal in {"NORMAL", "STRONG"}:
            high_conviction_expiry_trend = high_conviction_expiry_trend or (
                float(score or 0) >= max(self._profile_int("soft_expiry_score", 72), 72)
                and entry_score >= max(self._profile_int("soft_expiry_entry_score", 68), 68)
            )
        soft_expiry_trend = (
            current_signal in {"CE", "PE"}
            and score >= self._profile_int("soft_expiry_score", 72)
            and entry_score >= self._profile_int("soft_expiry_entry_score", 64)
            and confidence in {"MEDIUM", "HIGH"}
            and volume_signal != "WEAK"
        )

        adaptive_continuation_mode = False
        soften_build_up_requirement = False
        soften_pressure_conflict = False

        if session_mode == "POST_EXPIRY_REBUILD":
            score_floor = int(self.profile.get("post_expiry_score_floor", 66))
            cautions.append("post_expiry_rebuild_mode")
            adaptive_continuation_mode = self.instrument in {"NIFTY", "BANKNIFTY"}
            soften_build_up_requirement = self.instrument in {"NIFTY", "BANKNIFTY"}
            soften_pressure_conflict = self.instrument in {"NIFTY", "BANKNIFTY"}

            if confidence == "LOW" and score < score_floor + 4:
                blockers.append("post_expiry_requires_medium_plus_confidence")
                allow_trade = False
            if volume_signal == "WEAK" and score < score_floor + 2:
                blockers.append("post_expiry_weak_volume")
                allow_trade = False

            return {
                "is_expiry_day": False,
                "session_mode": session_mode,
                "allow_trade": allow_trade,
                "blockers": blockers,
                "cautions": cautions,
                "score_floor": score_floor,
                "adaptive_continuation_mode": adaptive_continuation_mode,
                "soften_build_up_requirement": soften_build_up_requirement,
                "soften_pressure_conflict": soften_pressure_conflict,
            }

        if session_mode == "PRE_EXPIRY_POSITIONING":
            score_floor = int(self.profile.get("pre_expiry_score_floor", 70))
            cautions.append("pre_expiry_positioning_mode")
            adaptive_continuation_mode = self.instrument == "SENSEX"
            soften_pressure_conflict = self.instrument == "SENSEX"
            watch_direction = (self.profile.get("pre_expiry_watch_direction") or "").upper()
            watch_min_score = self._profile_int("pre_expiry_watch_min_score", score_floor + 15)
            watch_volume_signals = {
                str(item).upper()
                for item in (self.profile.get("pre_expiry_watch_volume_signals") or ["NORMAL", "STRONG"])
            }
            watch_friendly_pre_expiry = (
                bool(watch_direction)
                and current_signal == watch_direction
                and confidence in {"MEDIUM", "HIGH"}
                and score >= max(score_floor + 13, watch_min_score)
                and volume_signal in watch_volume_signals
            )
            relaxed_pre_expiry_volume_ok = (
                current_signal in {"CE", "PE"}
                and confidence in {"MEDIUM", "HIGH"}
                and score >= score_floor
                and not opposite_pressure
            )

            if watch_friendly_pre_expiry:
                cautions.append("pre_expiry_watch_friendly")

            if confidence == "LOW" and score < score_floor + 4:
                blockers.append("pre_expiry_requires_medium_plus_confidence")
                allow_trade = False
            if volume_signal == "WEAK":
                if relaxed_pre_expiry_volume_ok:
                    cautions.append("pre_expiry_weak_volume_watch")
                elif score < score_floor + 2:
                    blockers.append("pre_expiry_weak_volume")
                    allow_trade = False

            return {
                "is_expiry_day": False,
                "session_mode": session_mode,
                "allow_trade": allow_trade,
                "blockers": blockers,
                "cautions": cautions,
                "score_floor": score_floor,
                "adaptive_continuation_mode": adaptive_continuation_mode,
                "soften_build_up_requirement": soften_build_up_requirement,
                "soften_pressure_conflict": soften_pressure_conflict,
            }

        if time(9, 15) <= now < self._opening_whipsaw_cutoff() and not high_conviction_expiry_trend:
            blockers.append("expiry_opening_whipsaw_window")
            allow_trade = False

        if time(11, 30) <= now < time(13, 30) and score < self._midday_score_floor() and not high_conviction_expiry_trend:
            blockers.append("expiry_midday_chop_window")
            allow_trade = False

        if now >= time(13, 30) and score < self._late_session_score_floor() and not high_conviction_expiry_trend:
            blockers.append("expiry_late_session_requires_high_score")
            allow_trade = False

        if confidence == "LOW":
            blockers.append("expiry_requires_medium_plus_confidence")
            allow_trade = False

        if volume_signal == "WEAK" and not soft_expiry_trend:
            blockers.append("expiry_weak_volume")
            allow_trade = False

        if vwap is not None and abs(price - vwap) > self._expiry_vwap_distance_limit():
            if high_conviction_expiry_trend:
                cautions.append("expiry_too_far_from_vwap")
            else:
                blockers.append("expiry_too_far_from_vwap")
                allow_trade = False

        opposite_pressure_floor = self._profile_int("expiry_opposite_pressure_score_floor", 78)
        if current_signal == "CE" and pressure_bias == "BEARISH" and not high_conviction_expiry_trend and score < opposite_pressure_floor:
            blockers.append("expiry_opposite_pressure")
            allow_trade = False
        elif current_signal == "PE" and pressure_bias == "BULLISH" and not high_conviction_expiry_trend and score < opposite_pressure_floor:
            blockers.append("expiry_opposite_pressure")
            allow_trade = False

        cautions.append("expiry_day_mode")
        if now >= self._profile_time("expiry_fast_decay_start", "13:00"):
            cautions.append("expiry_fast_decay")

        return {
            "is_expiry_day": True,
            "session_mode": session_mode,
            "allow_trade": allow_trade,
            "blockers": blockers,
            "cautions": cautions,
            "score_floor": score_floor,
            "adaptive_continuation_mode": False,
            "soften_build_up_requirement": False,
            "soften_pressure_conflict": False,
        }

    def adjust_strike_choice(self, expiry_value, signal, strike, strike_reason, price, strategy_score, confidence):
        if not self.is_expiry_day(expiry_value):
            return strike, strike_reason

        strike_gap = Config.STRIKE_GAP
        atm = round(price / strike_gap) * strike_gap

        if signal == "CE":
            safer_itm = atm - strike_gap
        else:
            safer_itm = atm + strike_gap

        if confidence == "HIGH" and strategy_score >= 88:
            return strike, f"{strike_reason} | Expiry day: conviction strong enough to keep current strike"

        if strike == atm:
            return safer_itm, "Expiry day: shifted 1-step ITM to reduce late premium decay risk"

        return strike, f"{strike_reason} | Expiry day: keeping safer non-ATM selection"
