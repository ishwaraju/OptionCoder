class OILadder:
    def __init__(self):
        self.prev_ce_oi = {}
        self.prev_pe_oi = {}
        self.prev_max_ce_strike = None
        self.prev_max_pe_strike = None
        self.prev_support = None
        self.prev_resistance = None
        self.prev_support_strength = None
        self.prev_resistance_strength = None

    def _get_strike_gap(self, ce_oi, pe_oi):
        strikes = sorted(set(ce_oi.keys()) | set(pe_oi.keys()))
        if len(strikes) < 2:
            return 1

        gaps = [b - a for a, b in zip(strikes, strikes[1:]) if (b - a) > 0]
        return min(gaps) if gaps else 1

    def _calc_deltas(self, current_map, previous_map):
        all_strikes = set(current_map.keys()) | set(previous_map.keys())
        return {
            strike: current_map.get(strike, 0) - previous_map.get(strike, 0)
            for strike in all_strikes
        }

    def _weighted_sum(self, value_map, atm, strike_gap):
        total = 0.0
        for strike, value in value_map.items():
            distance_steps = abs(strike - atm) / strike_gap if strike_gap else 0
            weight = 1 / (1 + distance_steps)
            total += value * weight
        return total

    def _scope_map(self, value_map, atm, strike_gap, max_distance_steps=2):
        scoped = {}
        if not strike_gap:
            return dict(value_map)
        for strike, value in value_map.items():
            distance_steps = abs(strike - atm) / strike_gap
            if distance_steps <= max_distance_steps:
                scoped[strike] = value
        return scoped

    @staticmethod
    def _strongest_strike(value_map, positive=True):
        if not value_map:
            return None, 0
        filtered = {
            strike: value
            for strike, value in value_map.items()
            if (value > 0 if positive else value < 0)
        }
        if not filtered:
            return None, 0
        if positive:
            strike = max(filtered, key=filtered.get)
            return strike, filtered[strike]
        strike = min(filtered, key=filtered.get)
        return strike, filtered[strike]

    def _pressure_scores(self, ce_delta, pe_delta, ce_oi, pe_oi, atm, strike_gap):
        near_ce_delta = self._scope_map(ce_delta, atm, strike_gap, max_distance_steps=2)
        near_pe_delta = self._scope_map(pe_delta, atm, strike_gap, max_distance_steps=2)
        weighted_ce_delta = self._weighted_sum(near_ce_delta, atm, strike_gap)
        weighted_pe_delta = self._weighted_sum(near_pe_delta, atm, strike_gap)
        weighted_ce_oi = self._weighted_sum(self._scope_map(ce_oi, atm, strike_gap, 2), atm, strike_gap)
        weighted_pe_oi = self._weighted_sum(self._scope_map(pe_oi, atm, strike_gap, 2), atm, strike_gap)

        bullish_score = 0.0
        bearish_score = 0.0

        if weighted_pe_delta > 0:
            bullish_score += weighted_pe_delta
        if weighted_ce_delta < 0:
            bullish_score += abs(weighted_ce_delta) * 0.7

        if weighted_ce_delta > 0:
            bearish_score += weighted_ce_delta
        if weighted_pe_delta < 0:
            bearish_score += abs(weighted_pe_delta) * 0.7

        if weighted_pe_oi > weighted_ce_oi:
            bullish_score += (weighted_pe_oi - weighted_ce_oi) * 0.05
        elif weighted_ce_oi > weighted_pe_oi:
            bearish_score += (weighted_ce_oi - weighted_pe_oi) * 0.05

        return {
            "bullish": round(bullish_score, 2),
            "bearish": round(bearish_score, 2),
            "weighted_ce_delta": round(weighted_ce_delta, 2),
            "weighted_pe_delta": round(weighted_pe_delta, 2),
            "weighted_ce_oi": round(weighted_ce_oi, 2),
            "weighted_pe_oi": round(weighted_pe_oi, 2),
        }

    def _build_oi_summary(self, support, resistance, pressure_scores, strongest_put_write, strongest_call_write, strongest_put_unwind, strongest_call_unwind):
        bullish = pressure_scores["bullish"]
        bearish = pressure_scores["bearish"]
        if bullish >= bearish * 1.2 and bullish > 0:
            bias = "BULLISH"
        elif bearish >= bullish * 1.2 and bearish > 0:
            bias = "BEARISH"
        else:
            bias = "MIXED"

        parts = [f"OI {bias}"]
        if support:
            parts.append(f"S {support}")
        if resistance:
            parts.append(f"R {resistance}")
        if strongest_put_write[0] is not None and strongest_put_write[1] > 0:
            parts.append(f"put write {strongest_put_write[0]}")
        if strongest_call_write[0] is not None and strongest_call_write[1] > 0:
            parts.append(f"call write {strongest_call_write[0]}")
        if strongest_put_unwind[0] is not None and strongest_put_unwind[1] < 0:
            parts.append(f"put unwind {strongest_put_unwind[0]}")
        if strongest_call_unwind[0] is not None and strongest_call_unwind[1] < 0:
            parts.append(f"call unwind {strongest_call_unwind[0]}")
        return " | ".join(parts)

    @staticmethod
    def _wall_state(current_strength, previous_strength):
        if previous_strength in (None, 0) or current_strength is None:
            return "UNKNOWN"
        change_ratio = (current_strength - previous_strength) / previous_strength if previous_strength else 0
        if change_ratio >= 0.12:
            return "STRENGTHENING"
        if change_ratio <= -0.12:
            return "WEAKENING"
        return "STABLE"

    @staticmethod
    def _strike_shift_label(current_strike, previous_strike, strike_gap):
        if current_strike is None or previous_strike is None or not strike_gap:
            return "NO_DATA"
        shift_steps = int((current_strike - previous_strike) / strike_gap)
        if shift_steps == 0:
            return "NO_SHIFT"
        if shift_steps > 0:
            return f"UP_{shift_steps}"
        return f"DOWN_{abs(shift_steps)}"

    def _volume_momentum_scores(self, ce_volume_delta, pe_volume_delta, atm, strike_gap):
        near_ce_delta = self._scope_map(ce_volume_delta, atm, strike_gap, max_distance_steps=2)
        near_pe_delta = self._scope_map(pe_volume_delta, atm, strike_gap, max_distance_steps=2)
        weighted_ce = self._weighted_sum(near_ce_delta, atm, strike_gap)
        weighted_pe = self._weighted_sum(near_pe_delta, atm, strike_gap)
        return {
            "bullish": round(max(weighted_pe, 0), 2),
            "bearish": round(max(weighted_ce, 0), 2),
            "weighted_ce_volume_delta": round(weighted_ce, 2),
            "weighted_pe_volume_delta": round(weighted_pe, 2),
        }

    def _price_vs_oi_divergence(
        self,
        price_change,
        pressure_scores,
        volume_scores,
        support_wall_state,
        resistance_wall_state,
    ):
        bullish_oi = pressure_scores["bullish"] > pressure_scores["bearish"] * 1.2 and volume_scores["bullish"] >= volume_scores["bearish"] * 0.9
        bearish_oi = pressure_scores["bearish"] > pressure_scores["bullish"] * 1.2 and volume_scores["bearish"] >= volume_scores["bullish"] * 0.9

        if price_change < 0 and bullish_oi and support_wall_state in {"STRENGTHENING", "STABLE"}:
            return "BULLISH_DIVERGENCE"
        if price_change > 0 and bearish_oi and resistance_wall_state in {"STRENGTHENING", "STABLE"}:
            return "BEARISH_DIVERGENCE"
        return "NONE"

    def _wall_break_alert(
        self,
        price,
        support,
        resistance,
        strike_gap,
        pressure_scores,
        volume_scores,
        support_wall_state,
        resistance_wall_state,
    ):
        if price is None or not strike_gap:
            return "NONE"

        near_buffer = strike_gap * 0.35
        bullish_oi = pressure_scores["bullish"] > pressure_scores["bearish"] * 1.15 and volume_scores["bullish"] >= volume_scores["bearish"] * 0.85
        bearish_oi = pressure_scores["bearish"] > pressure_scores["bullish"] * 1.15 and volume_scores["bearish"] >= volume_scores["bullish"] * 0.85

        if resistance is not None and price >= (resistance - near_buffer) and resistance_wall_state == "WEAKENING" and bullish_oi:
            return "RESISTANCE_BREAK_RISK"
        if support is not None and price <= (support + near_buffer) and support_wall_state == "WEAKENING" and bearish_oi:
            return "SUPPORT_BREAK_RISK"
        return "NONE"

    # =========================
    # Find Support & Resistance
    # =========================
    def get_support_resistance(self, ce_oi, pe_oi):
        """
        Support = Highest PE OI
        Resistance = Highest CE OI
        If both same strike -> take second highest
        """
        if not ce_oi or not pe_oi:
            return None, None

        # Sort by OI
        sorted_ce = sorted(ce_oi.items(), key=lambda x: x[1], reverse=True)
        sorted_pe = sorted(pe_oi.items(), key=lambda x: x[1], reverse=True)

        resistance = sorted_ce[0][0]
        support = sorted_pe[0][0]

        # If both same strike -> take second highest
        if resistance == support:
            if len(sorted_pe) > 1:
                support = sorted_pe[1][0]
            elif len(sorted_ce) > 1:
                resistance = sorted_ce[1][0]

        return support, resistance

    # =========================
    # OI Trend
    # =========================
    def get_oi_trend(self, ce_delta, pe_delta, ce_oi, pe_oi, atm, strike_gap):
        pressure_scores = self._pressure_scores(ce_delta, pe_delta, ce_oi, pe_oi, atm, strike_gap)
        bullish_score = pressure_scores["bullish"]
        bearish_score = pressure_scores["bearish"]

        if bullish_score > bearish_score * 1.18 and bullish_score > 0:
            return "BULLISH"
        if bearish_score > bullish_score * 1.18 and bearish_score > 0:
            return "BEARISH"

        weighted_ce_delta = self._weighted_sum(ce_delta, atm, strike_gap)
        weighted_pe_delta = self._weighted_sum(pe_delta, atm, strike_gap)

        if weighted_pe_delta > 0 and weighted_ce_delta <= 0:
            return "BULLISH"
        elif weighted_ce_delta > 0 and weighted_pe_delta <= 0:
            return "BEARISH"

        delta_gap = weighted_pe_delta - weighted_ce_delta
        if delta_gap > strike_gap * 1000:
            return "BULLISH"
        if delta_gap < -(strike_gap * 1000):
            return "BEARISH"

        weighted_total_ce = self._weighted_sum(ce_oi, atm, strike_gap)
        weighted_total_pe = self._weighted_sum(pe_oi, atm, strike_gap)
        total_ratio = (weighted_total_pe / weighted_total_ce) if weighted_total_ce else 0

        if total_ratio >= 1.1:
            return "BULLISH"
        if weighted_total_pe and total_ratio <= 0.9:
            return "BEARISH"

        return "NEUTRAL"

    # =========================
    # PCR
    # =========================
    def calculate_pcr(self, ce_oi, pe_oi):
        total_ce = sum(ce_oi.values())
        total_pe = sum(pe_oi.values())

        if total_ce == 0:
            return 0

        return round(total_pe / total_ce, 2)

    # =========================
    # OI Build-up Logic
    # =========================
    def oi_build_up(self, price_change, ce_delta, pe_delta, atm, strike_gap):
        pressure_scores = self._pressure_scores(ce_delta, pe_delta, {}, {}, atm, strike_gap)
        weighted_ce_delta = self._weighted_sum(ce_delta, atm, strike_gap)
        weighted_pe_delta = self._weighted_sum(pe_delta, atm, strike_gap)

        if price_change > 0:
            bullish_build = max(weighted_pe_delta, 0)
            bullish_cover = max(-weighted_ce_delta, 0)
            if bullish_build == 0 and bullish_cover == 0:
                return "NO_CLEAR_SIGNAL"
            if pressure_scores["bullish"] > pressure_scores["bearish"] * 1.2 and bullish_build > 0:
                return "LONG_BUILDUP"
            return "LONG_BUILDUP" if bullish_build >= bullish_cover else "SHORT_COVERING"

        if price_change < 0:
            bearish_build = max(weighted_ce_delta, 0)
            bearish_unwind = max(-weighted_pe_delta, 0)
            if bearish_build == 0 and bearish_unwind == 0:
                return "NO_CLEAR_SIGNAL"
            if pressure_scores["bearish"] > pressure_scores["bullish"] * 1.2 and bearish_build > 0:
                return "SHORT_BUILDUP"
            return "SHORT_BUILDUP" if bearish_build >= bearish_unwind else "LONG_UNWINDING"

        return "NO_CLEAR_SIGNAL"

    # =========================
    # OI Shift Detection
    # =========================
    def get_oi_shift(self, ce_oi, pe_oi):
        if not ce_oi or not pe_oi:
            return "NO_DATA"

        max_ce_strike = max(ce_oi, key=ce_oi.get)
        max_pe_strike = max(pe_oi, key=pe_oi.get)

        if self.prev_max_ce_strike is None or self.prev_max_pe_strike is None:
            shift = "NO_DATA"
        else:
            ce_shift = max_ce_strike - self.prev_max_ce_strike
            pe_shift = max_pe_strike - self.prev_max_pe_strike

            if ce_shift == 0 and pe_shift == 0:
                shift = "NO_SHIFT"
            elif ce_shift >= 0 and pe_shift >= 0:
                shift = "OI_SHIFT_UP"
            elif ce_shift <= 0 and pe_shift <= 0:
                shift = "OI_SHIFT_DOWN"
            else:
                shift = "MIXED_SHIFT"

        self.prev_max_ce_strike = max_ce_strike
        self.prev_max_pe_strike = max_pe_strike
        return shift

    # =========================
    # Full Analysis
    # =========================
    def analyze(self, ce_oi, pe_oi, price_change=0, atm=None, price=None, ce_volume_delta=None, pe_volume_delta=None):
        strike_gap = self._get_strike_gap(ce_oi, pe_oi)
        if atm is None:
            strikes = sorted(set(ce_oi.keys()) | set(pe_oi.keys()))
            atm = strikes[len(strikes) // 2] if strikes else 0

        ce_delta = self._calc_deltas(ce_oi, self.prev_ce_oi)
        pe_delta = self._calc_deltas(pe_oi, self.prev_pe_oi)
        support, resistance = self.get_support_resistance(ce_oi, pe_oi)
        trend = self.get_oi_trend(ce_delta, pe_delta, ce_oi, pe_oi, atm, strike_gap)
        pcr = self.calculate_pcr(ce_oi, pe_oi)
        buildup = self.oi_build_up(price_change, ce_delta, pe_delta, atm, strike_gap)
        shift = self.get_oi_shift(ce_oi, pe_oi)
        pressure_scores = self._pressure_scores(ce_delta, pe_delta, ce_oi, pe_oi, atm, strike_gap)
        volume_scores = self._volume_momentum_scores(ce_volume_delta or {}, pe_volume_delta or {}, atm, strike_gap)
        strongest_put_write = self._strongest_strike(pe_delta, positive=True)
        strongest_call_write = self._strongest_strike(ce_delta, positive=True)
        strongest_put_unwind = self._strongest_strike(pe_delta, positive=False)
        strongest_call_unwind = self._strongest_strike(ce_delta, positive=False)
        strongest_put_volume_burst = self._strongest_strike(pe_volume_delta or {}, positive=True)
        strongest_call_volume_burst = self._strongest_strike(ce_volume_delta or {}, positive=True)
        support_strength = pe_oi.get(support, 0) if support is not None else 0
        resistance_strength = ce_oi.get(resistance, 0) if resistance is not None else 0
        support_wall_state = self._wall_state(support_strength, self.prev_support_strength if support == self.prev_support else None)
        resistance_wall_state = self._wall_state(resistance_strength, self.prev_resistance_strength if resistance == self.prev_resistance else None)
        support_shift = self._strike_shift_label(support, self.prev_support, strike_gap)
        resistance_shift = self._strike_shift_label(resistance, self.prev_resistance, strike_gap)
        divergence = self._price_vs_oi_divergence(
            price_change,
            pressure_scores,
            volume_scores,
            support_wall_state,
            resistance_wall_state,
        )
        wall_break_alert = self._wall_break_alert(
            price,
            support,
            resistance,
            strike_gap,
            pressure_scores,
            volume_scores,
            support_wall_state,
            resistance_wall_state,
        )
        oi_summary = self._build_oi_summary(
            support,
            resistance,
            pressure_scores,
            strongest_put_write,
            strongest_call_write,
            strongest_put_unwind,
            strongest_call_unwind,
        )
        if support_wall_state == "STRENGTHENING":
            oi_summary += f" | S+ {support}"
        elif support_wall_state == "WEAKENING":
            oi_summary += f" | S- {support}"
        if resistance_wall_state == "STRENGTHENING":
            oi_summary += f" | R+ {resistance}"
        elif resistance_wall_state == "WEAKENING":
            oi_summary += f" | R- {resistance}"
        if support_shift not in {"NO_DATA", "NO_SHIFT"}:
            oi_summary += f" | S shift {support_shift}"
        if resistance_shift not in {"NO_DATA", "NO_SHIFT"}:
            oi_summary += f" | R shift {resistance_shift}"
        if strongest_put_volume_burst[0] is not None and strongest_put_volume_burst[1] > 0:
            oi_summary += f" | PE vol {strongest_put_volume_burst[0]}"
        if strongest_call_volume_burst[0] is not None and strongest_call_volume_burst[1] > 0:
            oi_summary += f" | CE vol {strongest_call_volume_burst[0]}"
        if divergence != "NONE":
            oi_summary += f" | {divergence}"
        if wall_break_alert != "NONE":
            oi_summary += f" | {wall_break_alert}"

        self.prev_ce_oi = dict(ce_oi)
        self.prev_pe_oi = dict(pe_oi)
        self.prev_support = support
        self.prev_resistance = resistance
        self.prev_support_strength = support_strength
        self.prev_resistance_strength = resistance_strength

        return {
            "support": support,
            "resistance": resistance,
            "trend": trend,
            "pcr": pcr,
            "build_up": buildup,
            "oi_shift": shift,
            "ce_delta_total": sum(ce_delta.values()),
            "pe_delta_total": sum(pe_delta.values()),
            "bullish_pressure_score": pressure_scores["bullish"],
            "bearish_pressure_score": pressure_scores["bearish"],
            "weighted_ce_delta": pressure_scores["weighted_ce_delta"],
            "weighted_pe_delta": pressure_scores["weighted_pe_delta"],
            "bullish_volume_score": volume_scores["bullish"],
            "bearish_volume_score": volume_scores["bearish"],
            "weighted_ce_volume_delta": volume_scores["weighted_ce_volume_delta"],
            "weighted_pe_volume_delta": volume_scores["weighted_pe_volume_delta"],
            "support_strength": support_strength,
            "resistance_strength": resistance_strength,
            "support_wall_state": support_wall_state,
            "resistance_wall_state": resistance_wall_state,
            "support_shift": support_shift,
            "resistance_shift": resistance_shift,
            "strongest_put_write_strike": strongest_put_write[0],
            "strongest_put_write_delta": strongest_put_write[1],
            "strongest_call_write_strike": strongest_call_write[0],
            "strongest_call_write_delta": strongest_call_write[1],
            "strongest_put_unwind_strike": strongest_put_unwind[0],
            "strongest_put_unwind_delta": strongest_put_unwind[1],
            "strongest_call_unwind_strike": strongest_call_unwind[0],
            "strongest_call_unwind_delta": strongest_call_unwind[1],
            "strongest_put_volume_burst_strike": strongest_put_volume_burst[0],
            "strongest_put_volume_burst_delta": strongest_put_volume_burst[1],
            "strongest_call_volume_burst_strike": strongest_call_volume_burst[0],
            "strongest_call_volume_burst_delta": strongest_call_volume_burst[1],
            "price_vs_oi_divergence": divergence,
            "wall_break_alert": wall_break_alert,
            "oi_summary": oi_summary,
        }
