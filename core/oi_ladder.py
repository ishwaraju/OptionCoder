class OILadder:
    def __init__(self):
        self.prev_ce_oi = {}
        self.prev_pe_oi = {}
        self.prev_max_ce_strike = None
        self.prev_max_pe_strike = None

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
        weighted_ce_delta = self._weighted_sum(ce_delta, atm, strike_gap)
        weighted_pe_delta = self._weighted_sum(pe_delta, atm, strike_gap)

        if price_change > 0:
            bullish_build = max(weighted_pe_delta, 0)
            bullish_cover = max(-weighted_ce_delta, 0)
            if bullish_build == 0 and bullish_cover == 0:
                return "NO_CLEAR_SIGNAL"
            return "LONG_BUILDUP" if bullish_build >= bullish_cover else "SHORT_COVERING"

        if price_change < 0:
            bearish_build = max(weighted_ce_delta, 0)
            bearish_unwind = max(-weighted_pe_delta, 0)
            if bearish_build == 0 and bearish_unwind == 0:
                return "NO_CLEAR_SIGNAL"
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
    def analyze(self, ce_oi, pe_oi, price_change=0, atm=None):
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

        self.prev_ce_oi = dict(ce_oi)
        self.prev_pe_oi = dict(pe_oi)

        return {
            "support": support,
            "resistance": resistance,
            "trend": trend,
            "pcr": pcr,
            "build_up": buildup,
            "oi_shift": shift,
            "ce_delta_total": sum(ce_delta.values()),
            "pe_delta_total": sum(pe_delta.values()),
        }
