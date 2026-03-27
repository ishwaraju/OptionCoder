class OILadder:
    def __init__(self):
        self.prev_total_oi = 0

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
    def get_oi_trend(self, ce_oi, pe_oi):
        total_ce = sum(ce_oi.values())
        total_pe = sum(pe_oi.values())

        if total_pe > total_ce:
            return "BULLISH"
        elif total_ce > total_pe:
            return "BEARISH"
        else:
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
    def oi_build_up(self, price_change, oi_change):
        if price_change > 0 and oi_change > 0:
            return "LONG_BUILDUP"
        elif price_change < 0 and oi_change > 0:
            return "SHORT_BUILDUP"
        elif price_change > 0 and oi_change < 0:
            return "SHORT_COVERING"
        elif price_change < 0 and oi_change < 0:
            return "LONG_UNWINDING"
        else:
            return "NO_CLEAR_SIGNAL"

    # =========================
    # OI Shift Detection
    # =========================
    def get_oi_shift(self, ce_oi, pe_oi):
        if not ce_oi or not pe_oi:
            return "NO_DATA"

        max_ce_strike = max(ce_oi, key=ce_oi.get)
        max_pe_strike = max(pe_oi, key=pe_oi.get)

        if max_pe_strike > max_ce_strike:
            return "OI_SHIFT_UP"
        elif max_ce_strike > max_pe_strike:
            return "OI_SHIFT_DOWN"
        else:
            return "NO_SHIFT"

    # =========================
    # Full Analysis
    # =========================
    def analyze(self, ce_oi, pe_oi, price_change=0, oi_change=0):
        support, resistance = self.get_support_resistance(ce_oi, pe_oi)
        trend = self.get_oi_trend(ce_oi, pe_oi)
        pcr = self.calculate_pcr(ce_oi, pe_oi)
        buildup = self.oi_build_up(price_change, oi_change)
        shift = self.get_oi_shift(ce_oi, pe_oi)

        return {
            "support": support,
            "resistance": resistance,
            "trend": trend,
            "pcr": pcr,
            "build_up": buildup,
            "oi_shift": shift
        }