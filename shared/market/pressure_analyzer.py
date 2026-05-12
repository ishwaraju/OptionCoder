from collections import deque


class PressureAnalyzer:
    def __init__(self):
        self.last_metrics = None
        self.flow_edge_history = deque(maxlen=5)
        self.near_call_ratio_history = deque(maxlen=3)
        self.near_put_ratio_history = deque(maxlen=3)
        self.full_call_ratio_history = deque(maxlen=3)
        self.full_put_ratio_history = deque(maxlen=3)

    @staticmethod
    def _safe_ratio(numerator, denominator):
        if denominator in (None, 0):
            return 0
        return round(numerator / denominator, 2)

    @staticmethod
    def _sum(rows, key):
        return sum((row.get(key) or 0) for row in rows)

    @staticmethod
    def _top_rows(rows, key, limit=2):
        ordered = sorted(rows, key=lambda row: row.get(key, 0), reverse=True)
        return ordered[:limit]

    @staticmethod
    def _delta_sum(rows, current_key, previous_key):
        total = 0
        found = False
        for row in rows:
            current = row.get(current_key)
            previous = row.get(previous_key)
            if current is None or previous is None:
                continue
            found = True
            total += float(current) - float(previous)
        return round(total, 2) if found else 0.0

    def analyze(self, option_data, underlying_price=None, oi_ladder_data=None):
        band_snapshots = option_data.get("band_snapshots", []) if option_data else []
        atm = option_data.get("atm") if option_data else None

        if not band_snapshots or atm is None:
            self.last_metrics = None
            return None

        if underlying_price is None and option_data:
            underlying_price = option_data.get("underlying_price")

        ce_rows = [row for row in band_snapshots if row["option_type"] == "CE"]
        pe_rows = [row for row in band_snapshots if row["option_type"] == "PE"]

        near_ce_rows = [row for row in ce_rows if abs(row["distance_from_atm"]) <= 1]
        near_pe_rows = [row for row in pe_rows if abs(row["distance_from_atm"]) <= 1]
        mid_ce_rows = [row for row in ce_rows if abs(row["distance_from_atm"]) <= 3]
        mid_pe_rows = [row for row in pe_rows if abs(row["distance_from_atm"]) <= 3]

        strongest_ce = max(ce_rows, key=lambda row: row.get("volume", 0), default=None)
        strongest_pe = max(pe_rows, key=lambda row: row.get("volume", 0), default=None)
        top_ce_oi_rows = self._top_rows(ce_rows, "oi", limit=2)
        top_pe_oi_rows = self._top_rows(pe_rows, "oi", limit=2)

        near_ce_volume = self._sum(near_ce_rows, "volume")
        near_pe_volume = self._sum(near_pe_rows, "volume")
        mid_ce_volume = self._sum(mid_ce_rows, "volume")
        mid_pe_volume = self._sum(mid_pe_rows, "volume")
        full_ce_volume = self._sum(ce_rows, "volume")
        full_pe_volume = self._sum(pe_rows, "volume")

        near_ce_oi = self._sum(near_ce_rows, "oi")
        near_pe_oi = self._sum(near_pe_rows, "oi")
        full_ce_oi = self._sum(ce_rows, "oi")
        full_pe_oi = self._sum(pe_rows, "oi")

        atm_ce = next((row for row in ce_rows if row["distance_from_atm"] == 0), None)
        atm_pe = next((row for row in pe_rows if row["distance_from_atm"] == 0), None)

        near_call_pressure_ratio = self._safe_ratio(near_ce_volume, near_pe_volume)
        near_put_pressure_ratio = self._safe_ratio(near_pe_volume, near_ce_volume)
        full_call_pressure_ratio = self._safe_ratio(full_ce_volume, full_pe_volume)
        full_put_pressure_ratio = self._safe_ratio(full_pe_volume, full_ce_volume)

        self.near_call_ratio_history.append(near_call_pressure_ratio)
        self.near_put_ratio_history.append(near_put_pressure_ratio)
        self.full_call_ratio_history.append(full_call_pressure_ratio)
        self.full_put_ratio_history.append(full_put_pressure_ratio)

        smooth_near_call = round(sum(self.near_call_ratio_history) / len(self.near_call_ratio_history), 2)
        smooth_near_put = round(sum(self.near_put_ratio_history) / len(self.near_put_ratio_history), 2)
        smooth_full_call = round(sum(self.full_call_ratio_history) / len(self.full_call_ratio_history), 2)
        smooth_full_put = round(sum(self.full_put_ratio_history) / len(self.full_put_ratio_history), 2)

        prev_metrics = self.last_metrics or {}
        prev_underlying = prev_metrics.get("underlying_price")
        prev_atm_ce_ltp = prev_metrics.get("atm_ce_ltp")
        prev_atm_pe_ltp = prev_metrics.get("atm_pe_ltp")

        atm_ce_ltp = float(atm_ce.get("ltp", 0) or 0) if atm_ce else 0.0
        atm_pe_ltp = float(atm_pe.get("ltp", 0) or 0) if atm_pe else 0.0
        underlying_delta = (
            round(float(underlying_price) - float(prev_underlying), 2)
            if underlying_price is not None and prev_underlying is not None
            else None
        )
        atm_ce_ltp_delta = (
            round(atm_ce_ltp - float(prev_atm_ce_ltp), 2)
            if prev_atm_ce_ltp is not None
            else None
        )
        atm_pe_ltp_delta = (
            round(atm_pe_ltp - float(prev_atm_pe_ltp), 2)
            if prev_atm_pe_ltp is not None
            else None
        )

        near_ce_volume_delta = self._delta_sum(near_ce_rows, "volume", "previous_volume")
        near_pe_volume_delta = self._delta_sum(near_pe_rows, "volume", "previous_volume")
        near_ce_oi_delta = self._delta_sum(near_ce_rows, "oi", "previous_oi")
        near_pe_oi_delta = self._delta_sum(near_pe_rows, "oi", "previous_oi")
        mid_ce_volume_delta = self._delta_sum(mid_ce_rows, "volume", "previous_volume")
        mid_pe_volume_delta = self._delta_sum(mid_pe_rows, "volume", "previous_volume")

        bullish_score = 0.0
        bearish_score = 0.0
        flow_notes = []

        if underlying_delta is not None:
            if underlying_delta > 0:
                bullish_score += 7
                flow_notes.append("spot_up")
            elif underlying_delta < 0:
                bearish_score += 7
                flow_notes.append("spot_down")

        if atm_ce_ltp_delta is not None and atm_pe_ltp_delta is not None:
            if atm_ce_ltp_delta > 0 and atm_pe_ltp_delta < 0:
                bullish_score += 10
                flow_notes.append("ce_up_pe_down")
            elif atm_pe_ltp_delta > 0 and atm_ce_ltp_delta < 0:
                bearish_score += 10
                flow_notes.append("pe_up_ce_down")

        if near_ce_volume_delta > 0 and (atm_ce_ltp_delta or 0) > 0:
            bullish_score += 8
            flow_notes.append("ce_flow_active")
            if near_ce_oi_delta <= 0:
                bullish_score += 4
                flow_notes.append("ce_short_covering")
            elif near_ce_oi_delta > 0:
                bullish_score += 2
                flow_notes.append("ce_fresh_buying")

        if near_pe_volume_delta > 0 and (atm_pe_ltp_delta or 0) > 0:
            bearish_score += 8
            flow_notes.append("pe_flow_active")
            if near_pe_oi_delta <= 0:
                bearish_score += 4
                flow_notes.append("pe_short_covering")
            elif near_pe_oi_delta > 0:
                bearish_score += 2
                flow_notes.append("pe_fresh_buying")

        if underlying_delta is not None and underlying_delta > 0 and near_pe_oi_delta > max(near_ce_oi_delta, 0) and (atm_pe_ltp_delta or 0) <= 0:
            bullish_score += 6
            flow_notes.append("put_writing_support")
        if underlying_delta is not None and underlying_delta < 0 and near_ce_oi_delta > max(near_pe_oi_delta, 0) and (atm_ce_ltp_delta or 0) <= 0:
            bearish_score += 6
            flow_notes.append("call_writing_resistance")

        if mid_ce_volume_delta > mid_pe_volume_delta * 1.1 and (atm_ce_ltp_delta or 0) > 0:
            bullish_score += 4
        elif mid_pe_volume_delta > mid_ce_volume_delta * 1.1 and (atm_pe_ltp_delta or 0) > 0:
            bearish_score += 4

        if oi_ladder_data:
            trend = (oi_ladder_data.get("trend") or "").upper()
            build_up = (oi_ladder_data.get("build_up") or "").upper()
            wall_alert = (oi_ladder_data.get("wall_break_alert") or "").upper()
            if trend == "BULLISH":
                bullish_score += 8
                flow_notes.append("oi_trend_bullish")
            elif trend == "BEARISH":
                bearish_score += 8
                flow_notes.append("oi_trend_bearish")
            if build_up in {"LONG_BUILDUP", "SHORT_COVERING"}:
                bullish_score += 6
                flow_notes.append(build_up.lower())
            elif build_up in {"SHORT_BUILDUP", "LONG_UNWINDING"}:
                bearish_score += 6
                flow_notes.append(build_up.lower())
            if wall_alert in {"RESISTANCE_BREAK_RISK", "RESISTANCE_SHIFTING_LOWER"}:
                bullish_score += 4
            elif wall_alert in {"SUPPORT_BREAK_RISK", "SUPPORT_SHIFTING_HIGHER"}:
                bearish_score += 4

        if smooth_near_put >= 1.15 and smooth_full_put >= 1.05:
            bullish_score += 3
        elif smooth_near_call >= 1.15 and smooth_full_call >= 1.05:
            bearish_score += 3

        flow_edge = round(abs(bullish_score - bearish_score), 2)
        self.flow_edge_history.append(flow_edge)
        smoothed_edge = round(sum(self.flow_edge_history) / len(self.flow_edge_history), 2)

        if bullish_score >= bearish_score + 6:
            pressure_bias = "BULLISH"
        elif bearish_score >= bullish_score + 6:
            pressure_bias = "BEARISH"
        else:
            pressure_bias = "NEUTRAL"

        flow_strength = "STRONG" if smoothed_edge >= 14 else "MODERATE" if smoothed_edge >= 7 else "MIXED"

        metrics = {
            "atm": atm,
            "pressure_bias": pressure_bias,
            "flow_strength": flow_strength,
            "bullish_pressure_score": round(bullish_score, 2),
            "bearish_pressure_score": round(bearish_score, 2),
            "flow_edge": flow_edge,
            "smoothed_flow_edge": smoothed_edge,
            "flow_notes": flow_notes,
            "underlying_price": float(underlying_price) if underlying_price is not None else None,
            "underlying_delta": underlying_delta,
            "near_ce_volume": near_ce_volume,
            "near_pe_volume": near_pe_volume,
            "mid_ce_volume": mid_ce_volume,
            "mid_pe_volume": mid_pe_volume,
            "full_ce_volume": full_ce_volume,
            "full_pe_volume": full_pe_volume,
            "near_ce_oi": near_ce_oi,
            "near_pe_oi": near_pe_oi,
            "full_ce_oi": full_ce_oi,
            "full_pe_oi": full_pe_oi,
            "near_call_pressure_ratio": near_call_pressure_ratio,
            "near_put_pressure_ratio": near_put_pressure_ratio,
            "full_call_pressure_ratio": full_call_pressure_ratio,
            "full_put_pressure_ratio": full_put_pressure_ratio,
            "smooth_near_call_pressure_ratio": smooth_near_call,
            "smooth_near_put_pressure_ratio": smooth_near_put,
            "smooth_full_call_pressure_ratio": smooth_full_call,
            "smooth_full_put_pressure_ratio": smooth_full_put,
            "near_ce_volume_delta": near_ce_volume_delta,
            "near_pe_volume_delta": near_pe_volume_delta,
            "mid_ce_volume_delta": mid_ce_volume_delta,
            "mid_pe_volume_delta": mid_pe_volume_delta,
            "near_ce_oi_delta": near_ce_oi_delta,
            "near_pe_oi_delta": near_pe_oi_delta,
            "atm_ce_ltp": atm_ce_ltp,
            "atm_pe_ltp": atm_pe_ltp,
            "atm_ce_ltp_delta": atm_ce_ltp_delta,
            "atm_pe_ltp_delta": atm_pe_ltp_delta,
            "atm_ce_volume": atm_ce.get("volume", 0) if atm_ce else 0,
            "atm_pe_volume": atm_pe.get("volume", 0) if atm_pe else 0,
            "atm_ce_oi": atm_ce.get("oi", 0) if atm_ce else 0,
            "atm_pe_oi": atm_pe.get("oi", 0) if atm_pe else 0,
            "atm_ce_concentration": self._safe_ratio(atm_ce.get("volume", 0) if atm_ce else 0, full_ce_volume),
            "atm_pe_concentration": self._safe_ratio(atm_pe.get("volume", 0) if atm_pe else 0, full_pe_volume),
            "strongest_ce_strike": strongest_ce.get("strike") if strongest_ce else None,
            "strongest_pe_strike": strongest_pe.get("strike") if strongest_pe else None,
            "strongest_ce_volume": strongest_ce.get("volume", 0) if strongest_ce else 0,
            "strongest_pe_volume": strongest_pe.get("volume", 0) if strongest_pe else 0,
            "strongest_ce_distance": strongest_ce.get("distance_from_atm") if strongest_ce else None,
            "strongest_pe_distance": strongest_pe.get("distance_from_atm") if strongest_pe else None,
            "top_call_wall_strike": top_ce_oi_rows[0].get("strike") if top_ce_oi_rows else None,
            "top_put_wall_strike": top_pe_oi_rows[0].get("strike") if top_pe_oi_rows else None,
            "top_call_wall_oi": top_ce_oi_rows[0].get("oi", 0) if top_ce_oi_rows else 0,
            "top_put_wall_oi": top_pe_oi_rows[0].get("oi", 0) if top_pe_oi_rows else 0,
            "second_call_wall_strike": top_ce_oi_rows[1].get("strike") if len(top_ce_oi_rows) > 1 else None,
            "second_put_wall_strike": top_pe_oi_rows[1].get("strike") if len(top_pe_oi_rows) > 1 else None,
            "second_call_wall_oi": top_ce_oi_rows[1].get("oi", 0) if len(top_ce_oi_rows) > 1 else 0,
            "second_put_wall_oi": top_pe_oi_rows[1].get("oi", 0) if len(top_pe_oi_rows) > 1 else 0,
        }

        top_call_wall_oi = metrics["top_call_wall_oi"] or 0
        top_put_wall_oi = metrics["top_put_wall_oi"] or 0
        second_call_wall_oi = metrics["second_call_wall_oi"] or 0
        second_put_wall_oi = metrics["second_put_wall_oi"] or 0
        call_wall_strength = self._safe_ratio(top_call_wall_oi, second_call_wall_oi) if second_call_wall_oi else (1.0 if top_call_wall_oi else 0.0)
        put_wall_strength = self._safe_ratio(top_put_wall_oi, second_put_wall_oi) if second_put_wall_oi else (1.0 if top_put_wall_oi else 0.0)
        metrics["call_wall_strength_ratio"] = round(call_wall_strength, 2) if call_wall_strength is not None else 0.0
        metrics["put_wall_strength_ratio"] = round(put_wall_strength, 2) if put_wall_strength is not None else 0.0
        metrics["wall_pressure_edge"] = round(abs(metrics["call_wall_strength_ratio"] - metrics["put_wall_strength_ratio"]), 2)

        self.last_metrics = metrics
        return metrics
