from collections import deque


class PressureAnalyzer:
    def __init__(self):
        self.last_metrics = None
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

    def analyze(self, option_data):
        band_snapshots = option_data.get("band_snapshots", []) if option_data else []
        atm = option_data.get("atm") if option_data else None

        if not band_snapshots or atm is None:
            self.last_metrics = None
            return None

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

        # Near-price PE participation usually supports bullish continuation,
        # while near-price CE participation often points to bearish pressure.
        if smooth_near_put >= 1.15 and smooth_full_put >= 1.05:
            pressure_bias = "BULLISH"
        elif smooth_near_call >= 1.15 and smooth_full_call >= 1.05:
            pressure_bias = "BEARISH"
        else:
            pressure_bias = "NEUTRAL"

        metrics = {
            "atm": atm,
            "pressure_bias": pressure_bias,
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
