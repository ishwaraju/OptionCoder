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
        }

        self.last_metrics = metrics
        return metrics
