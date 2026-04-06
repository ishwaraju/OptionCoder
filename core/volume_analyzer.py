from config import Config
from utils.time_utils import TimeUtils


class VolumeAnalyzer:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.volumes = []
        self.avg_volume = None
        self.baseline_avg_volume = None
        self.current_day = None

    def _get_session_day(self, candle):
        candle_dt = candle.get("time") or candle.get("datetime") or candle.get("close_time")

        if hasattr(candle_dt, "date"):
            return candle_dt.date().isoformat()

        return self.time_utils.today_str()

    def reset(self):
        self.volumes = []
        self.avg_volume = None
        self.baseline_avg_volume = None

    def update(self, candle):
        """
        Update volume from 5-min candle
        candle = 5-min candle
        """
        session_day = self._get_session_day(candle)
        if self.current_day != session_day:
            self.reset()
            self.current_day = session_day

        volume = candle["volume"]

        # Baseline for classifying this candle should come from prior candles only.
        if self.volumes:
            self.baseline_avg_volume = sum(self.volumes) / len(self.volumes)
        else:
            self.baseline_avg_volume = None

        self.volumes.append(volume)

        # Keep last 10 candles
        if len(self.volumes) > 10:
            self.volumes.pop(0)

        # Calculate average volume
        if len(self.volumes) > 0:
            self.avg_volume = sum(self.volumes) / len(self.volumes)

        return self.avg_volume

    def get_volume_signal(self, current_volume):
        """
        Volume strength signal
        """

        baseline = self.baseline_avg_volume
        if baseline is None:
            return "NO_DATA"

        if current_volume > baseline * 1.5:
            return "STRONG"

        elif current_volume > baseline:
            return "NORMAL"

        else:
            return "WEAK"

    def get_volume_ratio(self, current_volume):
        """
        Volume ratio
        """
        baseline = self.baseline_avg_volume
        if baseline is None or baseline == 0:
            return 0

        return round(current_volume / baseline, 2)
