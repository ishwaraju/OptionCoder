from config import Config
from utils.time_utils import TimeUtils


class VolumeAnalyzer:
    def __init__(self):
        self.time_utils = TimeUtils()
        self.volumes = []
        self.avg_volume = None

    def update(self, candle):
        """
        Update volume from 5-min candle
        candle = 5-min candle
        """

        volume = candle["volume"]
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

        if self.avg_volume is None:
            return "NO_DATA"

        if current_volume > self.avg_volume * 1.5:
            return "STRONG"

        elif current_volume > self.avg_volume:
            return "NORMAL"

        else:
            return "WEAK"

    def get_volume_ratio(self, current_volume):
        """
        Volume ratio
        """
        if self.avg_volume is None or self.avg_volume == 0:
            return 0

        return round(current_volume / self.avg_volume, 2)