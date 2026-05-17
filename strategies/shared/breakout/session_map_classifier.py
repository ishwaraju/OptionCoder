from datetime import time


class SessionMapClassifier:
    @staticmethod
    def classify(*, candle_time, time_regime, recent_candles_5m, vwap):
        if candle_time is None:
            return {"phase": "UNKNOWN", "summary": "session unknown"}

        now = candle_time.time()
        recent = list(recent_candles_5m or [])[-6:]
        closes = [float(item.get("close") or 0.0) for item in recent]

        if now < time(9, 45):
            phase = "OPEN_AUCTION"
        elif now < time(10, 30):
            phase = "FIRST_DIRECTIONAL_ATTEMPT"
        elif now < time(12, 15):
            phase = "MIDDAY_RESET"
        elif now < time(14, 15):
            phase = "AFTERNOON_EXPANSION"
        else:
            phase = "LATE_SESSION"

        if len(closes) >= 3 and vwap is not None:
            above_vwap = sum(1 for c in closes[-3:] if c > vwap)
            below_vwap = sum(1 for c in closes[-3:] if c < vwap)
            if phase == "MIDDAY_RESET" and (above_vwap == 3 or below_vwap == 3):
                phase = "MIDDAY_ACCEPTANCE"
            elif phase == "LATE_SESSION" and abs(closes[-1] - closes[0]) > 0:
                phase = "LATE_EXPANSION"

        return {
            "phase": phase,
            "time_regime": time_regime,
            "summary": f"session_phase={phase} | regime={time_regime}",
        }


def classify_session_map(**kwargs):
    return SessionMapClassifier.classify(**kwargs)
