class SignalStateManager:
    @staticmethod
    def reset_retest_setup(strategy):
        strategy.retest_setup = None

    @staticmethod
    def reset_confirmation_setup(strategy):
        strategy.confirmation_setup = None

    @staticmethod
    def set_retest_setup(strategy, direction, level, current_bar_time, score):
        strategy.retest_setup = {
            "direction": direction,
            "level": level,
            "bars_remaining": strategy.retest_bars_max,
            "session_day": current_bar_time.date() if current_bar_time is not None else strategy.time_utils.now_ist().date(),
            "score": score,
        }

    @staticmethod
    def set_confirmation_setup(strategy, direction, level, current_bar_time, score):
        bars_remaining = 3 if (score or 0) >= 68 else 2
        strategy.confirmation_setup = {
            "direction": direction,
            "level": level,
            "bars_remaining": bars_remaining,
            "session_day": current_bar_time.date() if current_bar_time is not None else strategy.time_utils.now_ist().date(),
            "score": score,
        }

    @staticmethod
    def should_suppress_duplicate(strategy, direction, signal_type, current_bar_time, level=None):
        if current_bar_time is None or signal_type not in {"BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "REVERSAL", "TRAP_REVERSAL"}:
            return False
        last = strategy.last_emitted_signal
        if not last or last["session_day"] != current_bar_time.date():
            return False
        if last["direction"] != direction or last["signal_type"] != signal_type:
            return False
        bars_apart = int((current_bar_time - last["time"]).total_seconds() // 300)
        max_bars_apart = 1 if signal_type in {"REVERSAL", "TRAP_REVERSAL"} else 2
        if bars_apart > max_bars_apart:
            return False
        if level is None or last["level"] is None:
            return True
        return abs(level - last["level"]) <= max(last["buffer"] * 2, 10)

    @staticmethod
    def mark_signal_emitted(strategy, direction, signal_type, current_bar_time, level=None, buffer=0):
        if current_bar_time is None:
            return
        strategy.last_emitted_signal = {
            "direction": direction,
            "signal_type": signal_type,
            "time": current_bar_time,
            "session_day": current_bar_time.date(),
            "level": level,
            "buffer": buffer or 0,
        }

    @staticmethod
    def update_retest_setup(strategy, current_bar_time):
        if not strategy.retest_setup:
            return
        session_day = current_bar_time.date() if current_bar_time is not None else strategy.time_utils.now_ist().date()
        if strategy.retest_setup["session_day"] != session_day:
            SignalStateManager.reset_retest_setup(strategy)
            return
        strategy.retest_setup["bars_remaining"] -= 1
        if strategy.retest_setup["bars_remaining"] <= 0:
            SignalStateManager.reset_retest_setup(strategy)

    @staticmethod
    def update_confirmation_setup(strategy, current_bar_time):
        if not strategy.confirmation_setup:
            return
        session_day = current_bar_time.date() if current_bar_time is not None else strategy.time_utils.now_ist().date()
        if strategy.confirmation_setup["session_day"] != session_day:
            SignalStateManager.reset_confirmation_setup(strategy)
            return
        strategy.confirmation_setup["bars_remaining"] -= 1
        if strategy.confirmation_setup["bars_remaining"] <= 0:
            SignalStateManager.reset_confirmation_setup(strategy)


def reset_retest_setup(strategy):
    return SignalStateManager.reset_retest_setup(strategy)


def reset_confirmation_setup(strategy):
    return SignalStateManager.reset_confirmation_setup(strategy)


def set_retest_setup(strategy, direction, level, current_bar_time, score):
    return SignalStateManager.set_retest_setup(strategy, direction, level, current_bar_time, score)


def set_confirmation_setup(strategy, direction, level, current_bar_time, score):
    return SignalStateManager.set_confirmation_setup(strategy, direction, level, current_bar_time, score)


def should_suppress_duplicate(strategy, direction, signal_type, current_bar_time, level=None):
    return SignalStateManager.should_suppress_duplicate(strategy, direction, signal_type, current_bar_time, level=level)


def mark_signal_emitted(strategy, direction, signal_type, current_bar_time, level=None, buffer=0):
    return SignalStateManager.mark_signal_emitted(strategy, direction, signal_type, current_bar_time, level=level, buffer=buffer)


def update_retest_setup(strategy, current_bar_time):
    return SignalStateManager.update_retest_setup(strategy, current_bar_time)


def update_confirmation_setup(strategy, current_bar_time):
    return SignalStateManager.update_confirmation_setup(strategy, current_bar_time)
