class OptionBuyerProtection:
    @staticmethod
    def check_adx_filter(strategy, candles_5m, signal):
        if not strategy.for_option_buyer:
            return True, 0, "ADX check skipped (not option buyer mode)"
        if not candles_5m or len(candles_5m) < 15:
            return False, 0, "Insufficient candles for ADX"
        for candle in candles_5m[-15:]:
            strategy.adx_calc.update(
                candle.get('high', candle[2] if isinstance(candle, (list, tuple)) else 0),
                candle.get('low', candle[3] if isinstance(candle, (list, tuple)) else 0),
                candle.get('close', candle[4] if isinstance(candle, (list, tuple)) else 0)
            )
        adx_data = strategy.adx_calc.get_current()
        if adx_data is None:
            return False, 0, "ADX calculation failed"
        adx_value = adx_data['adx']
        di_plus = adx_data['di_plus']
        di_minus = adx_data['di_minus']
        if adx_value < strategy.option_buyer_min_adx:
            return False, 0, f"ADX {adx_value} < {strategy.option_buyer_min_adx} (sideways market)"
        if signal == "CE" and di_plus <= di_minus:
            return False, adx_value, f"ADX {adx_value} OK but DI+ {di_plus} <= DI- {di_minus}"
        if signal == "PE" and di_minus <= di_plus:
            return False, adx_value, f"ADX {adx_value} OK but DI- {di_minus} <= DI+ {di_plus}"
        adx_score = 20 if adx_value >= 35 else 15
        return True, adx_score, f"ADX {adx_value} confirmed with trend alignment"

    @staticmethod
    def check_volume_filter(strategy, current_volume):
        if not strategy.for_option_buyer:
            return True, 0, "Volume check skipped (not option buyer mode)"
        if current_volume is None or current_volume <= 0:
            return False, 0, "No volume data"
        confirmed, ratio, reason = strategy.volume_detector.is_breakout_confirmed(
            volume=current_volume,
            for_option_buyer=True
        )
        if ratio >= 2.5:
            score = 25
        elif ratio >= 1.8:
            score = 20
        elif ratio >= 1.5:
            score = 15
        else:
            score = 0
        if not confirmed:
            return False, score, f"Volume weak: {reason}"
        return True, score, f"Volume confirmed: {reason}"

    @staticmethod
    def check_session_filter(strategy, timestamp=None):
        if not strategy.for_option_buyer:
            return True, 0, "Session check skipped (not option buyer mode)"
        is_good, score, reason = strategy.session_rules.is_tradable(timestamp)
        if not is_good:
            return False, score, f"Bad session: {reason}"
        return True, score, f"Good session: {reason}"

    @staticmethod
    def check_oi_filter(strategy, oi_data, price, signal):
        if not strategy.for_option_buyer:
            return True, 0, "OI check skipped (not option buyer mode)"
        if oi_data is None:
            return False, 0, "No OI data"
        current_oi = oi_data.get('current_oi', oi_data.get('oi', 0))
        strategy.oi_analyzer.update(current_oi, price)
        confirmed, score, reason = strategy.oi_analyzer.confirm_signal(signal)
        if not confirmed:
            return False, score, f"OI not confirming: {reason}"
        return True, score, f"OI confirming: {reason}"

    @staticmethod
    def check_multi_timeframe_filter(strategy, trend_15m, signal):
        if not strategy.for_option_buyer:
            return True, 0, "Multi-TF check skipped (not option buyer mode)"
        if trend_15m in [None, "NEUTRAL", "UNKNOWN", "INSUFFICIENT_DATA"]:
            return True, 0, "15m trend unavailable"
        strategy.multi_tf_trend.update_trends("BULLISH" if signal == "CE" else "BEARISH", trend_15m)
        aligned, strength, reason = strategy.multi_tf_trend.check_alignment(signal)
        if not aligned:
            return False, strength, f"15m against: {reason}"
        return True, strength, f"Timeframes aligned: {reason}"

    @staticmethod
    def apply_option_buyer_filters(strategy, signal, candles_5m, current_volume, oi_data, price, trend_15m=None, timestamp=None):
        if not strategy.for_option_buyer:
            return True, 0, []
        blockers = []
        total_score = 0
        session_ok, session_score, session_reason = OptionBuyerProtection.check_session_filter(strategy, timestamp)
        if not session_ok:
            blockers.append(f"SESSION: {session_reason}")
        else:
            total_score += session_score
        adx_ok, adx_score, adx_reason = OptionBuyerProtection.check_adx_filter(strategy, candles_5m, signal)
        if not adx_ok:
            blockers.append(f"ADX: {adx_reason}")
        else:
            total_score += adx_score
        vol_ok, vol_score, vol_reason = OptionBuyerProtection.check_volume_filter(strategy, current_volume)
        if not vol_ok:
            blockers.append(f"VOLUME: {vol_reason}")
        else:
            total_score += vol_score
        oi_ok, oi_score, oi_reason = OptionBuyerProtection.check_oi_filter(strategy, oi_data, price, signal)
        if not oi_ok:
            blockers.append(f"OI: {oi_reason}")
        else:
            total_score += oi_score
        tf_ok, tf_score, tf_reason = OptionBuyerProtection.check_multi_timeframe_filter(strategy, trend_15m, signal)
        if not tf_ok:
            blockers.append(f"TIMEFRAME: {tf_reason}")
        else:
            total_score += tf_score
        return len(blockers) == 0, total_score, blockers


def check_adx_filter(strategy, candles_5m, signal):
    return OptionBuyerProtection.check_adx_filter(strategy, candles_5m, signal)


def check_volume_filter(strategy, current_volume):
    return OptionBuyerProtection.check_volume_filter(strategy, current_volume)


def check_session_filter(strategy, timestamp=None):
    return OptionBuyerProtection.check_session_filter(strategy, timestamp)


def check_oi_filter(strategy, oi_data, price, signal):
    return OptionBuyerProtection.check_oi_filter(strategy, oi_data, price, signal)


def check_multi_timeframe_filter(strategy, trend_15m, signal):
    return OptionBuyerProtection.check_multi_timeframe_filter(strategy, trend_15m, signal)


def apply_option_buyer_filters(strategy, signal, candles_5m, current_volume, oi_data, price, trend_15m=None, timestamp=None):
    return OptionBuyerProtection.apply_option_buyer_filters(strategy, signal, candles_5m, current_volume, oi_data, price, trend_15m=trend_15m, timestamp=timestamp)
