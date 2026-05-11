from shared.utils.notifier import Notifier


def test_entry_trigger_notification_includes_spot_and_risk_plan():
    notifier = Notifier()
    notifier.enabled = True
    captured = []
    notifier.send_alert = lambda message: captured.append(message)

    notifier.send_entry_trigger_notification(
        {
            "instrument": "NIFTY",
            "signal": "CE",
            "strike": 24250,
            "confidence": "HIGH",
            "confidence_summary": "Spike watch microstructure override",
            "signal_type": "BREAKOUT_CONFIRM",
            "signal_grade": "A",
            "price": 188.4,
            "underlying_price": 24242.75,
            "trigger_price": 24237.15,
            "invalidate_price": 24211.5,
            "first_target_price": 24288.45,
            "stop_loss_option_price": 145.07,
            "first_target_option_price": 259.99,
            "option_stop_loss_pct": 23,
            "option_target_pct": 38,
            "entry_bid": 187.9,
            "entry_ask": 188.4,
            "entry_spread": 0.5,
            "context": "SpikeWatch | 15m UP | 5m ready | S 24200 | R 24300",
            "risk_note": "1m spike watch tha; follow-through ke baad hi entry confirm hui.",
            "decision_label": "CONFIRMED_CE_ENTRY",
            "reason": "1m trigger confirmed after 5m watch | Spike watch microstructure override",
            "greek_summary": "Delta 0.42 | Theta -8.2",
        }
    )

    assert captured
    message = captured[0]
    assert "Confirmed CE Entry" in message
    assert "Spot 24242.75" in message
    assert "Trig 24237.15" in message
    assert "SL 24211.5" in message
    assert "T1 24288.45" in message
    assert "BUY 24250 CE near 188.40 (bid 187.90 / ask 188.40). Chase mat karo above 188.40." in message
    assert "OptSL 145.07 (-23%)" in message
    assert "OptT1 259.99 (+38%)" in message


def test_trade_notification_includes_actual_premium_levels_and_percentages():
    notifier = Notifier()
    notifier.enabled = True
    captured = []
    notifier.send_alert = lambda message: captured.append(message)

    notifier.send_trade_notification(
        {
            "instrument": "NIFTY",
            "signal": "PE",
            "strike": 23950,
            "confidence": "MEDIUM",
            "signal_type": "BREAKOUT_CONFIRM",
            "signal_grade": "B",
            "price": 111.6,
            "spot_price": 23895.2,
            "trigger_price": 23900,
            "invalidate_price": 23945,
            "first_target_price": 23840,
            "projected_premium_sl": 98.21,
            "projected_premium_t1": 136.15,
            "option_stop_loss_pct": 12,
            "option_target_pct": 22,
            "entry_bid": 111.1,
            "entry_ask": 111.6,
            "entry_spread": 0.5,
            "decision_label": "CONFIRMED_PE_ENTRY",
        }
    )

    assert captured
    message = captured[0]
    assert "BUY 23950 PE near 111.60" in message
    assert "OptSL 98.21 (-12%)" in message
    assert "OptT1 136.15 (+22%)" in message
