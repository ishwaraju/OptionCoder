from shared.utils.notifier import Notifier


def test_monitor_notification_includes_strike_for_tracked_contract():
    notifier = Notifier()
    notifier.enabled = True
    captured = []
    notifier.send_alert = lambda message: captured.append(message)

    notifier.send_trade_monitor_update(
        {
            "instrument": "NIFTY",
            "signal": "CE",
            "strike": 24250,
            "guidance": "HOLD_STRONG",
            "decision_label": "HOLD_STRONG",
            "signal_type": "BREAKOUT_CONFIRM",
            "setup_bucket": "BREAKOUT",
            "pnl_points": 18.1,
            "pnl_percent": 9.21,
            "option_price": 214.55,
            "price": 24365.3,
            "entry_price": 196.45,
            "psar_style_level": 24354.91,
            "action_text": "Abhi hold karo. Momentum strong hai.",
        }
    )

    assert captured
    message = captured[0]
    assert "24250 CE" in message
    assert "Premium Entry 196.45" in message
    assert "LET WINNER RUN" in message
