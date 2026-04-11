"""
Notifier Module
Handles notifications and alerts
"""

from config import Config
import requests

class Notifier:
    """Notification handler"""
    
    def __init__(self):
        """Initialize notifier"""
        self.enabled = Config.ENABLE_ALERTS
        self.telegram_enabled = (
            self.enabled
            and Config.TELEGRAM_ENABLED
            and bool(Config.TELEGRAM_BOT_TOKEN)
            and bool(Config.TELEGRAM_CHAT_ID)
        )

    def _send_telegram(self, message):
        if not self.telegram_enabled:
            return

        try:
            requests.post(
                f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": Config.TELEGRAM_CHAT_ID,
                    "text": message,
                },
                timeout=5,
            )
        except Exception as e:
            print(f"[TELEGRAM ERROR] {e}")
    
    def send_alert(self, message):
        """Send alert notification"""
        if not self.enabled:
            return
        if Config.ENABLE_SOUND_ALERT:
            print("\a", end="")
        print(f"[ALERT] {message}")
        self._send_telegram(message)
    
    def send_trade_notification(self, trade_data):
        """Send trade notification"""
        if not self.enabled:
            return
        signal = trade_data.get("signal")
        strike = trade_data.get("strike")
        confidence = trade_data.get("confidence")
        signal_type = trade_data.get("signal_type")
        grade = trade_data.get("signal_grade")
        instrument = trade_data.get("instrument")
        prefix = f"{instrument} | " if instrument else ""
        message = f"{prefix}TRADE NOW {signal} | strike={strike} | confidence={confidence}"
        if signal_type:
            message += f" | type={signal_type}"
        if grade:
            message += f" | grade={grade}"
        self.send_alert(message)

    def send_entry_trigger_notification(self, trigger_data):
        """Send immediate 1-minute entry trigger notification"""
        if not self.enabled:
            return

        signal = trigger_data.get("signal")
        strike = trigger_data.get("strike")
        confidence = trigger_data.get("confidence")
        signal_type = trigger_data.get("signal_type")
        signal_grade = trigger_data.get("signal_grade")
        price = trigger_data.get("price")
        trigger_price = trigger_data.get("trigger_price")

        instrument = trigger_data.get("instrument")
        prefix = f"{instrument} | " if instrument else ""
        message = f"{prefix}1M ENTRY TRIGGER {signal} | strike={strike} | price={price} | confidence={confidence}"
        if signal_type:
            message += f" | type={signal_type}"
        if signal_grade:
            message += f" | grade={signal_grade}"
        if trigger_price is not None:
            message += f" | trigger={trigger_price}"

        self.send_alert(message)
    
    def send_error_alert(self, error_message):
        """Send error alert"""
        if not self.enabled:
            return
        self.send_alert(f"ERROR: {error_message}")
    
    def send_trade_monitor_update(self, monitor_data):
        """Send manual trade monitor guidance update."""
        if not self.enabled:
            return

        instrument = monitor_data.get("instrument")
        signal = monitor_data.get("signal")
        price = monitor_data.get("price")
        entry_price = monitor_data.get("entry_price")
        guidance = monitor_data.get("guidance")
        time_regime = monitor_data.get("time_regime")
        quality = monitor_data.get("quality")
        reason = monitor_data.get("reason")
        structure = monitor_data.get("structure")
        pnl_points = monitor_data.get("pnl_points")
        structure_short = structure or ""
        prefix = f"{instrument} | " if instrument else ""
        message = f"{prefix}{signal} | {guidance}"
        if pnl_points is not None:
            message += f" | {pnl_points:+.2f}pts"
        if structure_short:
            message += f" | {structure_short}"
        if time_regime:
            message += f" | {time_regime}"
        if quality:
            message += f" | Q{quality}"
        if reason:
            message += f"\n{reason}"

        self.send_alert(message)
