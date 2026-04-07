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
        message = f"TRADE NOW {signal} | strike={strike} | confidence={confidence}"
        self.send_alert(message)
    
    def send_error_alert(self, error_message):
        """Send error alert"""
        if not self.enabled:
            return
        self.send_alert(f"ERROR: {error_message}")
