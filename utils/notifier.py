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
        message = f"TRADE NOW {signal} | strike={strike} | confidence={confidence}"
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

        message = f"1M ENTRY TRIGGER {signal} | strike={strike} | price={price} | confidence={confidence}"
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
    
    def send_strategy_decision(self, decision_data):
        """Send strategy decision notification"""
        if not self.enabled:
            return
        
        signal = decision_data.get("signal", "NO_TRADE")
        score = decision_data.get("score")
        confidence = decision_data.get("confidence")
        regime = decision_data.get("regime")
        price = decision_data.get("price")
        reason = decision_data.get("reason")
        manual_guidance = decision_data.get("manual_guidance")
        blockers = decision_data.get("blockers")
        cautions = decision_data.get("cautions")
        
        message = f"STRATEGY DECISION\n"
        message += f"Signal: {signal}\n"
        message += f"Price: {price}\n"
        message += f"Score: {score}\n"
        message += f"Confidence: {confidence}\n"
        message += f"Regime: {regime}\n"
        message += f"Reason: {reason}\n"
        message += f"Guidance: {manual_guidance}"
        
        if blockers:
            message += f"\nBlockers: {blockers}"
        if cautions:
            message += f"\nCautions: {cautions}"
            
        self.send_alert(message)
