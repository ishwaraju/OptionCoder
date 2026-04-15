"""
Session-Based Trading Rules
For option buyers: Avoid high-risk periods, trade during optimal windows
"""
from shared.utils.time_utils import TimeUtils
from datetime import datetime, time as dt_time


class SessionRules:
    """
    Define optimal trading sessions for option buyers
    Avoid: Opening volatility, closing noise, lunch period
    Target: Main session when trend is established
    """

    # Market session times (IST)
    MARKET_OPEN = dt_time(9, 15)
    MORNING_VOLATILITY_END = dt_time(9, 45)  # High volatility period
    OPTIMAL_START = dt_time(10, 0)  # When trend establishes
    LUNCH_START = dt_time(12, 30)  # Low volatility
    LUNCH_END = dt_time(13, 30)  # Resume
    AFTERNOON_START = dt_time(13, 30)
    CHOPPY_START = dt_time(14, 30)  # Choppy period starts
    MARKET_CLOSE = dt_time(15, 30)

    # Session quality scores
    SESSION_SCORES = {
        'OPENING_VOLATILITY': 30,  # Avoid - high volatility, premiums expensive
        'MORNING_SESSION': 85,  # Best - trend established
        'LUNCH': 40,  # Avoid - low volume, false breakouts
        'AFTERNOON_SESSION': 75,  # Good - momentum continues
        'CHOPPY': 35,  # Avoid - direction unclear
        'CLOSING': 50,  # Mixed - can have sharp moves
    }

    # Minimum scores for option buyers
    OPTION_BUYER_MIN_SCORE = 70  # Strict threshold
    NORMAL_MIN_SCORE = 50  # Normal threshold

    def __init__(self, for_option_buyer=True):
        self.time_utils = TimeUtils()
        self.for_option_buyer = for_option_buyer
        self.min_score = self.OPTION_BUYER_MIN_SCORE if for_option_buyer else self.NORMAL_MIN_SCORE

    def get_current_session(self, timestamp=None):
        """Get current market session"""
        if timestamp is None:
            timestamp = self.time_utils.now_ist()

        current_time = timestamp.time() if isinstance(timestamp, datetime) else timestamp

        if self.MARKET_OPEN <= current_time < self.MORNING_VOLATILITY_END:
            return 'OPENING_VOLATILITY'
        elif self.MORNING_VOLATILITY_END <= current_time < self.LUNCH_START:
            return 'MORNING_SESSION'
        elif self.LUNCH_START <= current_time < self.LUNCH_END:
            return 'LUNCH'
        elif self.LUNCH_END <= current_time < self.CHOPPY_START:
            return 'AFTERNOON_SESSION'
        elif self.CHOPPY_START <= current_time < self.MARKET_CLOSE:
            return 'CHOPPY'
        else:
            return 'CLOSED'

    def get_session_score(self, timestamp=None):
        """Get quality score for current session"""
        session = self.get_current_session(timestamp)
        return self.SESSION_SCORES.get(session, 0)

    def is_tradable(self, timestamp=None):
        """Check if current time is good for trading"""
        session = self.get_current_session(timestamp)
        score = self.get_session_score(timestamp)

        is_good = score >= self.min_score

        reason = f"{session}: Score {score}"
        if is_good:
            reason += f" >= {self.min_score} ✅"
        else:
            reason += f" < {self.min_score} ❌"

        return is_good, score, reason

    def get_recommended_action(self, timestamp=None):
        """Get recommended action for current time"""
        session = self.get_current_session(timestamp)
        score = self.get_session_score(timestamp)

        recommendations = {
            'OPENING_VOLATILITY': {
                'action': 'WAIT',
                'message': 'Opening volatility - wait 15-30 min for direction',
                'enter_after': '10:00 AM'
            },
            'MORNING_SESSION': {
                'action': 'TRADE',
                'message': 'Optimal session - trends established, good for options',
                'best_until': '12:30 PM'
            },
            'LUNCH': {
                'action': 'AVOID',
                'message': 'Lunch session - low volume, avoid option buying',
                'resume_after': '13:30 PM'
            },
            'AFTERNOON_SESSION': {
                'action': 'TRADE',
                'message': 'Good session - momentum often continues',
                'watch_until': '14:30 PM'
            },
            'CHOPPY': {
                'action': 'AVOID',
                'message': 'Choppy period - direction unclear, high theta loss risk',
                'stop_trading': True
            },
            'CLOSED': {
                'action': 'CLOSED',
                'message': 'Market closed',
                'next_session': '09:15 AM'
            }
        }

        return {
            'session': session,
            'score': score,
            **recommendations.get(session, {'action': 'UNKNOWN', 'message': 'Unknown session'})
        }

    def get_best_sessions(self):
        """Get list of best trading sessions"""
        return [
            ('10:00-12:30', 'MORNING_SESSION', 85),
            ('13:30-14:30', 'AFTERNOON_SESSION', 75)
        ]

    def get_avoid_sessions(self):
        """Get list of sessions to avoid"""
        return [
            ('09:15-09:45', 'OPENING_VOLATILITY', 30),
            ('12:30-13:30', 'LUNCH', 40),
            ('14:30-15:30', 'CHOPPY', 35)
        ]

    def get_adjusted_min_score(self, timestamp=None):
        """Get adjusted minimum score based on time"""
        base_score = 60
        session = self.get_current_session(timestamp)

        adjustments = {
            'OPENING_VOLATILITY': +10,  # Be stricter
            'MORNING_SESSION': 0,
            'LUNCH': +15,  # Much stricter
            'AFTERNOON_SESSION': 0,
            'CHOPPY': +20,  # Very strict
        }

        adjustment = adjustments.get(session, 0)
        return base_score + adjustment

    def is_expiry_day_adjustment_needed(self, is_expiry=False):
        """Get adjustments for expiry day"""
        if not is_expiry:
            return None

        return {
            'avoid_afternoon': True,
            'stop_trading_by': '14:00',
            'reason': 'Expiry day - high volatility, close positions by 2 PM'
        }

    def get_time_based_cooldown(self, timestamp=None):
        """Get recommended cooldown between signals based on time"""
        session = self.get_current_session(timestamp)

        cooldowns = {
            'OPENING_VOLATILITY': 600,  # 10 min - let volatility settle
            'MORNING_SESSION': 300,  # 5 min - normal
            'LUNCH': 900,  # 15 min - avoid overtrading in low volume
            'AFTERNOON_SESSION': 300,  # 5 min
            'CHOPPY': 1200,  # 20 min - avoid chop
        }

        return cooldowns.get(session, 300)

    def format_time_recommendation(self, timestamp=None):
        """Format time recommendation for display"""
        rec = self.get_recommended_action(timestamp)

        lines = [
            f"⏰ Current Session: {rec['session']}",
            f"📊 Quality Score: {rec['score']}/100",
            f"🎯 Action: {rec['action']}",
            f"💡 {rec['message']}"
        ]

        if 'enter_after' in rec:
            lines.append(f"   ⏳ Wait until: {rec['enter_after']}")
        if 'stop_trading' in rec:
            lines.append(f"   🚫 Stop trading for today")

        return '\n'.join(lines)


def is_optimal_trading_time(timestamp=None, for_option_buyer=True):
    """
    Quick check if current time is optimal for trading
    Returns: (is_optimal, score, recommendation)
    """
    rules = SessionRules(for_option_buyer=for_option_buyer)
    is_good, score, reason = rules.is_tradable(timestamp)

    rec = rules.get_recommended_action(timestamp)

    return is_good, score, rec['message']


def get_session_info(timestamp=None):
    """Get full session information"""
    rules = SessionRules(for_option_buyer=True)
    return {
        'session': rules.get_current_session(timestamp),
        'score': rules.get_session_score(timestamp),
        'tradable': rules.is_tradable(timestamp),
        'recommendation': rules.get_recommended_action(timestamp),
        'best_sessions': rules.get_best_sessions(),
        'avoid_sessions': rules.get_avoid_sessions()
    }


if __name__ == "__main__":
    print("="*60)
    print("⏰ Session-Based Trading Rules Test")
    print("="*60)

    rules = SessionRules(for_option_buyer=True)

    # Test different times
    test_times = [
        dt_time(9, 20),  # Opening
        dt_time(10, 30),  # Morning
        dt_time(12, 45),  # Lunch
        dt_time(13, 45),  # Afternoon
        dt_time(14, 45),  # Choppy
    ]

    print("\n📊 Option Buyer Session Analysis:")
    print("-" * 60)

    for test_time in test_times:
        session = rules.get_current_session(test_time)
        score = rules.get_session_score(test_time)
        is_good, _, reason = rules.is_tradable(test_time)

        status = "✅ TRADE" if is_good else "❌ AVOID"
        print(f"\n{test_time.strftime('%H:%M')} - {session}")
        print(f"   Score: {score} | Min Required: {rules.min_score}")
        print(f"   {status} - {reason}")

    print("\n" + "="*60)
    print("🎯 Best Trading Windows:")
    print("-" * 60)
    for window, name, score in rules.get_best_sessions():
        print(f"   ✅ {window}: {name} (Score: {score})")

    print("\n🚫 Sessions to AVOID:")
    print("-" * 60)
    for window, name, score in rules.get_avoid_sessions():
        print(f"   ❌ {window}: {name} (Score: {score})")

    print("\n" + "="*60)
    print("✅ Session Rules ready for option buyer protection!")
    print("="*60)


__all__ = [
    'SessionRules',
    'is_optimal_trading_time',
    'get_session_info'
]
