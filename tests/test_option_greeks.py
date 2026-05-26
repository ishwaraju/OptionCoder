from datetime import datetime
from zoneinfo import ZoneInfo

from shared.utils.option_greeks import project_option_price, time_to_expiry_years


def test_time_to_expiry_accepts_timezone_aware_current_datetime():
    current_dt = datetime(2026, 5, 22, 12, 51, tzinfo=ZoneInfo("Asia/Kolkata"))

    years = time_to_expiry_years(current_dt, "2026-05-22")

    assert years is not None
    assert years > 0


def test_project_option_price_accepts_timezone_aware_current_datetime():
    current_dt = datetime(2026, 5, 22, 12, 51, tzinfo=ZoneInfo("Asia/Kolkata"))

    projected = project_option_price(
        underlying_price=75700,
        strike=75600,
        option_type="CE",
        implied_volatility=14.5,
        current_dt=current_dt,
        expiry_value="2026-05-22",
    )

    assert projected is not None
    assert projected > 0
