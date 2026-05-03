import math
from datetime import date, datetime, time


def _norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _coerce_expiry_date(expiry_value):
    if expiry_value is None:
        return None
    if isinstance(expiry_value, date) and not isinstance(expiry_value, datetime):
        return expiry_value
    if isinstance(expiry_value, datetime):
        return expiry_value.date()
    try:
        return date.fromisoformat(str(expiry_value))
    except Exception:
        return None


def time_to_expiry_years(current_dt, expiry_value, market_close=time(15, 30)):
    expiry_date = _coerce_expiry_date(expiry_value)
    if current_dt is None or expiry_date is None:
        return None

    expiry_dt = datetime.combine(expiry_date, market_close)
    if current_dt >= expiry_dt:
        return 1.0 / (365.0 * 24.0 * 60.0)

    seconds = max((expiry_dt - current_dt).total_seconds(), 60.0)
    return seconds / (365.0 * 24.0 * 60.0 * 60.0)


def calculate_black_scholes_greeks(
    underlying_price,
    strike,
    option_type,
    implied_volatility,
    time_to_expiry,
    risk_free_rate=0.06,
):
    try:
        spot = float(underlying_price)
        strike = float(strike)
        iv = float(implied_volatility)
        years = float(time_to_expiry)
    except Exception:
        return None

    if spot <= 0 or strike <= 0 or iv <= 0 or years <= 0:
        return None

    sigma = iv / 100.0 if iv > 1 else iv
    if sigma <= 0:
        return None

    sqrt_t = math.sqrt(years)
    d1 = (math.log(spot / strike) + (risk_free_rate + 0.5 * sigma * sigma) * years) / (sigma * sqrt_t)
    d2 = d1 - (sigma * sqrt_t)
    pdf_d1 = _norm_pdf(d1)

    if option_type == "CE":
        delta = _norm_cdf(d1)
        theoretical_price = (spot * _norm_cdf(d1)) - (strike * math.exp(-risk_free_rate * years) * _norm_cdf(d2))
        theta = (
            -(spot * pdf_d1 * sigma) / (2.0 * sqrt_t)
            - risk_free_rate * strike * math.exp(-risk_free_rate * years) * _norm_cdf(d2)
        ) / 365.0
    elif option_type == "PE":
        delta = _norm_cdf(d1) - 1.0
        theoretical_price = (strike * math.exp(-risk_free_rate * years) * _norm_cdf(-d2)) - (spot * _norm_cdf(-d1))
        theta = (
            -(spot * pdf_d1 * sigma) / (2.0 * sqrt_t)
            + risk_free_rate * strike * math.exp(-risk_free_rate * years) * _norm_cdf(-d2)
        ) / 365.0
    else:
        return None

    gamma = pdf_d1 / (spot * sigma * sqrt_t)
    vega = (spot * pdf_d1 * sqrt_t) / 100.0

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 4),
        "theta": round(theta, 4),
        "vega": round(vega, 4),
        "iv": round(iv, 2 if iv > 1 else 4),
        "theoretical_price": round(theoretical_price, 2),
        "source": "CALCULATED",
    }


def project_option_price(
    underlying_price,
    strike,
    option_type,
    implied_volatility,
    current_dt,
    expiry_value,
    risk_free_rate=0.06,
):
    years = time_to_expiry_years(current_dt, expiry_value)
    greeks = calculate_black_scholes_greeks(
        underlying_price=underlying_price,
        strike=strike,
        option_type=option_type,
        implied_volatility=implied_volatility,
        time_to_expiry=years,
        risk_free_rate=risk_free_rate,
    )
    if not greeks:
        return None
    return greeks.get("theoretical_price")


def enrich_contract_greeks(option_contract, underlying_price, option_type, current_dt, expiry_value, risk_free_rate=0.06):
    if not option_contract:
        return None

    enriched = dict(option_contract)
    existing_delta = enriched.get("delta")
    existing_gamma = enriched.get("gamma")
    existing_theta = enriched.get("theta")
    existing_vega = enriched.get("vega")
    existing_iv = enriched.get("iv")

    if all(value is not None for value in [existing_delta, existing_gamma, existing_theta, existing_vega]):
        enriched["greeks_source"] = "LIVE"
        return enriched

    years = time_to_expiry_years(current_dt, expiry_value)
    calculated = calculate_black_scholes_greeks(
        underlying_price=underlying_price,
        strike=enriched.get("strike"),
        option_type=option_type,
        implied_volatility=existing_iv,
        time_to_expiry=years,
        risk_free_rate=risk_free_rate,
    )
    if not calculated:
        enriched["greeks_source"] = "UNAVAILABLE"
        return enriched

    enriched["delta"] = calculated["delta"]
    enriched["gamma"] = calculated["gamma"]
    enriched["theta"] = calculated["theta"]
    enriched["vega"] = calculated["vega"]
    enriched["iv"] = calculated["iv"]
    enriched["theoretical_price"] = calculated["theoretical_price"]
    enriched["greeks_source"] = calculated["source"]
    return enriched


def format_greek_summary(option_contract):
    if not option_contract:
        return None

    delta = option_contract.get("delta")
    gamma = option_contract.get("gamma")
    theta = option_contract.get("theta")
    vega = option_contract.get("vega")
    iv = option_contract.get("iv")
    source = option_contract.get("greeks_source")

    parts = []
    if delta is not None:
        parts.append(f"Delta {delta:.2f}")
    if gamma is not None:
        parts.append(f"Gamma {gamma:.3f}")
    if theta is not None:
        parts.append(f"Theta {theta:.2f}")
    if vega is not None:
        parts.append(f"Vega {vega:.2f}")
    if iv is not None:
        parts.append(f"IV {iv:.2f}%")
    if source and source != "LIVE":
        parts.append(source.title())

    if not parts:
        return None
    return "Greeks: " + " | ".join(parts)
