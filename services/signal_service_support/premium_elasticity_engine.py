class PremiumElasticityEngine:
    @staticmethod
    def evaluate(
        *,
        signal,
        underlying_price,
        trigger_price,
        premium_guard,
        selected_option_contract,
        futures_acceptance,
    ):
        signal = (signal or "").upper()
        premium_guard = premium_guard or {}
        momentum_pct = premium_guard.get("premium_momentum_pct")
        spread_pct = premium_guard.get("spread_pct")
        volume_supporting = bool(premium_guard.get("volume_supporting"))
        current_ltp = premium_guard.get("current_ltp")
        previous_ltp = premium_guard.get("previous_ltp")

        underlying_move = None
        if underlying_price is not None and trigger_price is not None:
            underlying_move = (float(underlying_price) - float(trigger_price))
            if signal == "PE":
                underlying_move *= -1.0

        premium_move = None
        if current_ltp is not None and previous_ltp is not None:
            premium_move = float(current_ltp) - float(previous_ltp)

        elasticity_ratio = None
        if underlying_move not in (None, 0) and premium_move is not None:
            elasticity_ratio = round(premium_move / max(abs(underlying_move), 0.01), 4)

        dead_premium_risk = False
        reasons = []
        if (
            underlying_move is not None
            and underlying_move >= 8.0
            and (momentum_pct is None or float(momentum_pct) < 0.5)
        ):
            dead_premium_risk = True
            reasons.append("premium_not_responding")
        if spread_pct is not None and float(spread_pct) >= 4.8:
            dead_premium_risk = True
            reasons.append("spread_too_wide")
        if (
            futures_acceptance
            and futures_acceptance.get("accepted")
            and underlying_move is not None
            and underlying_move >= 8.0
            and not volume_supporting
        ):
            dead_premium_risk = True
            reasons.append("volume_not_supporting")

        if dead_premium_risk:
            label = "DEAD_PREMIUM_RISK"
            score = 15.0
        else:
            base = 65.0 if volume_supporting else 52.0
            if momentum_pct is not None:
                base += min(max(float(momentum_pct), -1.0), 3.0) * 6.0
            if spread_pct is not None:
                base -= max(float(spread_pct) - 2.0, 0.0) * 4.0
            if elasticity_ratio is not None:
                base += min(max(elasticity_ratio, 0.0), 1.0) * 12.0
            label = "ELASTIC" if base >= 72 else "ADEQUATE" if base >= 58 else "DULL"
            score = max(min(base, 100.0), 0.0)

        return {
            "label": label,
            "score": round(score, 2),
            "elasticity_ratio": elasticity_ratio,
            "underlying_move": underlying_move,
            "premium_move": premium_move,
            "dead_premium_risk": dead_premium_risk,
            "reasons": reasons,
            "summary": (
                f"elasticity={label} | ratio={elasticity_ratio} | "
                f"underlying_move={round(underlying_move, 2) if underlying_move is not None else 'na'} | "
                f"premium_move={round(premium_move, 2) if premium_move is not None else 'na'}"
            ),
        }


def evaluate_premium_elasticity(**kwargs):
    return PremiumElasticityEngine.evaluate(**kwargs)
