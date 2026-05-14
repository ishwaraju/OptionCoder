class SignalComponentScorer:
    @staticmethod
    def score_signal_components(
        price,
        orb_high,
        orb_low,
        vwap,
        volume_signal,
        oi_bias,
        oi_trend,
        build_up,
        pressure_metrics,
        buffer,
    ):
        if vwap is None:
            return {
                "price_structure_score": 0.0,
                "option_flow_score": 0.0,
                "oi_structure_score": 0.0,
                "contract_quality_score": 0.0,
                "bullish_total": 0.0,
                "bearish_total": 0.0,
                "direction": None,
                "components": ["VWAP unavailable"],
            }

        bullish_price = 0.0
        bearish_price = 0.0
        bullish_flow = 0.0
        bearish_flow = 0.0
        bullish_oi = 0.0
        bearish_oi = 0.0
        neutral_contract = 45.0
        components = []
        neutral_components = []

        if price > vwap:
            bullish_price += 22
            components.append("price_above_vwap")
        elif price < vwap:
            bearish_price += 22
            components.append("price_below_vwap")

        if orb_high is not None and price > orb_high + buffer:
            bullish_price += 20
            components.append("orb_breakout_up")
        elif orb_low is not None and price < orb_low - buffer:
            bearish_price += 20
            components.append("orb_breakout_down")

        if volume_signal == "STRONG":
            bullish_flow += 7
            bearish_flow += 7
            neutral_contract += 10
            neutral_components.append("strong_volume")
        elif volume_signal == "NORMAL":
            bullish_flow += 3
            bearish_flow += 3
            neutral_contract += 5
            neutral_components.append("normal_volume")

        if oi_bias == "BULLISH":
            bullish_oi += 12
            components.append("bullish_oi_bias")
        elif oi_bias == "BEARISH":
            bearish_oi += 12
            components.append("bearish_oi_bias")

        if oi_trend == "BULLISH":
            bullish_oi += 12
            components.append("bullish_oi_trend")
        elif oi_trend == "BEARISH":
            bearish_oi += 12
            components.append("bearish_oi_trend")

        if build_up in ["LONG_BUILDUP", "SHORT_COVERING"]:
            bullish_oi += 10
            components.append("bullish_build_up")
        elif build_up in ["SHORT_BUILDUP", "LONG_UNWINDING"]:
            bearish_oi += 10
            components.append("bearish_build_up")

        if pressure_metrics:
            if pressure_metrics["pressure_bias"] == "BULLISH":
                bullish_flow += 16
                components.append("bullish_pressure")
            elif pressure_metrics["pressure_bias"] == "BEARISH":
                bearish_flow += 16
                components.append("bearish_pressure")

            if pressure_metrics.get("atm_pe_concentration", 0) >= 0.2 and pressure_metrics.get("atm_pe_concentration", 0) > pressure_metrics.get("atm_ce_concentration", 0):
                bullish_flow += 4
                components.append("atm_pe_concentration")

            if pressure_metrics.get("atm_ce_concentration", 0) >= 0.2 and pressure_metrics.get("atm_ce_concentration", 0) > pressure_metrics.get("atm_pe_concentration", 0):
                bearish_flow += 4
                components.append("atm_ce_concentration")

            flow_bull = float(pressure_metrics.get("bullish_pressure_score") or 0.0)
            flow_bear = float(pressure_metrics.get("bearish_pressure_score") or 0.0)
            bullish_flow += min(flow_bull * 0.35, 12.0)
            bearish_flow += min(flow_bear * 0.35, 12.0)
            neutral_contract += 4 if pressure_metrics.get("flow_strength") == "STRONG" else 2 if pressure_metrics.get("flow_strength") == "MODERATE" else 0

        bullish_total = bullish_price + bullish_flow + bullish_oi
        bearish_total = bearish_price + bearish_flow + bearish_oi
        if bullish_total > bearish_total:
            direction = "CE"
        elif bearish_total > bullish_total:
            direction = "PE"
        else:
            direction = None
            components.append("balanced_directional_signals")

        price_structure_score = round(min(max(max(bullish_price, bearish_price), 0.0), 100.0), 2)
        option_flow_score = round(min(max(max(bullish_flow, bearish_flow), 0.0), 100.0), 2)
        oi_structure_score = round(min(max(max(bullish_oi, bearish_oi), 0.0), 100.0), 2)
        contract_quality_score = round(min(max(neutral_contract, 0.0), 100.0), 2)
        components.extend(neutral_components)
        return {
            "price_structure_score": price_structure_score,
            "option_flow_score": option_flow_score,
            "oi_structure_score": oi_structure_score,
            "contract_quality_score": contract_quality_score,
            "bullish_total": round(bullish_total, 2),
            "bearish_total": round(bearish_total, 2),
            "direction": direction,
            "components": components,
        }


def score_signal_components(
    price,
    orb_high,
    orb_low,
    vwap,
    volume_signal,
    oi_bias,
    oi_trend,
    build_up,
    pressure_metrics,
    buffer,
):
    return SignalComponentScorer.score_signal_components(
        price,
        orb_high,
        orb_low,
        vwap,
        volume_signal,
        oi_bias,
        oi_trend,
        build_up,
        pressure_metrics,
        buffer,
    )
