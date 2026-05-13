from shared.market.pressure_analyzer import PressureAnalyzer


def _row(strike, distance, option_type, oi, volume, ltp, previous_oi, previous_volume):
    return {
        "strike": strike,
        "distance_from_atm": distance,
        "option_type": option_type,
        "oi": oi,
        "volume": volume,
        "ltp": ltp,
        "previous_oi": previous_oi,
        "previous_volume": previous_volume,
    }


def test_pressure_analyzer_reads_bearish_put_flow_as_bearish():
    analyzer = PressureAnalyzer()
    first = {
        "atm": 24000,
        "underlying_price": 24010,
        "band_snapshots": [
            _row(24000, 0, "CE", 100000, 150000, 120.0, 98000, 140000),
            _row(24000, 0, "PE", 110000, 160000, 118.0, 108000, 150000),
        ],
    }
    analyzer.analyze(first, underlying_price=24010, oi_ladder_data={"trend": "NEUTRAL", "build_up": None})

    second = {
        "atm": 24000,
        "underlying_price": 23972,
        "band_snapshots": [
            _row(24000, 0, "CE", 101000, 152000, 102.0, 100000, 150000),
            _row(24000, 0, "PE", 118500, 182000, 142.0, 110000, 160000),
        ],
    }
    metrics = analyzer.analyze(second, underlying_price=23972, oi_ladder_data={"trend": "BEARISH", "build_up": "SHORT_BUILDUP"})

    assert metrics["pressure_bias"] == "BEARISH"
    assert metrics["bearish_pressure_score"] > metrics["bullish_pressure_score"]
    assert "pe_flow_active" in metrics["flow_notes"]


def test_pressure_analyzer_reads_bullish_call_flow_as_bullish():
    analyzer = PressureAnalyzer()
    first = {
        "atm": 24000,
        "underlying_price": 23960,
        "band_snapshots": [
            _row(24000, 0, "CE", 100000, 150000, 95.0, 98000, 140000),
            _row(24000, 0, "PE", 110000, 160000, 125.0, 108000, 150000),
        ],
    }
    analyzer.analyze(first, underlying_price=23960, oi_ladder_data={"trend": "NEUTRAL", "build_up": None})

    second = {
        "atm": 24000,
        "underlying_price": 24008,
        "band_snapshots": [
            _row(24000, 0, "CE", 98500, 178000, 124.0, 100000, 150000),
            _row(24000, 0, "PE", 109000, 155000, 102.0, 110000, 160000),
        ],
    }
    metrics = analyzer.analyze(second, underlying_price=24008, oi_ladder_data={"trend": "BULLISH", "build_up": "SHORT_COVERING"})

    assert metrics["pressure_bias"] == "BULLISH"
    assert metrics["bullish_pressure_score"] > metrics["bearish_pressure_score"]
    assert "ce_flow_active" in metrics["flow_notes"]


def test_pressure_ratio_only_acts_as_tiebreak_not_primary_vote():
    analyzer = PressureAnalyzer()
    first = {
        "atm": 24000,
        "underlying_price": 24010,
        "band_snapshots": [
            _row(24000, 0, "CE", 100000, 150000, 118.0, 98000, 140000),
            _row(24000, 0, "PE", 120000, 160000, 121.0, 118000, 150000),
        ],
    }
    analyzer.analyze(first, underlying_price=24010, oi_ladder_data={"trend": "NEUTRAL", "build_up": None})

    second = {
        "atm": 24000,
        "underlying_price": 23972,
        "band_snapshots": [
            _row(24000, 0, "CE", 122000, 212000, 99.0, 100000, 150000),
            _row(24000, 0, "PE", 121000, 120000, 140.0, 120000, 160000),
        ],
    }
    metrics = analyzer.analyze(second, underlying_price=23972, oi_ladder_data={"trend": "BEARISH", "build_up": "SHORT_BUILDUP"})

    assert metrics["ratio_bias_hint"] == "CALL_HEAVY"
    assert "call_ratio_tiebreak" in metrics["flow_notes"]
    assert metrics["pressure_bias"] == "BEARISH"
