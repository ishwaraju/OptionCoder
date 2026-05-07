from datetime import date, datetime

from tools.analyze_watch_conversion import (
    EntryEvent,
    WatchEvent,
    analyze_conversion,
    infer_watch_failure_reason,
    parse_alert_blocks,
    render_table,
)


def test_parse_alert_blocks_extracts_watch_and_entry(tmp_path):
    log_file = tmp_path / "signal_service_nifty.log"
    log_file.write_text(
        "\n".join(
            [
                "[10:32:43] [Signal Service] something",
                "[10:32:43] [ALERT] NIFTY PE watch below 24365.25",
                "IST 10:32 | Breakout Confirm | SPIKE_EARLY | Watch Pe Spike | S:78 | E:72 | G:B",
                "WATCH_PE_SPIKE | PE 1m spike | breadth 10/11 | vol 6/11 | 1m vol x1.83",
                "",
                "[10:54:50] [ALERT] NIFTY Confirmed PE Entry",
                "IST 10:54 | Breakout Confirm | HIGH | G:A | Contract 24400 PE",
                "",
            ]
        ),
        encoding="utf-8",
    )

    watches, entries = parse_alert_blocks(log_file, date(2026, 5, 7))

    assert len(watches) == 1
    assert watches[0].instrument == "NIFTY"
    assert watches[0].direction == "PE"
    assert watches[0].score == 78
    assert watches[0].entry_score == 72
    assert watches[0].grade == "B"
    assert watches[0].breadth == 10
    assert watches[0].volume_breadth == 6
    assert len(entries) == 1
    assert entries[0].direction == "PE"


def test_analyze_conversion_maps_entry_to_watch():
    watch = WatchEvent(
        instrument="NIFTY",
        direction="PE",
        ts=datetime(2026, 5, 7, 10, 52),
        header="NIFTY PE watch below 24344.25",
        lines=[],
        score=86,
        entry_score=80,
        grade="A",
        breadth=11,
        breadth_total=11,
        volume_breadth=11,
        volume_total=11,
    )
    entry = EntryEvent(
        instrument="NIFTY",
        direction="PE",
        ts=datetime(2026, 5, 7, 10, 54),
        header="NIFTY Confirmed PE Entry",
        lines=[],
    )

    results = analyze_conversion([watch], [entry], window_minutes=30)

    assert len(results) == 1
    assert results[0]["converted"] is True
    assert results[0]["latency_minutes"] == 2.0


def test_infer_watch_failure_reason_prefers_replacement_then_breadth():
    watch = WatchEvent(
        instrument="BANKNIFTY",
        direction="CE",
        ts=datetime(2026, 5, 7, 10, 38),
        header="BANKNIFTY CE watch above 56041.15",
        lines=["structure limited"],
        score=78,
        entry_score=72,
        grade="B",
        breadth=7,
        breadth_total=11,
        volume_breadth=11,
        volume_total=11,
    )
    next_same = WatchEvent(
        instrument="BANKNIFTY",
        direction="CE",
        ts=datetime(2026, 5, 7, 10, 49),
        header="BANKNIFTY CE watch above 56091.15",
        lines=[],
        score=86,
        entry_score=80,
        grade="A",
        breadth=10,
        breadth_total=11,
        volume_breadth=10,
        volume_total=11,
    )

    reason = infer_watch_failure_reason(
        watch,
        next_same_direction_watch=next_same,
        next_opposite_watch=None,
        end_of_day=datetime(2026, 5, 7, 15, 30),
    )

    assert reason == "replaced_by_fresher_watch"


def test_render_table_aligns_columns():
    table = render_table(
        ["Instrument", "Signal", "Points"],
        [["NIFTY", "CE", "24.05"], ["BANKNIFTY", "PE", "62.35"]],
    )

    assert "Instrument | Signal | Points" in table
    assert "NIFTY      | CE     | 24.05 " in table


def test_render_table_supports_trade_detail_shape():
    table = render_table(
        ["Instrument", "Signal", "Entry", "Strike", "AlertPrem", "MonitorStart", "PeakPrem", "PeakPts", "Exit", "ExitPrem", "ExitPts", "ExitWhy"],
        [["NIFTY", "CE", "12:02", "24300", "212.25", "11:57", "220.50", "8.25", "12:13", "220.50", "8.25", "EXIT_PROFIT_PROTECT"]],
    )

    assert "AlertPrem" in table
    assert "MonitorStart" in table
    assert "EXIT_PROFIT_PROTECT" in table
