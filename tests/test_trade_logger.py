from utils.logger import TradeLogger


def test_logger_uses_ist_timestamp_for_summary_file():
    logger = TradeLogger()

    assert logger.summary_file.startswith("data/session_summary_")
    assert len(logger.summary_file) == len("data/session_summary_YYYYMMDD.txt")
