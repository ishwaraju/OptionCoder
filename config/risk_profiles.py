"""
YAML-backed risk profile loader for option-buyer premium management.
"""

from pathlib import Path


REQUIRED_INSTRUMENTS = ("NIFTY", "BANKNIFTY", "SENSEX")
REQUIRED_SESSIONS = ("EXPIRY", "NON_EXPIRY", "PRE_EXPIRY", "POST_EXPIRY")
REQUIRED_SETUPS = ("BREAKOUT", "CONTINUATION", "REVERSAL")
REQUIRED_IV_BUCKETS = ("CHEAP", "NORMAL", "RICH", "EXTREME")
REQUIRED_FIELDS = ("sl", "target", "trail")


def _yaml_path():
    return Path(__file__).with_name("risk_profiles.yaml")


def _validate_profile_leaf(instrument, session_bucket, setup_bucket, iv_bucket, leaf):
    if not isinstance(leaf, dict):
        raise ValueError(f"{instrument}/{session_bucket}/{setup_bucket}/{iv_bucket} must be a mapping")
    for field in REQUIRED_FIELDS:
        if field not in leaf:
            raise ValueError(f"{instrument}/{session_bucket}/{setup_bucket}/{iv_bucket} missing '{field}'")
        value = leaf[field]
        if not isinstance(value, (int, float)):
            raise ValueError(f"{instrument}/{session_bucket}/{setup_bucket}/{iv_bucket}/{field} must be numeric")
    if float(leaf["target"]) <= float(leaf["sl"]):
        raise ValueError(f"{instrument}/{session_bucket}/{setup_bucket}/{iv_bucket} target must be > sl")


def _validate_risk_profile_matrix(matrix):
    if not isinstance(matrix, dict):
        raise ValueError("risk profile matrix must be a mapping")

    for instrument in REQUIRED_INSTRUMENTS:
        instrument_map = matrix.get(instrument)
        if not isinstance(instrument_map, dict):
            raise ValueError(f"missing instrument '{instrument}'")
        for session_bucket in REQUIRED_SESSIONS:
            session_map = instrument_map.get(session_bucket)
            if not isinstance(session_map, dict):
                raise ValueError(f"missing session '{instrument}/{session_bucket}'")
            for setup_bucket in REQUIRED_SETUPS:
                setup_map = session_map.get(setup_bucket)
                if not isinstance(setup_map, dict):
                    raise ValueError(f"missing setup '{instrument}/{session_bucket}/{setup_bucket}'")
                for iv_bucket in REQUIRED_IV_BUCKETS:
                    _validate_profile_leaf(
                        instrument,
                        session_bucket,
                        setup_bucket,
                        iv_bucket,
                        setup_map.get(iv_bucket),
                    )


def get_risk_profile_matrix():
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is required to load config/risk_profiles.yaml. "
            "Install dependencies from requirements.txt."
        ) from exc

    yaml_path = _yaml_path()
    if not yaml_path.exists():
        raise FileNotFoundError(f"Risk profile YAML not found: {yaml_path}")

    with yaml_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    matrix = data.get("risk_profiles") if isinstance(data, dict) and "risk_profiles" in data else data
    _validate_risk_profile_matrix(matrix)
    return matrix
