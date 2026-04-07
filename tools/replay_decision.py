import csv
import sys
from pathlib import Path


def main():
    audit_path = Path("/Users/ishwar/Documents/OptionCoder/data/decision_audit.csv")
    if not audit_path.exists():
        print("decision_audit.csv not found")
        return

    rows = list(csv.DictReader(audit_path.open()))
    if not rows:
        print("No decision rows found")
        return

    target = sys.argv[1] if len(sys.argv) > 1 else None

    if target:
        matches = [row for row in rows if target in row["time"]]
        if not matches:
            print(f"No decision found for time containing: {target}")
            return
        row = matches[-1]
    else:
        row = rows[-1]

    print("Decision Replay")
    print("Time:", row["time"])
    print("Instrument:", row["instrument"])
    print("Signal:", row["signal"])
    print("Strike:", row["strike"])
    print("Score:", row["score"])
    print("Confidence:", row["confidence"])
    print("Regime:", row["regime"])
    print("Manual Guidance:", row["manual_guidance"])
    print("Signal Valid Till:", row["signal_valid_till"])
    print("Blockers:", row["blockers"])
    print("Cautions:", row["cautions"])
    print("Score Factors:", row["score_factors"])
    print("Reason:", row["reason"])
    print("Strike Reason:", row["strike_reason"])


if __name__ == "__main__":
    main()
