#!/usr/bin/env python3
"""Train a time-series-safe ML model for signal prediction."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


TIME_REGIME_MAP = {
    "PRE_MARKET": 0,
    "OPENING": 1,
    "MID_MORNING": 2,
    "MIDDAY": 3,
    "LUNCH": 3,
    "AFTERNOON": 4,
    "LATE_DAY": 4,
    "ENDGAME": 5,
    "CLOSING": 5,
}
PRESSURE_MAP = {"NONE": 0, "MILD": 1, "MODERATE": 2, "SEVERE": 3}
OI_BIAS_MAP = {"BEARISH": -1, "NEUTRAL": 0, "BULLISH": 1}
SIGNAL_TYPE_MAP = {"OPENING_DRIVE": 0, "BREAKOUT": 1, "BREAKOUT_CONFIRM": 2, "RETEST": 3, "CONTINUATION": 4}
SIGNAL_GRADE_MAP = {"SKIP": 0, "B": 1, "A": 2, "A+": 3}


def _load_training_frame(limit):
    from shared.db.pool import DBPool

    DBPool.initialize()
    if not DBPool._enabled:
        raise RuntimeError("Database not connected")

    query = f"""
    SELECT 
        m.id,
        m.alert_ts,
        m.instrument,
        m.signal_direction,
        m.score,
        m.adx,
        m.volume_ratio,
        m.oi_change_pct,
        m.vwap_distance,
        m.time_hour,
        m.time_regime,
        m.atr,
        m.price_momentum,
        m.pressure_conflict_level,
        m.oi_bias,
        m.trend_15m,
        m.trend_aligned,
        m.risk_reward_ratio,
        m.has_hybrid_mode,
        m.signal_type,
        m.signal_grade,
        m.entry_score,
        m.context_score,
        m.spread_pct,
        m.actual_outcome,
        m.max_favorable_points,
        m.max_adverse_points
    FROM ml_features_log m
    WHERE m.actual_outcome IS NOT NULL
      AND m.actual_outcome IN ('PROFIT', 'LOSS')
      AND m.score > 0
    ORDER BY m.alert_ts ASC
    LIMIT {int(limit)};
    """

    with DBPool.connection() as conn:
        return pd.read_sql(query, conn)


def _prepare_features(df):
    frame = df.copy()
    frame["time_regime_encoded"] = frame["time_regime"].map(TIME_REGIME_MAP).fillna(3)
    frame["pressure_encoded"] = frame["pressure_conflict_level"].map(PRESSURE_MAP).fillna(0)
    frame["oi_bias_encoded"] = frame["oi_bias"].map(OI_BIAS_MAP).fillna(0)
    frame["signal_type_encoded"] = frame["signal_type"].map(SIGNAL_TYPE_MAP).fillna(1)
    frame["signal_grade_encoded"] = frame["signal_grade"].map(SIGNAL_GRADE_MAP).fillna(0)
    frame["trend_aligned_num"] = frame["trend_aligned"].fillna(False).astype(int)
    frame["has_hybrid_num"] = frame["has_hybrid_mode"].fillna(False).astype(int)

    fill_defaults = {
        "adx": 25.0,
        "volume_ratio": 1.0,
        "oi_change_pct": 0.0,
        "vwap_distance": 0.0,
        "risk_reward_ratio": 1.0,
        "spread_pct": 3.0,
        "atr": 0.0,
        "price_momentum": 0.0,
    }
    for column, default in fill_defaults.items():
        frame[column] = frame[column].fillna(default)

    frame["entry_score"] = frame["entry_score"].fillna(frame["score"])
    frame["context_score"] = frame["context_score"].fillna(frame["score"])
    frame["time_hour"] = frame["time_hour"].fillna(12)

    feature_cols = [
        "score",
        "entry_score",
        "context_score",
        "adx",
        "volume_ratio",
        "oi_change_pct",
        "vwap_distance",
        "time_hour",
        "time_regime_encoded",
        "pressure_encoded",
        "oi_bias_encoded",
        "trend_aligned_num",
        "risk_reward_ratio",
        "has_hybrid_num",
        "signal_type_encoded",
        "signal_grade_encoded",
        "spread_pct",
        "atr",
        "price_momentum",
    ]
    return frame, feature_cols


def _evaluate_model(model, X, y, n_splits=5):
    from sklearn.metrics import brier_score_loss, roc_auc_score
    from sklearn.model_selection import TimeSeriesSplit

    tscv = TimeSeriesSplit(n_splits=n_splits)
    auc_scores = []
    brier_scores = []
    fold_rows = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X), start=1):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        model.fit(X_train, y_train)
        prob = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, prob) if len(set(y_test)) > 1 else 0.5
        brier = brier_score_loss(y_test, prob)
        auc_scores.append(float(auc))
        brier_scores.append(float(brier))
        fold_rows.append(
            {
                "fold": fold,
                "test_size": len(test_idx),
                "roc_auc": round(float(auc), 4),
                "brier": round(float(brier), 4),
            }
        )

    return {
        "auc_mean": float(np.mean(auc_scores)),
        "auc_std": float(np.std(auc_scores)),
        "brier_mean": float(np.mean(brier_scores)),
        "folds": fold_rows,
    }


def check_data_status():
    from shared.db.pool import DBPool

    DBPool.initialize()
    if not DBPool._enabled:
        return

    with DBPool.connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN actual_outcome IS NOT NULL THEN 1 ELSE 0 END) as with_outcome
            FROM ml_features_log
            """
        )
        total, with_outcome = cur.fetchone()
        print("\n📊 Data Status:")
        print(f"   Total signals logged: {total}")
        print(f"   With known outcomes: {with_outcome}")
        print(f"   Missing outcomes: {total - with_outcome}")
        if with_outcome < 50:
            print(f"\n   Need {50 - with_outcome} more completed trades")
            print("   Outcomes come from alert_reviews_5m table")
            print("   Run: python3 tools/score_alert_outcomes.py")
        cur.close()


def update_outcomes():
    print("\n🔄 Updating outcomes from alert_reviews...")
    from shared.db.pool import DBPool

    DBPool.initialize()
    if not DBPool._enabled:
        return

    update_query = """
    UPDATE ml_features_log m
    SET 
        actual_outcome = CASE 
            WHEN a.usefulness = 'PROFITABLE' THEN 'PROFIT'
            WHEN a.usefulness = 'LOSS_MAKING' THEN 'LOSS'
            ELSE 'BREAKEVEN'
        END,
        max_favorable_points = a.max_favorable_points,
        max_adverse_points = a.max_adverse_points,
        close_pnl_points = a.close_move_points,
        outcome_tag = a.outcome_tag,
        updated_at = NOW()
    FROM alert_reviews_5m a
    WHERE m.alert_ts = a.alert_ts
      AND m.instrument = a.instrument
      AND a.alert_kind = 'SWING'
      AND m.actual_outcome IS NULL;
    """

    with DBPool.connection() as conn:
        cur = conn.cursor()
        cur.execute(update_query)
        updated = cur.rowcount
        conn.commit()
        cur.close()

    print(f"✅ Updated {updated} records with outcomes")


def train_model(limit=3000, calibrate=True):
    print("=" * 70)
    print("ML Model Trainer - time-series safe")
    print("=" * 70)

    try:
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
        import joblib
        print("✅ scikit-learn found")
    except ImportError:
        print("❌ scikit-learn not installed")
        print("Install with: pip install scikit-learn joblib")
        return

    print("\n📊 Loading training data from database...")
    try:
        df = _load_training_frame(limit=limit)
    except Exception as exc:
        print(f"❌ {exc}")
        return

    if len(df) == 0:
        print("❌ No training data found!")
        check_data_status()
        return

    print(f"✅ Loaded {len(df)} training samples")
    outcome_counts = df["actual_outcome"].value_counts()
    print("\n📈 Outcome Distribution:")
    for outcome, count in outcome_counts.items():
        pct = count / len(df) * 100
        print(f"   {outcome}: {count} ({pct:.1f}%)")

    if len(df) < 50:
        print(f"\n⚠️  Only {len(df)} samples. Need at least 50 for reliable training.")
        return

    print("\n🔧 Preparing features...")
    prepared, feature_cols = _prepare_features(df)
    X = prepared[feature_cols]
    y = (prepared["actual_outcome"] == "PROFIT").astype(int)

    print(f"\n📋 Features used ({len(feature_cols)}):")
    for col in feature_cols:
        print(f"   - {col}")

    print("\n🧪 Running walk-forward validation...")
    models = {
        "RandomForest": RandomForestClassifier(
            n_estimators=120,
            max_depth=5,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42,
        ),
        "GradientBoosting": GradientBoostingClassifier(
            n_estimators=120,
            learning_rate=0.08,
            random_state=42,
        ),
    }

    best_name = None
    best_model = None
    best_metrics = None
    best_score = -1.0
    for name, model in models.items():
        metrics = _evaluate_model(model, X, y)
        print(
            f"   {name}: ROC-AUC={metrics['auc_mean']:.3f} (+/- {metrics['auc_std']:.3f}) | "
            f"Brier={metrics['brier_mean']:.3f}"
        )
        if metrics["auc_mean"] > best_score:
            best_name = name
            best_model = model
            best_metrics = metrics
            best_score = metrics["auc_mean"]

    print(f"\n✅ Best model: {best_name} (ROC-AUC: {best_score:.3f})")

    final_model = best_model
    calibrated = bool(calibrate and len(df) >= 120)
    if calibrated:
        print("\n🧭 Applying sigmoid probability calibration...")
        final_model = CalibratedClassifierCV(best_model, method="sigmoid", cv=3)

    print(f"\n🎯 Training final model on all {len(df)} samples...")
    final_model.fit(X, y)

    base_estimator = getattr(final_model, "estimator", final_model)
    if hasattr(base_estimator, "feature_importances_"):
        print("\n📊 Feature Importance:")
        importances = base_estimator.feature_importances_
        for name, importance in sorted(zip(feature_cols, importances), key=lambda item: item[1], reverse=True):
            bar = "#" * int(importance * 50)
            print(f"   {name:20s}: {importance:.3f} {bar}")

    model_dir = ROOT / "models"
    model_dir.mkdir(exist_ok=True)
    model_path = model_dir / "signal_predictor.pkl"
    metadata_path = model_dir / "model_metadata.json"

    joblib.dump(final_model, model_path)
    metadata = {
        "model_type": best_name,
        "calibrated": calibrated,
        "training_samples": len(df),
        "roc_auc": best_metrics["auc_mean"] if best_metrics else None,
        "roc_auc_std": best_metrics["auc_std"] if best_metrics else None,
        "brier_score": best_metrics["brier_mean"] if best_metrics else None,
        "features": feature_cols,
        "trained_at": datetime.now().isoformat(),
        "outcome_distribution": outcome_counts.to_dict(),
        "folds": best_metrics["folds"] if best_metrics else [],
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"\n💾 Model saved to: {model_path}")
    print(f"💾 Metadata saved to: {metadata_path}")
    print("\n📈 Performance by Instrument:")
    for inst in prepared["instrument"].unique():
        inst_df = prepared[prepared["instrument"] == inst]
        win_rate = (inst_df["actual_outcome"] == "PROFIT").mean() * 100
        print(f"   {inst}: {len(inst_df)} trades, Win rate: {win_rate:.1f}%")

    print("\nNext steps:")
    print("1. Restart signal services to load the new model")
    print("2. Review model_metadata.json for ROC-AUC and Brier score")
    print("3. Watch logs for ML Approved / ML Filtered decisions")


def parse_args():
    parser = argparse.ArgumentParser(description="Train ML model on historical signals")
    parser.add_argument("--update-outcomes", action="store_true", help="Update missing outcomes from alert_reviews first")
    parser.add_argument("--limit", type=int, default=3000, help="Maximum training rows to load")
    parser.add_argument("--no-calibration", action="store_true", help="Skip probability calibration")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.update_outcomes:
        update_outcomes()
    train_model(limit=args.limit, calibrate=not args.no_calibration)
