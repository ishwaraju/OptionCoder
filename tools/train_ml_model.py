#!/usr/bin/env python3
"""
ML Model Trainer for Signal Prediction
FREE - Trains on your historical data from alert_reviews_5m
Usage: python3 tools/train_ml_model.py
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

def train_model():
    """
    Train ML model on historical signal data
    """
    print("="*70)
    print("🤖 ML Model Trainer - FREE (scikit-learn)")
    print("="*70)
    
    # Check if scikit-learn is installed
    try:
        from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
        from sklearn.model_selection import train_test_split, cross_val_score, TimeSeriesSplit
        from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
        from sklearn.preprocessing import StandardScaler
        import joblib
        print("✅ scikit-learn found")
    except ImportError:
        print("❌ scikit-learn not installed")
        print("Install with: pip install scikit-learn joblib")
        print("")
        print("Installing now...")
        os.system("pip install scikit-learn joblib -q")
        print("✅ Installed! Please run again.")
        return
    
    # Load data from database
    print("\n📊 Loading training data from database...")
    
    from shared.db.pool import DBPool
    DBPool.initialize()
    
    if not DBPool._enabled:
        print("❌ Database not connected")
        return
    
    # Query training data from ml_features_log joined with outcomes
    query = """
    SELECT 
        m.id,
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
        m.actual_outcome,
        m.max_favorable_points,
        m.max_adverse_points,
        a.usefulness,
        a.outcome_tag
    FROM ml_features_log m
    LEFT JOIN alert_reviews_5m a 
        ON m.alert_ts = a.alert_ts 
        AND m.instrument = a.instrument
        AND a.alert_kind = 'SWING'
    WHERE m.actual_outcome IS NOT NULL
    AND m.actual_outcome IN ('PROFIT', 'LOSS')
    AND m.score > 0
    ORDER BY m.alert_ts DESC
    LIMIT 2000;
    """
    
    with DBPool.connection() as conn:
        df = pd.read_sql(query, conn)
    
    if len(df) == 0:
        print("❌ No training data found!")
        print("\nYou need at least 50 completed trades with known outcomes.")
        print("Outcomes are filled from alert_reviews_5m table.")
        print("\nCurrent status:")
        check_data_status()
        return
    
    print(f"✅ Loaded {len(df)} training samples")
    
    # Check class distribution
    outcome_counts = df['actual_outcome'].value_counts()
    print(f"\n📈 Outcome Distribution:")
    for outcome, count in outcome_counts.items():
        pct = count / len(df) * 100
        print(f"   {outcome}: {count} ({pct:.1f}%)")
    
    if len(df) < 50:
        print(f"\n⚠️  Only {len(df)} samples. Need at least 50 for reliable training.")
        print("Continue collecting data...")
        return
    
    # Prepare features
    print("\n🔧 Preparing features...")
    
    # Encode categorical variables
    df['time_regime_encoded'] = df['time_regime'].map({
        'PRE_MARKET': 0, 'OPENING': 1, 'MID_MORNING': 2,
        'LUNCH': 3, 'AFTERNOON': 4, 'CLOSING': 5
    }).fillna(3)
    
    df['pressure_encoded'] = df['pressure_conflict_level'].map({
        'NONE': 0, 'MILD': 1, 'MODERATE': 2, 'SEVERE': 3
    }).fillna(0)
    
    df['oi_bias_encoded'] = df['oi_bias'].map({
        'BEARISH': -1, 'NEUTRAL': 0, 'BULLISH': 1
    }).fillna(0)
    
    df['trend_aligned_num'] = df['trend_aligned'].astype(int)
    df['has_hybrid_num'] = df['has_hybrid_mode'].astype(int)
    
    # Fill missing values
    df['adx'] = df['adx'].fillna(25)
    df['volume_ratio'] = df['volume_ratio'].fillna(1.0)
    df['oi_change_pct'] = df['oi_change_pct'].fillna(0)
    df['vwap_distance'] = df['vwap_distance'].fillna(0)
    df['risk_reward_ratio'] = df['risk_reward_ratio'].fillna(1.0)
    
    # Feature columns
    feature_cols = [
        'score', 'adx', 'volume_ratio', 'oi_change_pct', 'vwap_distance',
        'time_hour', 'time_regime_encoded', 'pressure_encoded', 'oi_bias_encoded',
        'trend_aligned_num', 'risk_reward_ratio', 'has_hybrid_num'
    ]
    
    X = df[feature_cols]
    y = (df['actual_outcome'] == 'PROFIT').astype(int)
    
    # Feature importance analysis
    print(f"\n📋 Features used ({len(feature_cols)}):")
    for col in feature_cols:
        print(f"   - {col}")
    
    # Time-series split for backtesting
    print("\n🧪 Running time-series cross-validation...")
    tscv = TimeSeriesSplit(n_splits=5)
    
    # Try multiple models
    models = {
        'RandomForest': RandomForestClassifier(
            n_estimators=100,
            max_depth=5,
            min_samples_split=10,
            min_samples_leaf=5,
            random_state=42
        ),
        'GradientBoosting': GradientBoostingClassifier(
            n_estimators=100,
            max_depth=3,
            learning_rate=0.1,
            random_state=42
        )
    }
    
    best_model = None
    best_score = 0
    best_model_name = ""
    
    for name, model in models.items():
        scores = cross_val_score(model, X, y, cv=tscv, scoring='roc_auc')
        mean_score = scores.mean()
        print(f"   {name}: ROC-AUC = {mean_score:.3f} (+/- {scores.std():.3f})")
        
        if mean_score > best_score:
            best_score = mean_score
            best_model = model
            best_model_name = name
    
    print(f"\n✅ Best model: {best_model_name} (ROC-AUC: {best_score:.3f})")
    
    # Train on full dataset
    print(f"\n🎯 Training final model on all {len(df)} samples...")
    best_model.fit(X, y)
    
    # Feature importance
    if hasattr(best_model, 'feature_importances_'):
        print("\n📊 Feature Importance:")
        importances = best_model.feature_importances_
        for name, importance in sorted(zip(feature_cols, importances), 
                                       key=lambda x: x[1], reverse=True):
            bar = "█" * int(importance * 50)
            print(f"   {name:20s}: {importance:.3f} {bar}")
    
    # Save model
    model_dir = Path("models")
    model_dir.mkdir(exist_ok=True)
    
    model_path = model_dir / "signal_predictor.pkl"
    joblib.dump(best_model, model_path)
    
    # Save metadata
    metadata = {
        'model_type': best_model_name,
        'training_samples': len(df),
        'roc_auc': best_score,
        'features': feature_cols,
        'trained_at': datetime.now().isoformat(),
        'outcome_distribution': outcome_counts.to_dict()
    }
    
    metadata_path = model_dir / "model_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n💾 Model saved to: {model_path}")
    print(f"💾 Metadata saved to: {metadata_path}")
    
    # Performance by instrument
    print("\n📈 Performance by Instrument:")
    for inst in df['instrument'].unique():
        inst_df = df[df['instrument'] == inst]
        win_rate = (inst_df['actual_outcome'] == 'PROFIT').mean() * 100
        print(f"   {inst}: {len(inst_df)} trades, Win rate: {win_rate:.1f}%")
    
    print("\n" + "="*70)
    print("✅ Model training complete!")
    print("="*70)
    print(f"\nNext steps:")
    print(f"1. Model is now active and will filter signals")
    print(f"2. Restart signal services to use new model")
    print(f"3. Threshold: 55% win probability minimum")
    print(f"4. Check logs for 'ML Approved' or 'ML Filtered' messages")


def check_data_status():
    """Check how much training data is available"""
    from shared.db.pool import DBPool
    DBPool.initialize()
    
    if not DBPool._enabled:
        return
    
    with DBPool.connection() as conn:
        # Count signals with outcomes
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN actual_outcome IS NOT NULL THEN 1 ELSE 0 END) as with_outcome
            FROM ml_features_log
        """)
        total, with_outcome = cur.fetchone()
        
        print(f"\n📊 Data Status:")
        print(f"   Total signals logged: {total}")
        print(f"   With known outcomes: {with_outcome}")
        print(f"   Missing outcomes: {total - with_outcome}")
        
        if with_outcome < 50:
            print(f"\n   Need {50 - with_outcome} more completed trades")
            print(f"   Outcomes come from alert_reviews_5m table")
            print(f"   Run: python3 tools/score_alert_outcomes.py")
        
        cur.close()


def update_outcomes():
    """Fill missing outcomes from alert_reviews"""
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


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Train ML model on historical signals")
    parser.add_argument("--update-outcomes", action="store_true", 
                       help="Update missing outcomes from alert_reviews first")
    args = parser.parse_args()
    
    if args.update_outcomes:
        update_outcomes()
    
    train_model()
