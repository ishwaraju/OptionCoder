"""
ML Signal Filter
Filters signals based on trained ML model
FREE - Uses local scikit-learn model
"""

import os
import sys
import json
from pathlib import Path
from typing import Tuple, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class MLSignalFilter:
    """
    ML-based signal filter
    Predicts win probability and filters low-probability signals
    """
    
    DEFAULT_THRESHOLD = 0.55  # 55% win probability minimum
    MIN_TRAINING_SAMPLES = 50  # Minimum samples before using model
    
    def __init__(self, model_path='models/signal_predictor.pkl', threshold=None):
        self.model_path = Path(model_path)
        self.threshold = threshold or self.DEFAULT_THRESHOLD
        self.model = None
        self.model_loaded = False
        self.training_samples = 0
        
        # Try to load model
        self._load_model()
        
    def _load_model(self):
        """Load trained model if available"""
        try:
            import joblib
            if self.model_path.exists():
                self.model = joblib.load(self.model_path)
                self.model_loaded = True
                print(f"[ML Filter] Model loaded: {self.model_path}")
            else:
                print(f"[ML Filter] No model found at {self.model_path}")
                print("[ML Filter] Will use rule-based fallback until model is trained")
        except ImportError:
            print("[ML Filter] scikit-learn not installed. Install with: pip install scikit-learn")
        except Exception as e:
            print(f"[ML Filter] Error loading model: {e}")

    @staticmethod
    def _normalize_time_regime(time_regime: str) -> str:
        regime = str(time_regime or "UNKNOWN").upper()
        aliases = {
            "LUNCH": "MIDDAY",
            "AFTERNOON": "LATE_DAY",
            "CLOSING": "ENDGAME",
        }
        return aliases.get(regime, regime)

    def _build_adaptive_profile(self, features: dict) -> dict:
        time_regime = self._normalize_time_regime(features.get("time_regime"))
        market_regime = str(features.get("market_regime", "UNKNOWN") or "UNKNOWN").upper()
        signal_type = str(features.get("signal_type", "UNKNOWN") or "UNKNOWN").upper()
        participation_quality = str(features.get("participation_quality", "UNKNOWN") or "UNKNOWN").upper()
        confidence = str(features.get("confidence", "LOW") or "LOW").upper()
        instrument = str(features.get("instrument", "UNKNOWN") or "UNKNOWN").upper()
        spread_pct = float(features.get("spread_pct", 0.0) or 0.0)

        profile = {
            "threshold": float(self.threshold),
            "score_floor": 60.0,
            "adx_floor": 18.0,
            "volume_floor": 1.10,
            "rr_floor": 1.20,
            "label": [],
            "time_regime": time_regime,
            "market_regime": market_regime,
        }

        if time_regime == "OPENING":
            profile["threshold"] -= 0.02
            profile["score_floor"] -= 2
            profile["adx_floor"] -= 1
            profile["label"].append("opening_relaxed")
        elif time_regime == "MID_MORNING":
            profile["threshold"] -= 0.01
            profile["score_floor"] -= 1
            profile["label"].append("mid_morning_friendly")
        elif time_regime == "MIDDAY":
            profile["threshold"] += 0.03
            profile["score_floor"] += 4
            profile["adx_floor"] += 2
            profile["volume_floor"] += 0.10
            profile["rr_floor"] += 0.10
            profile["label"].append("midday_strict")
        elif time_regime == "LATE_DAY":
            profile["threshold"] += 0.02
            profile["score_floor"] += 2
            profile["volume_floor"] += 0.05
            profile["label"].append("late_day_cautious")
        elif time_regime == "ENDGAME":
            profile["threshold"] += 0.04
            profile["score_floor"] += 5
            profile["adx_floor"] += 1
            profile["volume_floor"] += 0.10
            profile["rr_floor"] += 0.15
            profile["label"].append("endgame_strict")

        if market_regime in {"TRENDING", "EXPANDING", "OPENING_EXPANSION"}:
            profile["threshold"] -= 0.02
            profile["score_floor"] -= 2
            profile["volume_floor"] -= 0.05
            profile["label"].append("trend_support")
        elif market_regime in {"RANGING", "CHOPPY"}:
            profile["threshold"] += 0.03
            profile["score_floor"] += 4
            profile["adx_floor"] += 2
            profile["volume_floor"] += 0.10
            profile["rr_floor"] += 0.10
            profile["label"].append("range_penalty")

        if signal_type in {"RETEST", "BREAKOUT_CONFIRM"}:
            profile["threshold"] -= 0.01
            profile["label"].append("confirmation_setup")
        elif signal_type == "CONTINUATION":
            profile["threshold"] += 0.01
            profile["score_floor"] += 2
            profile["label"].append("continuation_needs_strength")

        if participation_quality == "STRONG":
            profile["threshold"] -= 0.01
            profile["label"].append("strong_participation")
        elif participation_quality == "WEAK":
            profile["threshold"] += 0.02
            profile["score_floor"] += 2
            profile["volume_floor"] += 0.05
            profile["label"].append("weak_participation")

        if confidence == "HIGH":
            profile["threshold"] -= 0.01
        elif confidence == "LOW":
            profile["threshold"] += 0.01

        if spread_pct >= 4.5:
            profile["threshold"] += 0.02
            profile["score_floor"] += 1
            profile["label"].append("wide_spread")
        elif 0 < spread_pct <= 2.0:
            profile["threshold"] -= 0.01

        if instrument == "SENSEX":
            profile["threshold"] -= 0.01
        elif instrument == "BANKNIFTY" and time_regime in {"MIDDAY", "ENDGAME"}:
            profile["threshold"] += 0.01
            profile["score_floor"] += 1

        profile["threshold"] = max(0.50, min(0.72, round(profile["threshold"], 3)))
        profile["score_floor"] = max(55.0, min(80.0, round(profile["score_floor"], 1)))
        profile["adx_floor"] = max(16.0, min(24.0, round(profile["adx_floor"], 1)))
        profile["volume_floor"] = max(1.0, min(1.4, round(profile["volume_floor"], 2)))
        profile["rr_floor"] = max(1.1, min(1.5, round(profile["rr_floor"], 2)))
        return profile

    def explain_decision(self, features: dict, probability: float = None) -> dict:
        profile = self._build_adaptive_profile(features)
        score = float(features.get("score", 0) or 0)
        entry_score = float(features.get("entry_score", score) or score)
        adx = float(features.get("adx", 0) or 0)
        volume_ratio = float(features.get("volume_ratio", 0) or 0)
        rr_ratio = float(features.get("risk_reward_ratio", 0) or 0)
        spread_pct = float(features.get("spread_pct", 0) or 0)
        positives = []
        negatives = []

        if max(score, entry_score) >= profile["score_floor"]:
            positives.append("score_floor_pass")
        else:
            negatives.append("score_floor_fail")
        if adx >= profile["adx_floor"]:
            positives.append("adx_ok")
        else:
            negatives.append("adx_weak")
        if volume_ratio >= profile["volume_floor"]:
            positives.append("volume_ok")
        else:
            negatives.append("volume_weak")
        if rr_ratio >= profile["rr_floor"]:
            positives.append("rr_ok")
        elif rr_ratio > 0:
            negatives.append("rr_weak")
        if spread_pct >= 6.5:
            negatives.append("spread_wide")
        if features.get("trend_aligned"):
            positives.append("trend_aligned")
        else:
            negatives.append("trend_not_aligned")
        if probability is not None:
            if probability >= profile["threshold"]:
                positives.append("probability_pass")
            else:
                negatives.append("probability_below_threshold")

        return {
            "threshold": profile["threshold"],
            "profile_tags": profile["label"],
            "positives": positives[:4],
            "negatives": negatives[:4],
        }
            
    def should_take_signal(self, features: dict) -> Tuple[bool, str, float]:
        """
        Determine if signal should be taken based on ML prediction
        
        Returns:
            (should_take: bool, reason: str, probability: float)
        """
        adaptive_profile = self._build_adaptive_profile(features)
        if not self.model_loaded or not self.model:
            return self._rule_based_fallback(features, adaptive_profile=adaptive_profile)
        
        try:
            # Convert features to vector
            from .feature_extractor import MLFeatureExtractor
            extractor = MLFeatureExtractor()
            feature_vector = extractor.get_feature_vector(features)
            
            # Predict probability
            import numpy as np
            X = np.array([feature_vector])
            prob = self.model.predict_proba(X)[0][1]  # Probability of class 1 (profit)
            
            # Check threshold
            threshold = adaptive_profile["threshold"]
            profile_tag = "/".join(adaptive_profile["label"][:2]) or "balanced"
            if prob >= threshold:
                return True, f"ML Approved (prob: {prob:.1%} >= {threshold:.1%}, {profile_tag})", prob
            else:
                return False, f"ML Filtered (prob: {prob:.1%} < {threshold:.1%}, {profile_tag})", prob
                
        except Exception as e:
            print(f"[ML Filter] Prediction error: {e}")
            # Fallback: approve and log
            return True, f"ML Error - Approving for logging", 0.5
    
    def _calculate_mock_probability(self, features: dict) -> float:
        """Calculate mock probability for logging purposes only"""
        score = 0
        
        # Simple scoring for reference
        base_score = features.get('score', 0)
        score += min(base_score / 2, 25)  # 0-25 pts
        
        adx = features.get('adx', 0)
        score += min(adx, 20)  # 0-20 pts
        
        volume = features.get('volume_ratio', 1.0)
        if volume >= 1.5: score += 15
        elif volume >= 1.2: score += 12
        elif volume >= 1.0: score += 8
        else: score += 4
        
        if features.get('trend_aligned', False):
            score += 15
        else:
            score += 5
        
        time_regime = features.get('time_regime', 'LUNCH')
        if time_regime in ['OPENING', 'MID_MORNING']: score += 15
        elif time_regime == 'AFTERNOON': score += 10
        else: score += 5
        
        return score / 100

    def _rule_based_fallback(self, features: dict, adaptive_profile: Optional[dict] = None) -> Tuple[bool, str, float]:
        """Conservative fallback used until a trained model exists."""
        adaptive_profile = adaptive_profile or self._build_adaptive_profile(features)
        prob = self._calculate_mock_probability(features)
        blockers = []
        penalties = []

        score = float(features.get('score', 0) or 0)
        entry_score = float(features.get('entry_score', score) or score)
        adx = float(features.get('adx', 0) or 0)
        volume_ratio = float(features.get('volume_ratio', 1.0) or 1.0)
        spread_pct = float(features.get('spread_pct', 0.0) or 0.0)
        rr_ratio = float(features.get('risk_reward_ratio', 0.0) or 0.0)
        time_regime = self._normalize_time_regime(features.get('time_regime', 'UNKNOWN'))
        confidence = str(features.get('confidence', 'LOW') or 'LOW').upper()
        signal_grade = str(features.get('signal_grade', 'SKIP') or 'SKIP').upper()
        signal_type = str(features.get('signal_type', 'UNKNOWN') or 'UNKNOWN').upper()
        conflict = str(features.get('pressure_conflict_level', 'NONE') or 'NONE').upper()
        participation_quality = str(features.get('participation_quality', 'UNKNOWN') or 'UNKNOWN').upper()
        breadth_ratio = float(features.get('breadth_ratio', 1.0) or 1.0)
        trend_aligned = bool(features.get('trend_aligned', False))
        score_floor = float(adaptive_profile["score_floor"])
        adx_floor = float(adaptive_profile["adx_floor"])
        volume_floor = float(adaptive_profile["volume_floor"])
        rr_floor = float(adaptive_profile["rr_floor"])
        threshold = float(adaptive_profile["threshold"])
        profile_tag = "/".join(adaptive_profile["label"][:2]) or "balanced"

        if signal_type not in {"OPENING_DRIVE", "BREAKOUT", "BREAKOUT_CONFIRM", "RETEST", "CONTINUATION"}:
            blockers.append("setup_not_whitelisted")
        if signal_grade not in {"A+", "A", "B"}:
            blockers.append("weak_grade")
        if max(score, entry_score) < score_floor:
            blockers.append("score_below_floor")
        if adx and adx < adx_floor:
            blockers.append("adx_too_low")
        if volume_ratio < volume_floor:
            blockers.append("volume_expansion_missing")
        if time_regime in {"MIDDAY", "UNKNOWN"} and max(score, entry_score) < 78:
            blockers.append("bad_time_regime")
        if spread_pct >= 6.5:
            blockers.append("spread_too_wide")
        if rr_ratio and rr_ratio < rr_floor:
            blockers.append("poor_risk_reward")
        if conflict in {"MODERATE", "SEVERE"} and not trend_aligned and max(score, entry_score) < 82:
            blockers.append("pressure_conflict")
        if participation_quality == "WEAK" and breadth_ratio < 1.05:
            blockers.append("weak_participation")

        if confidence == "HIGH":
            prob += 0.06
        elif confidence == "LOW":
            penalties.append("low_confidence")
            prob -= 0.05

        if trend_aligned:
            prob += 0.05
        else:
            penalties.append("trend_not_aligned")
            prob -= 0.05

        if signal_grade in {"A+", "A"}:
            prob += 0.04
        elif signal_grade == "B":
            prob -= 0.03

        if participation_quality == "STRONG":
            prob += 0.05
        elif participation_quality == "WEAK":
            prob -= 0.08

        if breadth_ratio >= 1.3:
            prob += 0.04
        elif breadth_ratio < 1.0:
            prob -= 0.05

        if spread_pct >= 4.5:
            penalties.append("wide_spread_penalty")
            prob -= 0.08
        elif 0 < spread_pct <= 2.0:
            prob += 0.03

        if volume_ratio >= 1.8:
            prob += 0.05
        elif volume_ratio < 1.2:
            prob -= 0.06

        prob = max(0.05, min(0.95, prob))

        if blockers:
            return False, f"Rule blocked: {', '.join(blockers[:3])} (p={prob:.1%}, thr={threshold:.1%}, {profile_tag})", prob

        if prob >= threshold:
            reason = f"Rule approved (p={prob:.1%}, thr={threshold:.1%}, {profile_tag})"
            if penalties:
                reason += f" with cautions: {', '.join(penalties[:2])}"
            return True, reason, prob

        return False, f"Rule filtered (p={prob:.1%} < {threshold:.1%}, {profile_tag})", prob
    
    def get_model_status(self) -> dict:
        """Get current model status"""
        return {
            'model_loaded': self.model_loaded,
            'model_path': str(self.model_path),
            'threshold': self.threshold,
            'training_samples': self.training_samples,
            'using_fallback': not self.model_loaded
        }
