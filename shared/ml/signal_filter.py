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
            
    def should_take_signal(self, features: dict) -> Tuple[bool, str, float]:
        """
        Determine if signal should be taken based on ML prediction
        
        Returns:
            (should_take: bool, reason: str, probability: float)
        """
        # TEMPORARY: Until ML model is trained (2 weeks), approve all signals
        # Just log features for training data collection
        if not self.model_loaded or not self.model:
            # Calculate what the score would be for logging purposes
            mock_prob = self._calculate_mock_probability(features)
            return True, f"Logging for ML (score: {mock_prob:.0%})", mock_prob
        
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
            if prob >= self.threshold:
                return True, f"ML Approved (prob: {prob:.1%})", prob
            else:
                return False, f"ML Filtered (prob: {prob:.1%} < {self.threshold:.1%})", prob
                
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
    
    def get_model_status(self) -> dict:
        """Get current model status"""
        return {
            'model_loaded': self.model_loaded,
            'model_path': str(self.model_path),
            'threshold': self.threshold,
            'training_samples': self.training_samples,
            'using_fallback': not self.model_loaded
        }

