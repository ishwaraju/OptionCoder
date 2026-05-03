"""
ML Feature Extractor
Extracts features from signal context for ML training/prediction
FREE - No API calls, uses local calculations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime
from typing import Dict, Any, Optional

from shared.indicators.adx_calculator import ADXCalculator


class MLFeatureExtractor:
    """
    Extract ML features from signal generation context
    All features are calculated locally - no external API calls
    """
    
    def __init__(self):
        self.feature_cache = {}
        
    def extract_features(
        self,
        instrument: str,
        signal_direction: str,
        price: float,
        vwap: float,
        atr: float,
        score: int,
        confidence: str,
        time_regime: str,
        oi_ladder_data: Optional[Dict] = None,
        pressure_metrics: Optional[Dict] = None,
        trend_15m: Optional[str] = None,
        recent_candles_5m: Optional[list] = None,
        strategy_context: Optional[Dict] = None,
        entry_plan: Optional[Dict] = None,
        participation_metrics: Optional[Dict] = None,
        market_regime: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Extract comprehensive feature set for ML model
        
        Returns dict with all features for logging and prediction
        """
        from shared.utils.time_utils import TimeUtils
        
        time_utils = TimeUtils()
        current_time = time_utils.now_ist()
        
        # Base features
        features = {
            'alert_ts': current_time,
            'instrument': instrument,
            'signal_direction': signal_direction,
            'score': score,
            'confidence': confidence,
            'time_hour': current_time.hour,
            'time_regime': time_regime,
            'market_regime': market_regime or 'UNKNOWN',
        }
        
        # VWAP distance (momentum)
        if vwap and price:
            vwap_distance = abs(price - vwap) / vwap * 100
            features['vwap_distance'] = round(vwap_distance, 3)
        else:
            features['vwap_distance'] = 0.0
            
        # ATR (volatility)
        features['atr'] = round(atr, 2) if atr else 0.0
        features['adx'] = self._calculate_adx(recent_candles_5m)
        
        # Price momentum (last 2 candles)
        if recent_candles_5m and len(recent_candles_5m) >= 2:
            try:
                prev_close = recent_candles_5m[-2].get('close', price)
                if prev_close and price:
                    momentum = (price - prev_close) / prev_close * 100
                    features['price_momentum'] = round(momentum, 3)
                else:
                    features['price_momentum'] = 0.0
            except:
                features['price_momentum'] = 0.0
        else:
            features['price_momentum'] = 0.0
            
        # OI Ladder features
        if oi_ladder_data:
            features['oi_bias'] = oi_ladder_data.get('trend', 'NEUTRAL')
            features['oi_trend'] = oi_ladder_data.get('trend', 'SIDEWAYS')
            features['wall_break_alert'] = oi_ladder_data.get('wall_break_alert')
            features['support_wall_state'] = oi_ladder_data.get('support_wall_state')
            features['resistance_wall_state'] = oi_ladder_data.get('resistance_wall_state')
            features['oi_divergence'] = oi_ladder_data.get('price_vs_oi_divergence')
            
            # Calculate OI change % if available
            ce_delta = oi_ladder_data.get('ce_delta_total', 0)
            pe_delta = oi_ladder_data.get('pe_delta_total', 0)
            total_delta = abs(ce_delta) + abs(pe_delta)
            if total_delta > 0:
                oi_change = (pe_delta - ce_delta) / total_delta * 100
                features['oi_change_pct'] = round(oi_change, 2)
            else:
                features['oi_change_pct'] = 0.0
        else:
            features['oi_bias'] = 'NEUTRAL'
            features['oi_trend'] = 'SIDEWAYS'
            features['oi_change_pct'] = 0.0
            
        # Pressure metrics
        if pressure_metrics:
            conflict = pressure_metrics.get('pressure_conflict_level', 'NONE')
            features['pressure_conflict_level'] = conflict
            
            # Volume ratio from pressure
            volume_ratio = pressure_metrics.get('volume_ratio', 1.0)
            features['volume_ratio'] = round(volume_ratio, 2)
        else:
            features['pressure_conflict_level'] = 'NONE'
            features['volume_ratio'] = 1.0

        derived_volume_ratio = self._calculate_volume_ratio(recent_candles_5m)
        if derived_volume_ratio is not None:
            features['volume_ratio'] = derived_volume_ratio
            
        # Trend alignment
        if trend_15m:
            features['trend_15m'] = trend_15m
            trend_5m = self._calculate_5m_trend(recent_candles_5m)
            features['trend_5m'] = trend_5m
            
            # Check if signal aligns with trends
            aligned_15m = (signal_direction == 'CE' and 'UP' in trend_15m) or \
                         (signal_direction == 'PE' and 'DOWN' in trend_15m)
            aligned_5m = (signal_direction == 'CE' and 'UP' in trend_5m) or \
                        (signal_direction == 'PE' and 'DOWN' in trend_5m)
            features['trend_aligned'] = aligned_15m and aligned_5m
        else:
            features['trend_15m'] = 'NEUTRAL'
            features['trend_5m'] = 'NEUTRAL'
            features['trend_aligned'] = False
            
        # Strategy context features
        if strategy_context:
            features['signal_type'] = strategy_context.get('signal_type', 'UNKNOWN')
            features['signal_grade'] = strategy_context.get('signal_grade', 'SKIP')
            features['entry_score'] = strategy_context.get('entry_score', 0)
            features['context_score'] = strategy_context.get('context_score', 0)
            features['has_hybrid_mode'] = strategy_context.get('hybrid_mode', False)
        else:
            features['signal_type'] = 'UNKNOWN'
            features['signal_grade'] = 'SKIP'
            features['entry_score'] = score
            features['context_score'] = score
            features['has_hybrid_mode'] = False
            
        # Entry plan (target/stop)
        if entry_plan:
            entry_above = entry_plan.get('entry_above')
            entry_below = entry_plan.get('entry_below')
            invalidate = entry_plan.get('invalidate_price')
            target = entry_plan.get('first_target_price')
            
            if signal_direction == 'CE' and entry_above and invalidate and target:
                stop = abs(entry_above - invalidate)
                tgt = abs(target - entry_above)
                features['target_points'] = round(tgt, 2)
                features['stop_points'] = round(stop, 2)
                features['risk_reward_ratio'] = round(tgt / stop, 2) if stop > 0 else 0.0
            elif signal_direction == 'PE' and entry_below and invalidate and target:
                stop = abs(entry_below - invalidate)
                tgt = abs(entry_below - target)
                features['target_points'] = round(tgt, 2)
                features['stop_points'] = round(stop, 2)
                features['risk_reward_ratio'] = round(tgt / stop, 2) if stop > 0 else 0.0
            else:
                features['target_points'] = 0.0
                features['stop_points'] = 0.0
                features['risk_reward_ratio'] = 0.0
        else:
            features['target_points'] = 0.0
            features['stop_points'] = 0.0
            features['risk_reward_ratio'] = 0.0

        directional_participation = (
            (participation_metrics or {}).get(signal_direction) if signal_direction in {'CE', 'PE'} else None
        ) or {}
        features['participation_quality'] = directional_participation.get('quality', 'UNKNOWN')
        features['spread_pct'] = (
            round(float(directional_participation.get('atm_spread_pct')), 2)
            if directional_participation.get('atm_spread_pct') is not None
            else 0.5
        )
        same_breadth = float(directional_participation.get('same_side_weighted_breadth') or 0.0)
        opp_breadth = float(directional_participation.get('opposite_side_weighted_breadth') or 0.0)
        features['breadth_ratio'] = round(same_breadth / max(opp_breadth, 0.1), 2)

        # IV Rank (mock - can be enhanced with actual IV data)
        features['iv_rank'] = 50.0  # Placeholder
        
        return features

    def _calculate_adx(self, candles_5m):
        if not candles_5m or len(candles_5m) < 15:
            return 0.0

        try:
            adx_calc = ADXCalculator(period=14)
            for candle in candles_5m[-15:]:
                adx_calc.update(
                    candle.get('high', 0),
                    candle.get('low', 0),
                    candle.get('close', 0),
                )
            adx_data = adx_calc.get_current()
            return round(float((adx_data or {}).get('adx') or 0.0), 2)
        except Exception:
            return 0.0

    def _calculate_volume_ratio(self, candles_5m):
        if not candles_5m or len(candles_5m) < 4:
            return None

        try:
            current_volume = float(candles_5m[-1].get('volume') or 0.0)
            prior_volumes = [
                float(candle.get('volume') or 0.0)
                for candle in candles_5m[-9:-1]
                if float(candle.get('volume') or 0.0) > 0
            ]
            if current_volume <= 0 or not prior_volumes:
                return None
            average_volume = sum(prior_volumes) / len(prior_volumes)
            if average_volume <= 0:
                return None
            return round(current_volume / average_volume, 2)
        except Exception:
            return None
    
    def _calculate_5m_trend(self, candles_5m):
        """Simple trend calculation from recent 5m candles"""
        if not candles_5m or len(candles_5m) < 3:
            return 'NEUTRAL'
            
        try:
            first = candles_5m[0].get('close', 0)
            last = candles_5m[-1].get('close', 0)
            
            if first and last:
                change = (last - first) / first * 100
                if change > 0.3:
                    return 'UP'
                elif change < -0.3:
                    return 'DOWN'
            return 'NEUTRAL'
        except:
            return 'NEUTRAL'
    
    def get_feature_vector(self, features: Dict[str, Any]) -> list:
        """
        Convert feature dict to numeric vector for ML model
        Returns list of numeric values
        """
        # Encode categorical variables
        confidence_map = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2, 'VERY_HIGH': 3}
        time_regime_map = {'PRE_MARKET': 0, 'OPENING': 1, 'MID_MORNING': 2, 
                          'MIDDAY': 3, 'LUNCH': 3, 'AFTERNOON': 4, 'LATE_DAY': 4, 'ENDGAME': 5, 'CLOSING': 5}
        bias_map = {'BEARISH': -1, 'NEUTRAL': 0, 'BULLISH': 1}
        market_regime_map = {
            'RANGING': 0,
            'CHOPPY': 1,
            'TRENDING': 2,
            'EXPANDING': 3,
            'OPENING_EXPANSION': 4,
        }

        conflict_map = {'NONE': 0, 'MILD': 1, 'MODERATE': 2, 'SEVERE': 3}
        
        vector = [
            features.get('score', 0) / 100.0,  # Normalize 0-1
            confidence_map.get(features.get('confidence', 'LOW'), 0) / 3.0,
            features.get('adx', 25) / 50.0,  # Normalize
            features.get('volume_ratio', 1.0) / 3.0,  # Cap at 3x
            features.get('oi_change_pct', 0) / 100.0,  # Normalize
            features.get('vwap_distance', 0) / 2.0,  # Cap at 2%
            features.get('time_hour', 12) / 24.0,  # Hour normalized
            time_regime_map.get(features.get('time_regime', 'LUNCH'), 3) / 5.0,
            market_regime_map.get(features.get('market_regime', 'UNKNOWN'), 1) / 4.0,
            bias_map.get(features.get('oi_bias', 'NEUTRAL'), 0),
            conflict_map.get(features.get('pressure_conflict_level', 'NONE'), 0) / 3.0,
            1.0 if features.get('trend_aligned', False) else 0.0,
            features.get('risk_reward_ratio', 1.0) / 3.0,  # Cap at 3:1
            1.0 if features.get('has_hybrid_mode', False) else 0.0,
        ]
        
        return vector
