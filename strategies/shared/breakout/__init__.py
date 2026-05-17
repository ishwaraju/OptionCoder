"""
Breakout strategy helper package.

Groups day-state, scoring, and setup-family evaluators under one folder so the
shared strategy surface stays easier to scan.
"""

from .confirmation_engine import ConfirmationEngine, evaluate_confirmation_and_retest
from .continuation_engine import ContinuationEngine, evaluate_aggressive_continuations, evaluate_core_continuations
from .day_state_analyzer import DayStateAnalyzer, apply_day_state_adjustment, derive_active_day_state, derive_opening_bias
from .futures_acceptance_engine import FuturesAcceptanceEngine, evaluate_futures_acceptance
from .hybrid_continuation_engine import HybridContinuationEngine, evaluate_hybrid_continuations
from .no_setup_finalizer import NoSetupFinalizer, finalize_no_setup
from .opening_drive_engine import OpeningDriveEngine, evaluate_opening_drive
from .option_buyer_protection import (
    OptionBuyerProtection,
    apply_option_buyer_filters,
    check_adx_filter,
    check_multi_timeframe_filter,
    check_oi_filter,
    check_session_filter,
    check_volume_filter,
)
from .orb_engine import OrbEngine, evaluate_manual_confirmations, evaluate_orb_breakouts
from .reversal_engine import ReversalEngine, evaluate_reversal_setups
from .session_map_classifier import SessionMapClassifier, classify_session_map
from .signal_component_scorer import SignalComponentScorer, score_signal_components
from .signal_state_manager import (
    SignalStateManager,
    mark_signal_emitted,
    reset_confirmation_setup,
    reset_retest_setup,
    set_confirmation_setup,
    set_retest_setup,
    should_suppress_duplicate,
    update_confirmation_setup,
    update_retest_setup,
)
from .trade_signal_context_builder import TradeSignalContextBuilder, build_trade_signal_context
from .trend_leg_classifier import TrendLegClassifier, classify_trend_leg
from .watch_engine import WatchEngine, finalize_watch_state

__all__ = [
    "apply_day_state_adjustment",
    "apply_option_buyer_filters",
    "check_adx_filter",
    "check_multi_timeframe_filter",
    "check_oi_filter",
    "check_session_filter",
    "check_volume_filter",
    "ConfirmationEngine",
    "ContinuationEngine",
    "DayStateAnalyzer",
    "derive_active_day_state",
    "derive_opening_bias",
    "evaluate_aggressive_continuations",
    "evaluate_confirmation_and_retest",
    "evaluate_core_continuations",
    "evaluate_futures_acceptance",
    "evaluate_hybrid_continuations",
    "evaluate_manual_confirmations",
    "evaluate_opening_drive",
    "evaluate_orb_breakouts",
    "evaluate_reversal_setups",
    "finalize_no_setup",
    "finalize_watch_state",
    "HybridContinuationEngine",
    "NoSetupFinalizer",
    "OpeningDriveEngine",
    "OptionBuyerProtection",
    "OrbEngine",
    "ReversalEngine",
    "FuturesAcceptanceEngine",
    "SessionMapClassifier",
    "SignalComponentScorer",
    "SignalStateManager",
    "score_signal_components",
    "mark_signal_emitted",
    "reset_confirmation_setup",
    "reset_retest_setup",
    "set_confirmation_setup",
    "set_retest_setup",
    "should_suppress_duplicate",
    "TradeSignalContextBuilder",
    "TrendLegClassifier",
    "build_trade_signal_context",
    "classify_session_map",
    "classify_trend_leg",
    "update_confirmation_setup",
    "update_retest_setup",
    "WatchEngine",
]
