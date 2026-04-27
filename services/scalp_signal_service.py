"""
Scalp Signal Service - Fast 1-minute signals
Generates quick scalping signals with 3-5 minute hold time
"""

import time as time_module
import sys
import os
import argparse
from datetime import timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.time_utils import TimeUtils
from shared.utils.log_utils import log_with_timestamp
from config import Config, get_config_for_instrument, get_scalp_config_for_instrument
from shared.db.reader import DBReader
from shared.db.writer import DBWriter
from shared.indicators.vwap import VWAPCalculator
from shared.indicators.volume_analyzer import VolumeAnalyzer
from shared.market.oi_analyzer import OIAnalyzer
from strategies.shared.scalp_strategy import ScalpStrategy
from shared.utils.instrument_profile import get_instrument_profile
from shared.utils.runtime_gap_detector import RuntimeGapDetector
from shared.utils.service_watchdog import ServiceWatchdog


class ScalpSignalService:
    """Fast scalping signal generation on 1-minute candles"""

    def __init__(self, instrument=None):
        self.time_utils = TimeUtils()
        self.profile = get_instrument_profile(instrument)
        self.instrument = self.profile["instrument"]
        self.watchdog = ServiceWatchdog("scalp_signal_service", self.instrument)

        # Get instrument-specific config
        self.config = get_config_for_instrument(self.instrument)
        self.scalp_config = get_scalp_config_for_instrument(self.instrument)

        # Database access
        self.db_reader = DBReader()
        self.db_writer = DBWriter()

        # Indicators
        self.vwap = VWAPCalculator()
        self.volume = VolumeAnalyzer()
        self.oi = OIAnalyzer()

        # Strategy with instrument-specific settings
        self.strategy = ScalpStrategy(
            min_score=self.scalp_config['min_score'],
            min_atr=self.scalp_config['min_atr'],
            require_atr=True
        )

        # Tracking
        self.running = False
        self.last_processed_1m_ts = None
        self.signals_generated = 0
        self.last_signal_time = None
        self.signal_cooldown = 0
        self.last_candle = None
        self.data_pause_active = False
        self.last_data_pause_reason = None

        # Intervals
        self.heartbeat_interval = 30
        self.last_heartbeat = 0
        self.cooldown_seconds = self.scalp_config['cooldown']  # Use config cooldown
        self.runtime_gap_detector = RuntimeGapDetector(
            threshold_seconds=20,
            sleep_confirmation_seconds=10,
        )

    def _rebuild_intraday_context(self, recent_candles):
        """Rebuild VWAP and volume context from recent 1m candles."""
        self.vwap.reset()
        self.volume.reset()

        if not recent_candles:
            return None, "NO_DATA"

        vwap_value = None
        for candle in recent_candles:
            vwap_value = self.vwap.update(candle)

        # Use prior candles only for the current-candle volume baseline.
        for candle in recent_candles[:-1]:
            self.volume.update(candle)

        current_volume = recent_candles[-1]["volume"]
        volume_signal = self.volume.get_volume_signal(current_volume)
        if volume_signal == "NO_DATA":
            volume_signal = "NORMAL"

        return vwap_value, volume_signal

    def _log(self, message):
        """Log with timestamp"""
        log_with_timestamp(f"[Scalp {self.instrument}] {message}")

    def _get_current_price(self):
        """Get current price from latest 1m candle"""
        candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=1)
        if candles:
            return candles[0]["close"]
        return None

    def _check_data_health(self):
        """Check if 1m data is fresh"""
        try:
            candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=1)
            if not candles:
                return False, "No 1m candles"

            latest = candles[0]
            current_time = self.time_utils.now_ist()
            age = current_time - latest["time"]

            if age > timedelta(minutes=2):
                return False, f"1m candle stale ({age.seconds}s old)"

            return True, "OK"
        except Exception as e:
            return False, f"Data check error: {e}"

    def _handle_runtime_gap_recovery(self, reason_label):
        self._log(f"🔄 Recovering after {reason_label}...")
        self.last_processed_1m_ts = None

        if self.data_pause_active:
            self.data_pause_active = False
            self.last_data_pause_reason = None
            self._log("   ✅ Reset data pause state")

        try:
            recent_candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=30)
            if recent_candles:
                vwap_value, volume_signal = self._rebuild_intraday_context(recent_candles)
                self._log(
                    "   ✅ Restored 1m context "
                    f"| candles: {len(recent_candles)} | "
                    f"VWAP: {round(vwap_value, 2) if vwap_value else 'NA'} | "
                    f"Volume: {volume_signal}"
                )
        except Exception as e:
            self._log(f"   ⚠️  Could not rebuild 1m context: {e}")

        self._log("🔄 Recovery complete. Resuming scalp loop...")

    def _handle_runtime_gap(self, gap_event):
        kind = gap_event["kind"]
        wall_gap = gap_event["wall_gap"]
        active_gap = gap_event["active_gap"]
        suspended_gap = gap_event["suspended_gap"]

        if kind == "system_sleep":
            self._log(
                "⚠️  SYSTEM SLEEP/WAKE DETECTED! "
                f"Wall gap: {wall_gap:.1f}s ({wall_gap/60:.1f} min) | "
                f"active runtime: {active_gap:.1f}s | suspended: {suspended_gap:.1f}s. "
                "Recovering..."
            )
            self._handle_runtime_gap_recovery("system sleep/wake")
            return

        self._log(
            "⚠️  PROCESSING/FEED GAP DETECTED! "
            f"Wall gap: {wall_gap:.1f}s ({wall_gap/60:.1f} min) | "
            f"active runtime: {active_gap:.1f}s | suspended: {suspended_gap:.1f}s. "
            "This is not a confirmed system sleep. Recovering..."
        )
        self._handle_runtime_gap_recovery("processing/feed gap")

    def _process_1m_candle(self, candle_1m):
        """Process 1-minute candle and generate scalp signal"""
        try:
            price = candle_1m["close"]

            # Get recent candles for volume and ATR analysis
            recent_candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=14)
            if not recent_candles:
                self._log("Skipping 1m candle - no recent candles available")
                return None

            # Make sure the newest candle participates in intraday context.
            if recent_candles[-1]["time"] != candle_1m["time"]:
                recent_candles.append(candle_1m)

            vwap_value, volume_signal = self._rebuild_intraday_context(recent_candles)
            if vwap_value is None or vwap_value <= 0:
                self._log("Skipping 1m candle - VWAP not ready")
                return None
            
            # Calculate ATR (simplified - using last 14 candles)
            atr_value = 0
            if len(recent_candles) >= 5:
                true_ranges = []
                for i in range(1, min(len(recent_candles), 14)):
                    high = float(recent_candles[i]["high"])
                    low = float(recent_candles[i]["low"])
                    prev_close = float(recent_candles[i-1]["close"])
                    tr1 = high - low
                    tr2 = abs(high - prev_close)
                    tr3 = abs(low - prev_close)
                    true_ranges.append(max(tr1, tr2, tr3))
                atr_value = sum(true_ranges) / len(true_ranges) if true_ranges else 0
            
            # Add ATR to candle for strategy filtering
            candle_1m["atr"] = atr_value

            # Get OI bias from latest OI snapshot
            oi_snapshot = self.db_reader.fetch_latest_oi_snapshot(self.instrument)
            oi_bias = "NEUTRAL"
            oi_trend = "SIDEWAYS"
            if oi_snapshot:
                ce_oi = oi_snapshot.get("ce_oi", 0)
                pe_oi = oi_snapshot.get("pe_oi", 0)
                if ce_oi > 0 and pe_oi > 0:
                    if pe_oi > ce_oi * 1.1:
                        oi_bias = "BULLISH"
                    elif ce_oi > pe_oi * 1.1:
                        oi_bias = "BEARISH"

            # Generate scalp signal
            signal, score, reason = self.strategy.generate_signal(
                price=price,
                candle_1m=candle_1m,
                vwap=vwap_value,
                oi_bias=oi_bias,
                oi_trend=oi_trend,
                volume_signal=volume_signal,
            )

            if signal:
                # Check cooldown
                if self.last_signal_time:
                    time_since_last = (self.time_utils.now_ist() - self.last_signal_time).total_seconds()
                    if time_since_last < self.cooldown_seconds:
                        self._log(f"Signal {signal} blocked - cooldown {int(self.cooldown_seconds - time_since_last)}s remaining")
                        return None

                self._log(f"🚨 SCALP SIGNAL: {signal} | Score: {score} | Price: {price}")
                self._log(f"   Reason: {reason}")

                # Save to database
                self._save_scalp_signal(candle_1m["time"], signal, price, score, reason)

                self.signals_generated += 1
                self.last_signal_time = self.time_utils.now_ist()

                return signal

            else:
                # Log why signal rejected (low score, etc)
                if score > 20:  # Only log if close to threshold
                    self._log(f"No scalp signal | Score: {score} | {reason}")

            return None

        except Exception as e:
            self._log(f"Error processing 1m candle: {e}")
            return None

    def _save_scalp_signal(self, ts, signal, price, score, reason):
        """Save scalp signal to database"""
        try:
            # Calculate target and stop loss (instrument-specific)
            target_points = self.scalp_config['target']
            stop_points = self.scalp_config['stop']
            
            if signal == "CE":
                target = price + target_points
                stop_loss = price - stop_points
            else:  # PE
                target = price - target_points
                stop_loss = price + stop_points

            row = (
                ts,
                self.instrument,
                signal,
                float(price),
                float(target),
                float(stop_loss),
                int(score),
                reason,
                "ACTIVE",  # status
                None,  # exit_ts
                None,  # exit_price
                None,  # pnl
            )
            self.db_writer.insert_scalp_signal(row)
        except Exception as e:
            self._log(f"DB save error (scalp signal): {e}")

    def _print_heartbeat(self):
        """Print heartbeat status"""
        current_time = self.time_utils.now_ist()
        print(f"\n[Scalp Service] Heartbeat | IST: {current_time.strftime('%H:%M:%S')} | "
              f"Status: {'✅ RUNNING' if self.running else '❌ STOPPED'} | "
              f"Signals: {self.signals_generated} | "
              f"Last: {self.last_signal_time.strftime('%H:%M') if self.last_signal_time else 'None'}")

    def run(self):
        """Main scalping loop"""
        self.running = True
        self.watchdog.start({"phase": "starting"})

        self._log("🚀 Starting Scalp Signal Service")
        self._log(f"   Instrument: {self.instrument}")
        self._log(f"   Min Score: {self.scalp_config['min_score']}")
        self._log(f"   Target: {self.scalp_config['target']} pts")
        self._log(f"   Stop: {self.scalp_config['stop']} pts")
        self._log(f"   Min ATR: {self.scalp_config['min_atr']}")
        self._log(f"   Cooldown: {self.scalp_config['cooldown']}s")

        # Warm up the 1m-derived indicators so the first live candle has context.
        recent_candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=30)
        if recent_candles:
            vwap_value, volume_signal = self._rebuild_intraday_context(recent_candles)
            self._log(
                f"   Warmup: {len(recent_candles)} candles | VWAP: {round(vwap_value, 2) if vwap_value else 'NA'} | Volume: {volume_signal}"
            )

        try:
            loop_count = 0
            while self.running:
                loop_count += 1

                gap_event = self.runtime_gap_detector.check()
                if gap_event:
                    self._handle_runtime_gap(gap_event)

                # Check data health
                data_ok, pause_reason = self._check_data_health()
                if not data_ok:
                    if self.last_data_pause_reason != pause_reason:
                        self._log(f"Pausing: {pause_reason}")
                        self.last_data_pause_reason = pause_reason
                    self.data_pause_active = True
                    self.watchdog.touch({"phase": "data_pause", "reason": pause_reason})
                    time_module.sleep(10)
                    continue

                if self.data_pause_active:
                    self._log("Data healthy - resuming")
                    self.data_pause_active = False
                    self.last_data_pause_reason = None
                    self.watchdog.touch({"phase": "resumed"})

                # Get latest 1m candle
                candles = self.db_reader.fetch_recent_candles_1m(self.instrument, limit=1)
                if not candles:
                    time_module.sleep(5)
                    continue

                latest = candles[0]
                current_time = self.time_utils.now_ist()

                # Check if new candle (1m buffer)
                candle_time = latest["time"]
                is_new = (current_time - candle_time < timedelta(minutes=1.5) and
                         candle_time != self.last_processed_1m_ts)

                if is_new:
                    self._process_1m_candle(latest)
                    self.last_processed_1m_ts = candle_time

                # Heartbeat
                current_epoch = time_module.time()
                if current_epoch - self.last_heartbeat >= self.heartbeat_interval:
                    self._print_heartbeat()
                    self.last_heartbeat = current_epoch
                    self.watchdog.touch({
                        "phase": "heartbeat",
                        "signals": self.signals_generated,
                        "last_signal": self.last_signal_time.isoformat() if self.last_signal_time else None,
                    })

                time_module.sleep(3)  # Check every 3 seconds

        except KeyboardInterrupt:
            self._log("Shutdown requested")
        except Exception as e:
            self._log(f"Fatal error: {e}")
        finally:
            self.running = False
            self.watchdog.stop()
            self._log("Stopped")

    def stop(self):
        """Stop the service"""
        self.running = False


def main():
    parser = argparse.ArgumentParser(description="Scalp Signal Service - Fast 1m signals")
    parser.add_argument("--instrument", default="NIFTY", help="Instrument to trade")
    args = parser.parse_args()

    service = ScalpSignalService(instrument=args.instrument)
    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()


if __name__ == "__main__":
    main()
