"""Runtime gap recovery helpers for SignalService."""


class RuntimeGapManager:
    @staticmethod
    def handle_runtime_gap_recovery(service, reason_label):
        service._log(f"🔄 Recovering after {reason_label}...")
        if service.last_processed_5m_ts:
            service._log(f"   Last processed candle preserved for backlog replay: {service.last_processed_5m_ts}")
        if service.data_pause_active:
            service.data_pause_active = False
            service.last_data_pause_reason = None
            service._log("   ✅ Reset data pause state")
        service.last_monitor_check_minute = None
        if service.active_trade_monitor:
            if service._active_trade_monitor_is_recoverable():
                service.active_trade_monitor["last_notified_minute"] = None
                service.active_trade_monitor["last_sent_monitor_minute"] = None
                service._log("   ✅ Preserved active trade monitor for fresh post-gap evaluation")
            else:
                service._log("   ⚠️  Clearing active trade monitor (expired during runtime gap)")
                service.active_trade_monitor = None
        if service.pending_entry_watch:
            if service._pending_watch_is_recoverable(service.pending_entry_watch):
                service.pending_entry_watch["last_checked_minute"] = None
                service._log("   ✅ Preserved pending entry watch for fresh 1m re-check")
            else:
                service._log("   ⚠️  Clearing pending entry watch (expired during runtime gap)")
                service.pending_entry_watch = None
        try:
            service._restore_indicator_state()
            service._log("   ✅ Restored indicator state from history")
        except Exception as e:
            service._log(f"   ⚠️  Could not restore indicators: {e}")
        service._log("🔄 Recovery complete. Resuming normal operation...")

    @staticmethod
    def handle_runtime_gap(service, gap_event):
        kind = gap_event["kind"]
        wall_gap = gap_event["wall_gap"]
        active_gap = gap_event["active_gap"]
        suspended_gap = gap_event["suspended_gap"]
        if kind == "system_sleep":
            service._log(
                "⚠️  SYSTEM SLEEP/WAKE DETECTED! "
                f"Wall gap: {wall_gap:.1f}s ({wall_gap/60:.1f} min) | "
                f"active runtime: {active_gap:.1f}s | suspended: {suspended_gap:.1f}s. "
                "Recovering..."
            )
            RuntimeGapManager.handle_runtime_gap_recovery(service, "system sleep/wake")
            return
        service._log(
            "⚠️  PROCESSING/FEED GAP DETECTED! "
            f"Wall gap: {wall_gap:.1f}s ({wall_gap/60:.1f} min) | "
            f"active runtime: {active_gap:.1f}s | suspended: {suspended_gap:.1f}s. "
            "This is not a confirmed system sleep. Recovering..."
        )
        RuntimeGapManager.handle_runtime_gap_recovery(service, "processing/feed gap")
