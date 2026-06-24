#!/usr/bin/env python3
"""Read-only local dashboard for OptionCoder."""

import argparse
import glob
import html
import json
import os
import re
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from email.utils import parsedate_to_datetime
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

sys.path.append(str(Path(__file__).resolve().parents[1]))

try:
    import yaml
except Exception:  # pragma: no cover - dashboard still works without profiles
    yaml = None

from config import Config
from shared.db.pool import DBPool
from tools.runtime_status import derive_status, load_json


ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"
INSTRUMENTS = ("NIFTY", "BANKNIFTY", "SENSEX")
NEWS_CACHE_TTL_SECONDS = 180
NEWS_CACHE = {"epoch": 0, "items": [], "errors": []}
MARKET_NEWS_FEEDS = (
    {
        "name": "ET Markets",
        "url": "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    },
    {
        "name": "Google News Index Options",
        "url": "https://news.google.com/rss/search?q=NIFTY%20BANKNIFTY%20SENSEX%20options%20when:1d&hl=en-IN&gl=IN&ceid=IN:en",
    },
    {
        "name": "Google News Macro Risk",
        "url": "https://news.google.com/rss/search?q=RBI%20SEBI%20rupee%20crude%20FII%20GIFT%20Nifty%20when:1d&hl=en-IN&gl=IN&ceid=IN:en",
    },
)


def to_jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


def strip_html(text):
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_date(value):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).isoformat()
    except Exception:
        return value


def news_impact(title, summary):
    text = f"{title} {summary}".lower()
    high_terms = (
        "rbi", "sebi", "fed", "inflation", "cpi", "crude", "oil", "war",
        "iran", "tariff", "fii", "fpi", "derivative", "gift nifty",
        "rupee", "usd/inr", "rate", "policy", "bank nifty",
    )
    medium_terms = (
        "nifty", "sensex", "banknifty", "bank nifty", "ipo", "earnings",
        "results", "global market", "dow", "nasdaq", "asia",
    )
    if any(term in text for term in high_terms):
        return "HIGH"
    if any(term in text for term in medium_terms):
        return "MEDIUM"
    return "LOW"


def news_tags(title, summary):
    text = f"{title} {summary}".lower()
    tags = []
    checks = (
        ("NIFTY", ("nifty", "gift nifty")),
        ("BANKNIFTY", ("bank nifty", "banknifty", "bank")),
        ("SENSEX", ("sensex", "bse")),
        ("RBI", ("rbi", "repo", "monetary policy")),
        ("SEBI", ("sebi",)),
        ("GLOBAL", ("fed", "dow", "nasdaq", "asia", "global", "us market")),
        ("CRUDE", ("crude", "oil", "brent")),
        ("FII", ("fii", "fpi", "foreign investor")),
        ("INR", ("rupee", "usd/inr", "dollar")),
    )
    for tag, terms in checks:
        if any(term in text for term in terms):
            tags.append(tag)
    return tags[:4]


def fetch_market_news():
    now = time.time()
    if now - NEWS_CACHE["epoch"] < NEWS_CACHE_TTL_SECONDS:
        return NEWS_CACHE

    items = []
    errors = []
    for feed in MARKET_NEWS_FEEDS:
        try:
            request = Request(
                feed["url"],
                headers={
                    "User-Agent": "OptionCoderLocalDashboard/1.0 (+local)",
                    "Accept": "application/rss+xml, application/xml, text/xml",
                },
            )
            with urlopen(request, timeout=4) as response:
                raw = response.read(1024 * 512)
            root = ElementTree.fromstring(raw)
            channel_items = root.findall(".//item")
            for item in channel_items[:12]:
                title = strip_html(item.findtext("title"))
                summary = strip_html(item.findtext("description"))
                link = item.findtext("link") or ""
                published = parse_date(item.findtext("pubDate"))
                source = item.findtext("source") or feed["name"]
                if not title:
                    continue
                items.append(
                    {
                        "feed": feed["name"],
                        "source": source,
                        "title": title,
                        "summary": summary[:260],
                        "link": link,
                        "published": published,
                        "impact": news_impact(title, summary),
                        "tags": news_tags(title, summary),
                    }
                )
        except Exception as exc:
            errors.append({"feed": feed["name"], "error": str(exc)})

    seen = set()
    deduped = []
    for item in items:
        key = (item["title"].lower(), item["source"].lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    def sort_key(item):
        impact_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(item["impact"], 3)
        return (impact_rank, item.get("published") or "")

    NEWS_CACHE.update(
        {
            "epoch": now,
            "items": sorted(deduped, key=sort_key)[:24],
            "errors": errors,
            "feeds": [feed["name"] for feed in MARKET_NEWS_FEEDS],
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    return NEWS_CACHE


def query_rows(query, params=()):
    if not DBPool.initialize():
        return []
    try:
        with DBPool.connection() as conn:
            if conn is None:
                return []
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
    except Exception as exc:
        return [{"error": str(exc)}]


def query_one(query, params=()):
    rows = query_rows(query, params)
    if rows and "error" not in rows[0]:
        return rows[0]
    return None


def normalize_instrument(value):
    instrument = (value or "NIFTY").upper()
    return instrument if instrument in INSTRUMENTS else "NIFTY"


def heartbeat_status():
    now = time.time()
    statuses = []
    for heartbeat_file in sorted(glob.glob(str(ROOT / "data/heartbeat/*.json"))):
        service_key = Path(heartbeat_file).stem
        state_file = ROOT / "data/watchdog" / f"{service_key.lower()}_state.json"
        heartbeat = load_json(heartbeat_file)
        raw_watchdog_state = load_json(str(state_file))
        result = derive_status(
            heartbeat,
            raw_watchdog_state or {"restart_epochs": []},
            now,
            watchdog_state_present=raw_watchdog_state is not None,
        )
        state = result.get("service_state") or {}
        statuses.append(
            {
                "service": service_key,
                "severity": result.get("severity"),
                "status": result.get("status"),
                "heartbeat_age": result.get("heartbeat_age"),
                "phase": state.get("phase"),
                "instrument": state.get("instrument"),
                "feed_connected": state.get("feed_connected"),
                "data_age_seconds": state.get("data_age_seconds"),
                "price": state.get("price"),
                "timestamp": result.get("timestamp"),
                "recent_restarts": len(result.get("recent_restarts") or []),
            }
        )
    return statuses


def load_risk_profiles():
    path = ROOT / "config/risk_profiles.yaml"
    if not yaml or not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}
    except Exception:
        return {}


def latest_candles(instrument):
    return query_rows(
        """
        SELECT ts, open, high, low, close, volume
        FROM candles_5m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 36;
        """,
        (instrument,),
    )


def latest_candles_1m(instrument):
    return query_rows(
        """
        SELECT ts, open, high, low, close, volume
        FROM candles_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 5;
        """,
        (instrument,),
    )


def latest_oi(instrument):
    return query_one(
        """
        SELECT
          ts, underlying_price, ce_oi, pe_oi, pcr, ce_oi_change, pe_oi_change,
          ce_volume, pe_volume, volume_pcr, oi_sentiment, oi_trend,
          support_level, resistance_level, data_quality, liquidity_score
        FROM oi_snapshots_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 1;
        """,
        (instrument,),
    )


def latest_option_band_pairs(instrument):
    return query_rows(
        """
        WITH latest_ts AS (
            SELECT DISTINCT ts
            FROM option_band_snapshots_1m
            WHERE instrument = %s
            ORDER BY ts DESC
            LIMIT 2
        )
        SELECT
          ts, atm_strike, strike, distance_from_atm, option_type,
          oi, volume, ltp, iv, spread, top_bid_price, top_ask_price,
          delta, gamma, theta, vega
        FROM option_band_snapshots_1m
        WHERE instrument = %s
          AND ts IN (SELECT ts FROM latest_ts)
          AND ABS(distance_from_atm) <= 5
        ORDER BY ts ASC, strike ASC, option_type ASC;
        """,
        (instrument, instrument),
    )


def recent_strategy_decisions(instrument):
    return query_rows(
        """
        SELECT
          ts, price, signal, reason, strategy_score, signal_quality, setup_type,
          tradability, time_regime, opening_bias, active_day_state,
          day_state_direction, oi_mode, pcr, strike, confidence_summary,
          entry_above, entry_below, invalidate_price, first_target_price
        FROM strategy_decisions_5m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 12;
        """,
        (instrument,),
    )


def recent_signals(instrument):
    return query_rows(
        """
        SELECT
          ts, instrument, signal, price, underlying_price, strike, atm_strike,
          option_entry_ltp, entry_bid, entry_ask, entry_spread, entry_iv,
          entry_delta, strategy_score, signal_quality, setup_type, tradability,
          time_regime, reason, confidence_summary, entry_above, entry_below,
          invalidate_price, first_target_price, telegram_sent, monitor_started
        FROM signals_issued
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 20;
        """,
        (instrument,),
    )


def option_candidates(instrument):
    return query_rows(
        """
        SELECT
          ts, candidate_direction, strike, atm_strike, distance_from_atm,
          option_ltp, bid_price, ask_price, spread, spread_percent, iv, delta,
          oi, volume, candidate_score, candidate_rank, expected_edge,
          selected_for_signal, reason
        FROM option_signal_candidates_5m
        WHERE instrument = %s
        ORDER BY ts DESC, candidate_rank ASC NULLS LAST
        LIMIT 18;
        """,
        (instrument,),
    )


def entry_decisions(instrument):
    return query_rows(
        """
        SELECT
          ts, watch_ts, direction, decision, underlying_price, trigger_price,
          invalidate_price, first_target_price, strike, option_ltp,
          option_spread, score, entry_score, signal_type, signal_grade,
          confidence, watch_bucket, option_buyer_action, reason
        FROM entry_decisions_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 16;
        """,
        (instrument,),
    )


def monitor_events(instrument):
    return query_rows(
        """
        SELECT
          ts, signal, entry_ts, entry_price, current_price, pnl_points,
          guidance, reason, structure_state, quality, time_regime, run_profile,
          runner_mode, dynamic_trail_pct, profit_lock_armed
        FROM trade_monitor_events_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 16;
        """,
        (instrument,),
    )


def latest_option_market_state(instrument):
    return query_rows(
        """
        SELECT
          ts, direction, underlying_price, atm_strike, strike, option_ltp,
          premium_change_1m, premium_change_3m, volume_delta, oi_delta, iv,
          spread, spread_percent, bid_price, ask_price, bid_quantity,
          ask_quantity, option_breadth_score, premium_state,
          liquidity_quality, recommended_action, reason
        FROM option_market_state_1m
        WHERE instrument = %s
        ORDER BY ts DESC
        LIMIT 4;
        """,
        (instrument,),
    )


def outcome_summary(instrument):
    return query_rows(
        """
        SELECT
          horizon_minutes,
          COUNT(*) AS sample_count,
          ROUND(AVG(pnl_points), 2) AS avg_points,
          ROUND(AVG(pnl_percent), 2) AS avg_percent,
          ROUND(100.0 * AVG(CASE WHEN pnl_points > 0 THEN 1 ELSE 0 END), 1) AS win_rate,
          ROUND(AVG(max_favorable_points), 2) AS avg_mfe,
          ROUND(AVG(max_adverse_points), 2) AS avg_mae
        FROM option_signal_horizon_outcomes
        WHERE instrument = %s
          AND signal_ts >= NOW() - INTERVAL '30 days'
        GROUP BY horizon_minutes
        ORDER BY horizon_minutes;
        """,
        (instrument,),
    )


def _age_minutes(ts_value):
    if not ts_value:
        return None
    if isinstance(ts_value, str):
        try:
            ts_value = datetime.fromisoformat(ts_value)
        except ValueError:
            return None
    if ts_value.tzinfo is None:
        return max(0.0, (datetime.now() - ts_value).total_seconds() / 60.0)
    return max(0.0, (datetime.now(ts_value.tzinfo) - ts_value).total_seconds() / 60.0)


def _as_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _direction_side(direction):
    direction = (direction or "").upper()
    if "PE" in direction or "PUT" in direction or "BEAR" in direction:
        return "PE"
    return "CE"


def build_gamma_blast_context(instrument, oi, candles_1m, band_rows, buyer_direction=None):
    latest_by_ts = {}
    for row in band_rows or []:
        if row.get("error"):
            continue
        latest_by_ts.setdefault(row.get("ts"), []).append(row)

    timestamps = sorted(latest_by_ts.keys())
    if len(timestamps) < 2:
        return {
            "state": "NO_DATA",
            "score": 0,
            "direction": _direction_side(buyer_direction),
            "summary": "Need two option-band snapshots for gamma radar.",
            "signals": [],
            "metrics": {},
        }

    prev_rows = latest_by_ts[timestamps[-2]]
    latest_rows = latest_by_ts[timestamps[-1]]
    prev_map = {(row["strike"], row["option_type"]): row for row in prev_rows}
    latest_map = {(row["strike"], row["option_type"]): row for row in latest_rows}

    directional_metrics = {}
    for direction in ("CE", "PE"):
        opposite = "PE" if direction == "CE" else "CE"
        price_breadth = 0
        volume_breadth = 0
        opposite_collapse = 0
        same_volume_total = 0
        same_price_total = 0.0
        max_premium_jump_pct = 0.0
        spread_values = []
        gamma_values = []
        examples = []

        for key, now_row in latest_map.items():
            strike, option_type = key
            if option_type != direction:
                continue
            prev_row = prev_map.get(key)
            opp_now = latest_map.get((strike, opposite))
            opp_prev = prev_map.get((strike, opposite))
            if not prev_row or not opp_now or not opp_prev:
                continue

            now_ltp = _as_float(now_row.get("ltp")) or 0.0
            prev_ltp = _as_float(prev_row.get("ltp")) or 0.0
            opp_now_ltp = _as_float(opp_now.get("ltp")) or 0.0
            opp_prev_ltp = _as_float(opp_prev.get("ltp")) or 0.0
            volume_delta = int((now_row.get("volume") or 0) - (prev_row.get("volume") or 0))
            price_delta = now_ltp - prev_ltp
            opp_delta = opp_now_ltp - opp_prev_ltp
            jump_pct = (price_delta / prev_ltp * 100.0) if prev_ltp > 0 else 0.0

            if price_delta > 0:
                price_breadth += 1
                same_price_total += price_delta
            if volume_delta > 0:
                volume_breadth += 1
                same_volume_total += volume_delta
            if opp_delta < 0:
                opposite_collapse += 1
            if jump_pct > max_premium_jump_pct:
                max_premium_jump_pct = jump_pct

            spread = _as_float(now_row.get("spread"))
            if spread is not None:
                spread_values.append(spread)
            gamma = _as_float(now_row.get("gamma"))
            if gamma is not None:
                gamma_values.append(gamma)
            if price_delta > 0 and volume_delta > 0 and len(examples) < 3:
                examples.append(f"{strike}{direction} +{round(price_delta, 1)} vol+{volume_delta}")

        directional_metrics[direction] = {
            "price_breadth": price_breadth,
            "volume_breadth": volume_breadth,
            "opposite_collapse": opposite_collapse,
            "same_volume_total": same_volume_total,
            "same_price_total": round(same_price_total, 2),
            "max_premium_jump_pct": round(max_premium_jump_pct, 2),
            "avg_spread": round(sum(spread_values) / len(spread_values), 2) if spread_values else None,
            "avg_gamma": round(sum(gamma_values) / len(gamma_values), 6) if gamma_values else None,
            "examples": examples,
        }

    direction = _direction_side(buyer_direction)
    if directional_metrics["PE"]["price_breadth"] > directional_metrics["CE"]["price_breadth"]:
        direction = "PE"
    elif directional_metrics["CE"]["price_breadth"] > directional_metrics["PE"]["price_breadth"]:
        direction = "CE"

    metrics = directional_metrics[direction]
    latest_candle = candles_1m[0] if candles_1m else {}
    previous_candle = candles_1m[1] if len(candles_1m or []) > 1 else {}
    liquidity_sweep = {
        "state": "NONE",
        "side": None,
        "level": None,
        "reclaim": False,
        "summary": "No fresh 1m sweep.",
    }
    prior_candles = list(reversed(candles_1m[1:5] if len(candles_1m or []) > 1 else []))
    if latest_candle and prior_candles:
        high = _as_float(latest_candle.get("high"))
        low = _as_float(latest_candle.get("low"))
        close = _as_float(latest_candle.get("close"))
        prior_high = max((_as_float(row.get("high")) or 0.0) for row in prior_candles)
        prior_low = min((_as_float(row.get("low")) or 0.0) for row in prior_candles)
        if None not in (high, low, close) and prior_high and prior_low:
            swept_high = high > prior_high and close < prior_high
            swept_low = low < prior_low and close > prior_low
            broke_high = high > prior_high and close >= prior_high
            broke_low = low < prior_low and close <= prior_low
            if swept_high:
                liquidity_sweep = {
                    "state": "UPSIDE_SWEEP_REJECTED",
                    "side": "PE",
                    "level": prior_high,
                    "reclaim": True,
                    "summary": f"High {round(high, 2)} swept prior {round(prior_high, 2)} and closed back below.",
                }
            elif swept_low:
                liquidity_sweep = {
                    "state": "DOWNSIDE_SWEEP_RECLAIMED",
                    "side": "CE",
                    "level": prior_low,
                    "reclaim": True,
                    "summary": f"Low {round(low, 2)} swept prior {round(prior_low, 2)} and closed back above.",
                }
            elif broke_high:
                liquidity_sweep = {
                    "state": "HIGH_BREAK_HOLDING",
                    "side": "CE",
                    "level": prior_high,
                    "reclaim": False,
                    "summary": f"Price broke prior high {round(prior_high, 2)} and is holding above.",
                }
            elif broke_low:
                liquidity_sweep = {
                    "state": "LOW_BREAK_HOLDING",
                    "side": "PE",
                    "level": prior_low,
                    "reclaim": False,
                    "summary": f"Price broke prior low {round(prior_low, 2)} and is holding below.",
                }

    candle_impulse = False
    if latest_candle and previous_candle:
        close = _as_float(latest_candle.get("close"))
        prev_close = _as_float(previous_candle.get("close"))
        high = _as_float(latest_candle.get("high"))
        low = _as_float(latest_candle.get("low"))
        open_price = _as_float(latest_candle.get("open"))
        if None not in (close, prev_close, high, low, open_price) and high > low:
            body_ratio = abs(close - open_price) / (high - low)
            close_strength = ((close - low) / (high - low)) if direction == "CE" else ((high - close) / (high - low))
            directional_close = close > prev_close if direction == "CE" else close < prev_close
            candle_impulse = directional_close and body_ratio >= 0.22 and close_strength >= 0.58

    wall_signal = "NEUTRAL"
    underlying = _as_float((oi or {}).get("underlying_price"))
    support = _as_float((oi or {}).get("support_level"))
    resistance = _as_float((oi or {}).get("resistance_level"))
    if direction == "CE" and underlying is not None and resistance is not None:
        distance = resistance - underlying
        wall_signal = "NEAR_RESISTANCE_BREAK" if distance >= 0 and distance <= 35 else "ABOVE_RESISTANCE" if distance < 0 else "AWAY"
    if direction == "PE" and underlying is not None and support is not None:
        distance = underlying - support
        wall_signal = "NEAR_SUPPORT_BREAK" if distance >= 0 and distance <= 35 else "BELOW_SUPPORT" if distance < 0 else "AWAY"

    score = 0
    signals = []
    if metrics["price_breadth"] >= 6:
        score += 22
        signals.append("cross-strike premium expansion")
    if metrics["volume_breadth"] >= 6:
        score += 18
        signals.append("volume breadth expanding")
    if metrics["opposite_collapse"] >= 6:
        score += 18
        signals.append("opposite option side collapsing")
    if metrics["max_premium_jump_pct"] >= 12:
        score += 14
        signals.append("fast premium repricing")
    if candle_impulse:
        score += 14
        signals.append("1m underlying impulse")
    if wall_signal in {"NEAR_RESISTANCE_BREAK", "ABOVE_RESISTANCE", "NEAR_SUPPORT_BREAK", "BELOW_SUPPORT"}:
        score += 14
        signals.append("OI wall break zone")
    if liquidity_sweep["state"] in {"UPSIDE_SWEEP_REJECTED", "DOWNSIDE_SWEEP_RECLAIMED"}:
        score += 18
        signals.append("liquidity sweep and reclaim/rejection")
    elif liquidity_sweep["state"] in {"HIGH_BREAK_HOLDING", "LOW_BREAK_HOLDING"}:
        score += 8
        signals.append("prior level break holding")

    score = min(score, 100)
    if score >= 76:
        state = "BLAST_ACTIVE"
    elif score >= 56:
        state = "BLAST_BUILDING"
    elif score >= 34:
        state = "EARLY_WATCH"
    else:
        state = "QUIET"

    return {
        "state": state,
        "score": score,
        "direction": direction,
        "wall_signal": wall_signal,
        "liquidity_sweep": liquidity_sweep,
        "candle_impulse": candle_impulse,
        "signals": signals,
        "metrics": metrics,
        "peer_metrics": directional_metrics,
        "summary": (
            f"{direction} {state}: breadth {metrics['price_breadth']}, "
            f"vol breadth {metrics['volume_breadth']}, opp collapse {metrics['opposite_collapse']}"
        ),
        "latest_ts": timestamps[-1],
    }


def build_buyer_context(instrument, signals, entries, candidates, market_states, risk_profile):
    latest_signal = signals[0] if signals else None
    latest_entry = entries[0] if entries else None
    selected_candidate = next((row for row in candidates if row.get("selected_for_signal")), None)
    top_candidate = selected_candidate or (candidates[0] if candidates else None)
    latest_market_state = market_states[0] if market_states else None

    signal_age = _age_minutes(latest_signal.get("ts")) if latest_signal else None
    entry_age = _age_minutes(latest_entry.get("ts")) if latest_entry else None

    spread_percent = _as_float(
        (latest_market_state or {}).get("spread_percent")
        or (top_candidate or {}).get("spread_percent")
    )
    premium_3m = _as_float((latest_market_state or {}).get("premium_change_3m"))
    entry_score = _as_float((latest_entry or {}).get("entry_score"))
    signal_score = _as_float((latest_signal or {}).get("strategy_score"))

    blockers = []
    cautions = []
    if not latest_signal and not latest_entry:
        blockers.append("No fresh buyer setup")
    if signal_age is not None and signal_age > Config.SIGNAL_VALIDITY_MINUTES:
        blockers.append(f"Signal age {signal_age:.1f}m > validity {Config.SIGNAL_VALIDITY_MINUTES}m")
    if entry_age is not None and entry_age > Config.ENTRY_TRIGGER_VALIDITY_MINUTES:
        blockers.append(f"Entry decision age {entry_age:.1f}m > trigger window {Config.ENTRY_TRIGGER_VALIDITY_MINUTES}m")
    if spread_percent is not None and spread_percent > Config.MAX_SPREAD_PERCENT:
        blockers.append(f"Spread {spread_percent:.2f}% > max {Config.MAX_SPREAD_PERCENT:.2f}%")
    if premium_3m is not None and premium_3m > Config.PREMIUM_CHASE_MAX_3M_PCT:
        blockers.append(f"Premium chase {premium_3m:.2f}% in 3m")
    if entry_score is not None and entry_score < Config.HIGH_PROB_MIN_ENTRY_SCORE:
        cautions.append(f"Entry score {entry_score:.0f} < high-prob {Config.HIGH_PROB_MIN_ENTRY_SCORE:.0f}")
    if signal_score is not None and signal_score < Config.HIGH_PROB_MIN_CONTEXT_SCORE:
        cautions.append(f"Context score {signal_score:.0f} < high-prob {Config.HIGH_PROB_MIN_CONTEXT_SCORE:.0f}")

    entry_decision = str((latest_entry or {}).get("decision") or "").upper()
    recommended_action = str((latest_market_state or {}).get("recommended_action") or "").upper()
    if "REJECT" in entry_decision or "AVOID" in recommended_action:
        blockers.append("Latest entry engine says avoid/reject")
    elif "WAIT" in entry_decision or "WATCH" in entry_decision:
        cautions.append("Entry is still in watch/wait state")

    readiness = "WAIT"
    if blockers:
        readiness = "AVOID"
    elif latest_entry and ("ACTION" in recommended_action or "BUY" in entry_decision or "ENTER" in entry_decision):
        readiness = "ACTIONABLE"
    elif latest_signal or latest_entry:
        readiness = "WATCH"

    stop_points = target_points = trail_points = None
    profile_block = risk_profile or {}
    for expiry_bucket in ("EXPIRY", "PRE_EXPIRY", "NON_EXPIRY", "POST_EXPIRY"):
        setup_profiles = profile_block.get(expiry_bucket) or {}
        for setup_type in ("BREAKOUT", "CONTINUATION", "REVERSAL"):
            normal = (setup_profiles.get(setup_type) or {}).get("NORMAL")
            if normal:
                stop_points = normal.get("sl")
                target_points = normal.get("target")
                trail_points = normal.get("trail")
                break
        if stop_points is not None:
            break

    return {
        "readiness": readiness,
        "blockers": blockers,
        "cautions": cautions,
        "latest_signal": latest_signal,
        "latest_entry": latest_entry,
        "top_candidate": top_candidate,
        "market_state": latest_market_state,
        "signal_age_minutes": signal_age,
        "entry_age_minutes": entry_age,
        "guardrails": {
            "signal_validity_minutes": Config.SIGNAL_VALIDITY_MINUTES,
            "entry_validity_minutes": Config.ENTRY_TRIGGER_VALIDITY_MINUTES,
            "max_spread_percent": Config.MAX_SPREAD_PERCENT,
            "premium_chase_max_2m_pct": Config.PREMIUM_CHASE_MAX_2M_PCT,
            "premium_chase_max_3m_pct": Config.PREMIUM_CHASE_MAX_3M_PCT,
            "min_context_score": Config.HIGH_PROB_MIN_CONTEXT_SCORE,
            "min_entry_score": Config.HIGH_PROB_MIN_ENTRY_SCORE,
            "min_premium": Config.PRO_TRADER_MIN_PREMIUM,
            "min_rr": Config.PRO_TRADER_MIN_RR,
        },
        "risk_plan": {
            "reference_profile": "first NORMAL risk profile in configured expiry order",
            "stop_points": stop_points,
            "target_points": target_points,
            "trail_points": trail_points,
            "rr": round(float(target_points) / float(stop_points), 2)
            if stop_points and target_points
            else None,
            "lot_size": Config.LOT_SIZE.get(instrument),
            "one_lot_stop_rupees": (
                round(float(stop_points) * float(Config.LOT_SIZE.get(instrument, 0)), 2)
                if stop_points and Config.LOT_SIZE.get(instrument)
                else None
            ),
            "one_lot_target_rupees": (
                round(float(target_points) * float(Config.LOT_SIZE.get(instrument, 0)), 2)
                if target_points and Config.LOT_SIZE.get(instrument)
                else None
            ),
        },
    }


def build_smart_money_guard(buyer, radar, decisions, market_states, oi):
    latest_decision = decisions[0] if decisions else {}
    latest_market = market_states[0] if market_states else {}
    sweep = (radar or {}).get("liquidity_sweep") or {}
    direction = (radar or {}).get("direction") or (buyer.get("latest_entry") or {}).get("direction")
    direction = _direction_side(direction)

    signal_age = buyer.get("signal_age_minutes")
    entry_age = buyer.get("entry_age_minutes")
    premium_state = str(latest_market.get("premium_state") or "").upper()
    liquidity_quality = str(latest_market.get("liquidity_quality") or "").upper()
    recommended_action = str(latest_market.get("recommended_action") or "").upper()
    tradability = str(latest_decision.get("tradability") or "").upper()
    signal_quality = str(latest_decision.get("signal_quality") or "").upper()
    pressure_conflict = str(latest_decision.get("pressure_conflict_level") or "").upper()
    time_regime = str(latest_decision.get("time_regime") or "").upper()
    oi_trend = str((oi or {}).get("oi_trend") or "").upper()
    oi_sentiment = str((oi or {}).get("oi_sentiment") or "").upper()

    trap_flags = []
    alignment_flags = []
    wait_flags = []

    if signal_age is not None and signal_age > Config.SIGNAL_VALIDITY_MINUTES:
        trap_flags.append("stale signal")
    if entry_age is not None and entry_age > Config.ENTRY_TRIGGER_VALIDITY_MINUTES:
        trap_flags.append("stale 1m entry")
    if sweep.get("state") in {"UPSIDE_SWEEP_REJECTED", "DOWNSIDE_SWEEP_RECLAIMED"} and sweep.get("side") != direction:
        trap_flags.append("sweep points opposite to chosen direction")
    if "CHASE" in premium_state or "FADING" in premium_state:
        trap_flags.append(f"premium {premium_state.lower()}")
    if "POOR" in liquidity_quality or "WIDE" in liquidity_quality:
        trap_flags.append(f"liquidity {liquidity_quality.lower()}")
    if pressure_conflict in {"STRONG", "SEVERE", "HIGH"}:
        trap_flags.append("pressure conflict high")
    if "REJECT" in recommended_action or "AVOID" in recommended_action:
        trap_flags.append("premium engine says avoid")

    if sweep.get("side") == direction and sweep.get("state") in {"UPSIDE_SWEEP_REJECTED", "DOWNSIDE_SWEEP_RECLAIMED"}:
        alignment_flags.append("liquidity sweep reclaimed in direction")
    if sweep.get("side") == direction and sweep.get("state") in {"HIGH_BREAK_HOLDING", "LOW_BREAK_HOLDING"}:
        alignment_flags.append("breakout/breakdown holding")
    if (radar or {}).get("score", 0) >= 56:
        alignment_flags.append("gamma radar building")
    if premium_state in {"EXPANDING", "CONFIRMED", "PREMIUM_OK"} or "ACTION" in recommended_action:
        alignment_flags.append("premium confirms move")
    if liquidity_quality in {"GOOD", "EXCELLENT"}:
        alignment_flags.append("liquidity acceptable")
    if signal_quality in {"A+", "A", "HIGH"} or tradability in {"ACTIONABLE", "TRADEABLE"}:
        alignment_flags.append("strategy quality acceptable")
    if direction == "CE" and (oi_trend == "BULLISH" or oi_sentiment == "BULLISH"):
        alignment_flags.append("OI supports CE")
    if direction == "PE" and (oi_trend == "BEARISH" or oi_sentiment == "BEARISH"):
        alignment_flags.append("OI supports PE")

    if not alignment_flags:
        wait_flags.append("no smart-money alignment yet")
    if time_regime in {"OPENING_VOLATILITY", "LUNCH_CHOP"}:
        wait_flags.append(f"time regime {time_regime.lower()}")
    if (radar or {}).get("state") in {"QUIET", "NO_DATA"}:
        wait_flags.append("gamma radar quiet")

    if trap_flags:
        state = "TRAP_RISK"
    elif len(alignment_flags) >= 3 and not wait_flags:
        state = "SMART_MONEY_ALIGNED"
    elif alignment_flags:
        state = "FOLLOW_THROUGH_PENDING"
    else:
        state = "NO_EDGE"

    score = max(0, min(100, (len(alignment_flags) * 18) - (len(trap_flags) * 22) - (len(wait_flags) * 8) + int((radar or {}).get("score", 0) * 0.25)))
    return {
        "state": state,
        "score": score,
        "direction": direction,
        "trap_flags": trap_flags,
        "alignment_flags": alignment_flags,
        "wait_flags": wait_flags,
        "rule": "Trade only when trap_flags empty, premium confirms, and sweep/gamma/structure align.",
        "summary": (
            trap_flags[0]
            if trap_flags
            else alignment_flags[0]
            if alignment_flags
            else wait_flags[0]
            if wait_flags
            else "No edge detected"
        ),
    }


def manual_focus_label(buyer, radar, smart_money=None):
    readiness = (buyer or {}).get("readiness")
    gamma_state = (radar or {}).get("state")
    gamma_score = int((radar or {}).get("score") or 0)
    sm_state = (smart_money or {}).get("state")
    if readiness == "AVOID":
        return "AVOID"
    if sm_state == "TRAP_RISK":
        return "AVOID"
    if sm_state == "SMART_MONEY_ALIGNED":
        return "FOCUS"
    if readiness == "ACTIONABLE" and gamma_score >= 56:
        return "FOCUS"
    if readiness in {"ACTIONABLE", "WATCH"} or gamma_state in {"BLAST_BUILDING", "BLAST_ACTIVE"}:
        return "WATCH"
    return "WAIT"


def build_manual_focus_queue(selected_instrument, selected_parts):
    queue = []
    for instrument in INSTRUMENTS:
        if instrument == selected_instrument:
            buyer = selected_parts["buyer"]
            radar = selected_parts["gamma_radar"]
            smart_money = selected_parts["smart_money"]
        else:
            risk_profile = selected_parts["profiles"].get(instrument, {})
            signals = recent_signals(instrument)
            entries = entry_decisions(instrument)
            candidates = option_candidates(instrument)
            market_states = latest_option_market_state(instrument)
            decisions = recent_strategy_decisions(instrument)
            oi = latest_oi(instrument)
            candles_1m = latest_candles_1m(instrument)
            radar = build_gamma_blast_context(
                instrument,
                oi,
                candles_1m,
                latest_option_band_pairs(instrument),
                entries[0].get("direction") if entries else signals[0].get("signal") if signals else None,
            )
            buyer = build_buyer_context(instrument, signals, entries, candidates, market_states, risk_profile)
            smart_money = build_smart_money_guard(buyer, radar, decisions, market_states, oi)

        blockers = buyer.get("blockers") or []
        cautions = buyer.get("cautions") or []
        risk_plan = buyer.get("risk_plan") or {}
        queue.append(
            # Reason priority is strict: hard blocker first, then smart-money state,
            # then softer buyer caution, then gamma context.
            # This keeps manual execution biased toward avoiding traps.
            {
                "instrument": instrument,
                "label": manual_focus_label(buyer, radar, smart_money),
                "readiness": buyer.get("readiness"),
                "gamma_state": radar.get("state"),
                "gamma_score": radar.get("score"),
                "smart_money_state": smart_money.get("state"),
                "smart_money_score": smart_money.get("score"),
                "direction": radar.get("direction") or (buyer.get("latest_entry") or {}).get("direction"),
                "sweep": (radar.get("liquidity_sweep") or {}).get("state"),
                "reason": (
                    blockers[0]
                    if blockers
                    else smart_money.get("summary")
                    or (cautions[0] if cautions else None)
                    or radar.get("summary")
                ),
                "lot_size": risk_plan.get("lot_size"),
                "one_lot_stop_rupees": risk_plan.get("one_lot_stop_rupees"),
                "one_lot_target_rupees": risk_plan.get("one_lot_target_rupees"),
            }
        )

    rank = {"FOCUS": 0, "WATCH": 1, "WAIT": 2, "AVOID": 3}
    return sorted(queue, key=lambda row: (rank.get(row["label"], 4), -int(row.get("gamma_score") or 0)))


def build_pro_buyer_discipline(buyer, radar, smart_money, news):
    market_state = buyer.get("market_state") or {}
    guardrails = buyer.get("guardrails") or {}
    latest_entry = buyer.get("latest_entry") or {}

    fomo_flags = []
    quality_flags = []
    no_trade_flags = []

    premium_3m = _as_float(market_state.get("premium_change_3m"))
    premium_1m = _as_float(market_state.get("premium_change_1m"))
    spread_pct = _as_float(market_state.get("spread_percent"))
    entry_score = _as_float(latest_entry.get("entry_score"))
    gamma_score = _as_float((radar or {}).get("score")) or 0.0
    high_impact_news = [
        item for item in (news or {}).get("items", [])[:8]
        if item.get("impact") == "HIGH"
    ]

    if premium_3m is not None and premium_3m > guardrails.get("premium_chase_max_3m_pct", Config.PREMIUM_CHASE_MAX_3M_PCT):
        fomo_flags.append("3m premium already stretched")
    if premium_1m is not None and premium_1m > Config.PREMIUM_CHASE_MAX_2M_PCT:
        fomo_flags.append("1m premium impulse may be late")
    if spread_pct is not None and spread_pct > Config.MAX_SPREAD_PERCENT:
        fomo_flags.append("spread too wide for clean manual fill")
    if buyer.get("signal_age_minutes") is not None and buyer["signal_age_minutes"] > Config.SIGNAL_VALIDITY_MINUTES:
        no_trade_flags.append("signal expired")
    if buyer.get("entry_age_minutes") is not None and buyer["entry_age_minutes"] > Config.ENTRY_TRIGGER_VALIDITY_MINUTES:
        no_trade_flags.append("1m entry expired")
    if high_impact_news:
        quality_flags.append("high-impact news awareness required")

    if smart_money.get("state") == "SMART_MONEY_ALIGNED":
        quality_flags.append("smart-money alignment")
    if gamma_score >= 56:
        quality_flags.append("gamma participation")
    if (radar.get("liquidity_sweep") or {}).get("reclaim"):
        quality_flags.append("sweep and reclaim/reject")
    if entry_score is not None and entry_score >= Config.HIGH_PROB_MIN_ENTRY_SCORE:
        quality_flags.append("entry score passes")
    if not buyer.get("blockers"):
        quality_flags.append("no buyer blocker")

    fomo_score = min(100, len(fomo_flags) * 28 + len(no_trade_flags) * 35)
    setup_score = max(0, min(100, len(quality_flags) * 18 + int(gamma_score * 0.2) - fomo_score))
    if no_trade_flags or smart_money.get("state") == "TRAP_RISK":
        verdict = "NO_TRADE"
    elif setup_score >= 72 and not fomo_flags:
        verdict = "A_PLUS_ONLY"
    elif setup_score >= 48:
        verdict = "SMALL_SIZE_WATCH"
    else:
        verdict = "WAIT"

    return {
        "verdict": verdict,
        "setup_score": setup_score,
        "fomo_score": fomo_score,
        "quality_flags": quality_flags,
        "fomo_flags": fomo_flags,
        "no_trade_flags": no_trade_flags,
        "rules": [
            "No trade if signal/entry is stale.",
            "No chase after stretched premium unless gamma + volume breadth confirms.",
            "Manual entry only after spread is acceptable.",
            "Prefer sweep reclaim/reject or break-hold with premium confirmation.",
            "Size from risk calculator, not conviction.",
        ],
    }


def _coach_action_from_guidance(guidance):
    guidance = (guidance or "").upper()
    if guidance == "HOLD_STRONG":
        return "HOLD"
    if guidance == "HOLD_WITH_TRAIL":
        return "HOLD_TRAIL"
    if guidance in {"BOOK_PARTIAL", "TRIM"}:
        return "BOOK_PARTIAL"
    if guidance in {"EXIT_PROFIT_PROTECT", "EXIT_PROTECT"}:
        return "EXIT_PROTECT"
    if guidance in {"EXIT_BIAS", "EXIT_STOPLOSS", "EXIT_TIMESTOP", "EXIT_TRAIL", "THESIS_BROKEN"}:
        return "EXIT_NOW"
    if guidance in {"THESIS_WEAKENING", "MOMENTUM_PAUSE", "TIME_DECAY_RISK"}:
        return "WATCH_CLOSELY"
    return "MONITOR"


def build_assumed_trade_coach(buyer, radar, smart_money, discipline, monitor_rows, market_states):
    latest_entry = buyer.get("latest_entry") or {}
    latest_signal = buyer.get("latest_signal") or {}
    latest_market = market_states[0] if market_states else {}
    latest_monitor = monitor_rows[0] if monitor_rows else {}

    direction = latest_entry.get("direction") or latest_signal.get("signal") or radar.get("direction")
    strike = latest_entry.get("strike") or latest_signal.get("strike") or (buyer.get("top_candidate") or {}).get("strike")
    assumed_entry = (
        _as_float(latest_entry.get("option_ltp"))
        or _as_float(latest_signal.get("option_entry_ltp"))
        or _as_float((buyer.get("top_candidate") or {}).get("option_ltp"))
        or _as_float(latest_market.get("option_ltp"))
    )
    current_premium = (
        _as_float(latest_monitor.get("current_price"))
        or _as_float(latest_market.get("option_ltp"))
        or assumed_entry
    )
    pnl_points = (
        _as_float(latest_monitor.get("pnl_points"))
        if latest_monitor.get("pnl_points") is not None
        else (round(current_premium - assumed_entry, 2) if current_premium is not None and assumed_entry is not None else None)
    )
    pnl_pct = (
        round((pnl_points / assumed_entry) * 100.0, 2)
        if pnl_points is not None and assumed_entry not in {None, 0}
        else None
    )

    guidance = latest_monitor.get("guidance")
    action = _coach_action_from_guidance(guidance)
    reason = latest_monitor.get("reason") or "No live monitor event yet; using premium/risk guard."

    stop_points = _as_float((buyer.get("risk_plan") or {}).get("stop_points")) or 0.0
    target_points = _as_float((buyer.get("risk_plan") or {}).get("target_points")) or 0.0
    trail_points = _as_float((buyer.get("risk_plan") or {}).get("trail_points")) or 0.0
    hard_stop = round(assumed_entry - stop_points, 2) if assumed_entry is not None and stop_points else None
    first_target = round(assumed_entry + target_points, 2) if assumed_entry is not None and target_points else None
    trail_floor = None

    if pnl_points is not None and assumed_entry is not None:
        if pnl_points <= -stop_points and stop_points:
            action = "EXIT_NOW"
            reason = "Hard stop reached on assumed premium."
        elif target_points and pnl_points >= target_points:
            action = "HOLD_TRAIL"
            reason = "Target zone reached; avoid early exit, trail the winner."
            trail_floor = round(max(assumed_entry, current_premium - max(trail_points, target_points * 0.35)), 2)
        elif pnl_points > 0 and pnl_points < max(target_points * 0.45, 1):
            if action in {"MONITOR", "WATCH_CLOSELY"}:
                action = "HOLD"
                reason = "Small profit only; do not exit just because it flickered green."
        elif pnl_points is not None and pnl_points < 0 and smart_money.get("state") == "TRAP_RISK":
            action = "EXIT_NOW"
            reason = "Loss plus trap-risk context; do not hope."

    if discipline.get("verdict") == "NO_TRADE" and not latest_monitor:
        action = "NO_TRADE"
        reason = "No valid assumed trade; discipline verdict is NO_TRADE."

    alert = False
    alert_text = None
    if discipline.get("verdict") == "A_PLUS_ONLY" and smart_money.get("state") == "SMART_MONEY_ALIGNED":
        alert = True
        alert_text = f"BUY {direction} {strike} only near planned premium; no chase."
    elif discipline.get("verdict") == "SMALL_SIZE_WATCH":
        alert = True
        alert_text = f"WATCH {direction} {strike}; wait for clean fill and premium confirmation."

    return {
        "alert": alert,
        "alert_text": alert_text,
        "action": action,
        "direction": direction,
        "strike": strike,
        "assumed_entry": assumed_entry,
        "current_premium": current_premium,
        "pnl_points": pnl_points,
        "pnl_pct": pnl_pct,
        "hard_stop": hard_stop,
        "first_target": first_target,
        "trail_floor": trail_floor,
        "monitor_guidance": guidance,
        "reason": reason,
        "updated_at": latest_monitor.get("ts") or latest_market.get("ts"),
        "cadence_seconds": 30,
        "rules": [
            "Do not exit a green trade before first target unless thesis breaks.",
            "After target zone, trail premium instead of guessing top.",
            "Exit immediately on hard stop or thesis broken.",
            "If premium fades while smart-money state flips to trap risk, get out.",
        ],
    }


def dashboard_payload(instrument):
    profiles = load_risk_profiles().get("risk_profiles", {})
    risk_profile = profiles.get(instrument, {})
    signals = recent_signals(instrument)
    entries = entry_decisions(instrument)
    candidates = option_candidates(instrument)
    market_states = latest_option_market_state(instrument)
    decisions = recent_strategy_decisions(instrument)
    monitor_rows = monitor_events(instrument)
    oi = latest_oi(instrument)
    candles_1m = latest_candles_1m(instrument)
    band_rows = latest_option_band_pairs(instrument)
    buyer_direction = None
    if entries:
        buyer_direction = entries[0].get("direction")
    elif signals:
        buyer_direction = signals[0].get("signal")
    gamma_radar = build_gamma_blast_context(instrument, oi, candles_1m, band_rows, buyer_direction)
    buyer_context = build_buyer_context(instrument, signals, entries, candidates, market_states, risk_profile)
    smart_money = build_smart_money_guard(buyer_context, gamma_radar, decisions, market_states, oi)
    news = fetch_market_news()
    discipline = build_pro_buyer_discipline(buyer_context, gamma_radar, smart_money, news)
    trade_coach = build_assumed_trade_coach(
        buyer_context,
        gamma_radar,
        smart_money,
        discipline,
        monitor_rows,
        market_states,
    )
    gamma_overview = []
    for overview_instrument in INSTRUMENTS:
        overview_oi = oi if overview_instrument == instrument else latest_oi(overview_instrument)
        overview_candles_1m = candles_1m if overview_instrument == instrument else latest_candles_1m(overview_instrument)
        overview_band_rows = band_rows if overview_instrument == instrument else latest_option_band_pairs(overview_instrument)
        overview_radar = (
            gamma_radar
            if overview_instrument == instrument
            else build_gamma_blast_context(overview_instrument, overview_oi, overview_candles_1m, overview_band_rows)
        )
        gamma_overview.append(
            {
                "instrument": overview_instrument,
                "state": overview_radar.get("state"),
                "score": overview_radar.get("score"),
                "direction": overview_radar.get("direction"),
                "summary": overview_radar.get("summary"),
                "wall_signal": overview_radar.get("wall_signal"),
            }
        )

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "instrument": instrument,
        "instruments": INSTRUMENTS,
        "runtime": heartbeat_status(),
        "market": {
            "oi": oi,
            "candles_5m": list(reversed(latest_candles(instrument))),
            "candles_1m": list(reversed(candles_1m)),
        },
        "strategy": {
            "decisions": decisions,
            "signals": signals,
            "candidates": candidates,
            "entries": entries,
            "monitor": monitor_rows,
            "outcomes": outcome_summary(instrument),
            "market_states": market_states,
        },
        "buyer": buyer_context,
        "gamma_radar": gamma_radar,
        "smart_money": smart_money,
        "gamma_overview": gamma_overview,
        "manual_focus": build_manual_focus_queue(
            instrument,
            {
                "buyer": buyer_context,
                "gamma_radar": gamma_radar,
                "smart_money": smart_money,
                "profiles": profiles,
            },
        ),
        "discipline": discipline,
        "trade_coach": trade_coach,
        "news": news,
        "risk_profile": risk_profile,
        "mode": {
            "paper_trade": Config.PAPER_TRADE,
            "test_mode": Config.TEST_MODE,
            "mock_data": Config.USE_MOCK_DATA,
            "high_prob_action_only": Config.HIGH_PROB_ACTION_ONLY,
            "require_1m_execution": Config.REQUIRE_1M_EXECUTION_FOR_ACTION,
            "min_score_threshold": Config.MIN_SCORE_THRESHOLD,
            "max_spread_percent": Config.MAX_SPREAD_PERCENT,
        },
    }


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def log_message(self, format, *args):
        return

    def send_json(self, payload, status=200):
        body = json.dumps(to_jsonable(payload), separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/dashboard":
            params = parse_qs(parsed.query)
            instrument = normalize_instrument((params.get("instrument") or ["NIFTY"])[0])
            self.send_json(dashboard_payload(instrument))
            return
        if parsed.path == "/health":
            self.send_json({"ok": True, "generated_at": datetime.now().isoformat(timespec="seconds")})
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()


def main():
    parser = argparse.ArgumentParser(description="Run the OptionCoder local dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"OptionCoder dashboard running at http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard")
    finally:
        DBPool.close_all()
        server.server_close()


if __name__ == "__main__":
    main()
