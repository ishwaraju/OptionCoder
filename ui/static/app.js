const state = {
  instrument: localStorage.getItem("optioncoder.instrument") || "NIFTY",
  instruments: ["NIFTY", "BANKNIFTY", "SENSEX"],
  refreshSeconds: 15,
  nextRefreshAt: Date.now() + 15000,
  previousGamma: new Map(),
  latestPayload: null,
  dismissedAlertKey: null,
};

const el = (id) => document.getElementById(id);

function fmt(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return number.toLocaleString("en-IN", {
    maximumFractionDigits: digits,
    minimumFractionDigits: Number.isInteger(number) ? 0 : Math.min(digits, 2),
  });
}

function timeOnly(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleTimeString("en-IN", {
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "Asia/Kolkata",
  });
}

function dateTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleString("en-IN", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "Asia/Kolkata",
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function badgeClass(value) {
  const text = String(value || "").toUpperCase();
  if (["OK", "HEALTHY", "GOOD", "ACTION", "BUY", "CE", "CALL"].some((part) => text.includes(part))) return "ok";
  if (["WARN", "STALE", "WATCH", "WAIT", "NEUTRAL"].some((part) => text.includes(part))) return "warn";
  if (["ALERT", "ERROR", "BAD", "NO_", "REJECT", "PUT", "PE"].some((part) => text.includes(part))) return "bad";
  return "";
}

function sideClass(value) {
  const text = String(value || "").toUpperCase();
  if (text.includes("CE") || text.includes("CALL") || text.includes("BULL")) return "call";
  if (text.includes("PE") || text.includes("PUT") || text.includes("BEAR")) return "put";
  return badgeClass(value);
}

function renderTabs() {
  el("instrument-tabs").innerHTML = state.instruments
    .map(
      (instrument) =>
        `<button class="${instrument === state.instrument ? "active" : ""}" data-instrument="${instrument}">${instrument}</button>`,
    )
    .join("");
}

function metric(label, value) {
  return `<div class="metric"><strong>${escapeHtml(value)}</strong><span>${escapeHtml(label)}</span></div>`;
}

function statusPill(label, value, className = "") {
  return `<div class="status-pill"><strong class="${className}">${escapeHtml(value)}</strong><span>${escapeHtml(label)}</span></div>`;
}

function readinessClass(value) {
  const text = String(value || "").toUpperCase();
  if (text === "ACTIONABLE") return "ok";
  if (text === "AVOID") return "bad";
  return "warn";
}

function renderMode(payload) {
  const mode = payload.mode || {};
  el("mode-strip").innerHTML = [
    statusPill("Paper Trade", mode.paper_trade ? "ON" : "OFF"),
    statusPill("Test Mode", mode.test_mode ? "ON" : "OFF"),
    statusPill("Mock Data", mode.mock_data ? "ON" : "OFF"),
    statusPill("High Prob", mode.high_prob_action_only ? "ONLY" : "OPEN"),
    statusPill("1M Execution", mode.require_1m_execution ? "REQ" : "SKIP"),
    statusPill("Min Score", fmt(mode.min_score_threshold, 0)),
    statusPill("Max Spread", `${fmt(mode.max_spread_percent, 1)}%`),
  ].join("");
}

function renderBuyer(payload) {
  const buyer = payload.buyer || {};
  const signal = buyer.latest_signal || {};
  const entry = buyer.latest_entry || {};
  const candidate = buyer.top_candidate || {};
  const state = buyer.market_state || {};
  const guardrails = buyer.guardrails || {};
  const risk = buyer.risk_plan || {};
  const readiness = buyer.readiness || "WAIT";

  el("buyer-state").innerHTML = `<span class="badge ${readinessClass(readiness)}">${escapeHtml(readiness)}</span>`;
  el("buyer-hero").innerHTML = `
    <div class="readiness-card ${readinessClass(readiness)}">
      <span>Current Buyer Bias</span>
      <strong>${escapeHtml(readiness)}</strong>
      <p>${escapeHtml(primaryBuyerMessage(buyer))}</p>
    </div>
    <div class="buyer-stat">
      <span>Direction</span>
      <strong>${escapeHtml(entry.direction || signal.signal || candidate.candidate_direction || "-")}</strong>
    </div>
    <div class="buyer-stat">
      <span>Strike</span>
      <strong>${fmt(entry.strike || signal.strike || candidate.strike, 0)}</strong>
    </div>
    <div class="buyer-stat">
      <span>Premium</span>
      <strong>${fmt(entry.option_ltp || signal.option_entry_ltp || candidate.option_ltp || state.option_ltp)}</strong>
    </div>
  `;

  el("buyer-plan").innerHTML = [
    planItem("Trigger", fmt(entry.trigger_price || signal.entry_above || signal.entry_below)),
    planItem("Invalidate", fmt(entry.invalidate_price || signal.invalidate_price)),
    planItem("First Target", fmt(entry.first_target_price || signal.first_target_price)),
    planItem("Entry Score", fmt(entry.entry_score, 0)),
    planItem("Context Score", fmt(signal.strategy_score, 0)),
    planItem("Signal Age", buyer.signal_age_minutes == null ? "-" : `${fmt(buyer.signal_age_minutes, 1)}m`),
  ].join("");

  el("buyer-guardrails").innerHTML = renderGuardrails(buyer);

  el("premium-state").innerHTML = [
    premiumCell("Recommended", state.recommended_action || "-"),
    premiumCell("Premium", state.premium_state || "-"),
    premiumCell("1m Change", `${fmt(state.premium_change_1m)}%`),
    premiumCell("3m Change", `${fmt(state.premium_change_3m)}%`),
    premiumCell("Spread", `${fmt(state.spread_percent ?? candidate.spread_percent, 2)}%`),
    premiumCell("Liquidity", state.liquidity_quality || "-"),
  ].join("");

  el("risk-plan").innerHTML = [
    riskTile("Stop", fmt(risk.stop_points)),
    riskTile("Target", fmt(risk.target_points)),
    riskTile("Trail", fmt(risk.trail_points)),
    riskTile("R:R", fmt(risk.rr, 2)),
  ].join("");

  el("buyer-checklist").innerHTML = [
    checklistItem("Fresh signal", buyer.signal_age_minutes == null || buyer.signal_age_minutes <= guardrails.signal_validity_minutes),
    checklistItem("1m entry confirmed", Boolean(entry.decision) && !String(entry.decision).toUpperCase().includes("REJECT")),
    checklistItem("Spread inside limit", Number(state.spread_percent ?? candidate.spread_percent ?? 0) <= Number(guardrails.max_spread_percent ?? 999)),
    checklistItem("No premium chase", Number(state.premium_change_3m ?? 0) <= Number(guardrails.premium_chase_max_3m_pct ?? 999)),
    checklistItem("Scores acceptable", Number(entry.entry_score ?? signal.strategy_score ?? 0) >= Number(guardrails.min_entry_score ?? 0)),
    checklistItem("Paper/live mode known", payload.mode?.paper_trade ? true : payload.mode?.test_mode),
  ].join("");
}

function renderGammaRadar(payload) {
  const radar = payload.gamma_radar || {};
  const state = radar.state || "NO_DATA";
  el("gamma-state").innerHTML = `<span class="badge ${gammaTone(state)}">${escapeHtml(state)}</span>`;

  const metrics = radar.metrics || {};
  const sweep = radar.liquidity_sweep || {};
  el("gamma-radar").innerHTML = [
    gammaTile("Score", fmt(radar.score, 0), gammaTone(state)),
    gammaTile("Direction", radar.direction || "-", sideClass(radar.direction)),
    gammaTile("Wall Zone", radar.wall_signal || "-"),
    gammaTile("Liquidity Sweep", sweep.state || "NONE", sweepTone(sweep.state)),
    gammaTile("Premium Breadth", fmt(metrics.price_breadth, 0)),
    gammaTile("Volume Breadth", fmt(metrics.volume_breadth, 0)),
    gammaTile("Opp Collapse", fmt(metrics.opposite_collapse, 0)),
    gammaTile("Max Jump", `${fmt(metrics.max_premium_jump_pct, 1)}%`),
    gammaTile("Same Vol+", fmt(metrics.same_volume_total, 0)),
  ].join("");

  const signals = radar.signals || [];
  const examples = metrics.examples || [];
  const rows = [
    guardrailItem("State", radar.summary || "No sweep context yet.", gammaTone(state)),
    guardrailItem("Sweep", sweep.summary || "No fresh 1m sweep.", sweepTone(sweep.state)),
    ...signals.map((text) => guardrailItem("Signal", text, gammaTone(state))),
    ...examples.map((text) => guardrailItem("Print", text, "ok")),
  ];
  if (!signals.length && !examples.length) {
    rows.push(guardrailItem("Watch", "Need premium breadth + volume breadth + opposite-side collapse together.", "warn"));
  }
  el("sweep-signals").innerHTML = rows.join("");
}

function renderGammaOverview(payload) {
  const rows = payload.gamma_overview || [];
  el("gamma-overview").innerHTML =
    rows
      .map(
        (row) => {
          const previous = state.previousGamma.get(row.instrument);
          const changed = previous && (previous.state !== row.state || Number(previous.score) !== Number(row.score));
          state.previousGamma.set(row.instrument, { state: row.state, score: row.score });
          return `
        <button class="gamma-summary ${gammaTone(row.state)} ${row.instrument === state.instrument ? "active" : ""} ${changed ? "changed" : ""}" data-instrument="${escapeHtml(row.instrument)}">
          <span>${escapeHtml(row.instrument)}</span>
          <strong>${escapeHtml(row.state || "-")} · ${fmt(row.score, 0)}</strong>
          <em>${escapeHtml(row.direction || "-")} · ${escapeHtml(row.wall_signal || "-")}</em>
        </button>`;
        },
      )
      .join("");
}

function renderNews(payload) {
  const news = payload.news || {};
  const items = news.items || [];
  const errors = news.errors || [];
  el("news-status").textContent = errors.length ? `${items.length} items · ${errors.length} feed issue` : `${items.length} items`;
  el("news-feed").innerHTML =
    items
      .map(
        (item) => `
        <a class="news-item ${String(item.impact || "").toLowerCase()}" href="${escapeHtml(item.link)}" target="_blank" rel="noreferrer">
          <div class="news-top">
            <span class="badge ${newsTone(item.impact)}">${escapeHtml(item.impact || "LOW")}</span>
            <em>${escapeHtml(item.source || item.feed || "-")} · ${dateTime(item.published)}</em>
          </div>
          <strong>${escapeHtml(item.title)}</strong>
          <p>${escapeHtml(item.summary || "")}</p>
          <div class="tag-row">${(item.tags || []).map((tag) => `<span>${escapeHtml(tag)}</span>`).join("")}</div>
        </a>`,
      )
      .join("") || `<div class="empty">No news feed items loaded</div>`;
}

function renderManualFocus(payload) {
  const rows = payload.manual_focus || [];
  el("manual-focus").innerHTML =
    rows
      .map(
        (row) => `
        <button class="focus-item ${focusTone(row.label)} ${row.instrument === state.instrument ? "active" : ""}" data-instrument="${escapeHtml(row.instrument)}">
          <div class="focus-top">
            <strong>${escapeHtml(row.instrument)}</strong>
            <span class="badge ${focusTone(row.label)}">${escapeHtml(row.label)}</span>
          </div>
          <div class="focus-metrics">
            <span>${escapeHtml(row.direction || "-")}</span>
            <span>${escapeHtml(row.gamma_state || "-")} ${fmt(row.gamma_score, 0)}</span>
            <span>${escapeHtml(row.smart_money_state || "-")} ${fmt(row.smart_money_score, 0)}</span>
            <span>${escapeHtml(row.sweep || "NO_SWEEP")}</span>
          </div>
          <p>${escapeHtml(row.reason || "")}</p>
        </button>`,
      )
      .join("");
}

function renderSmartMoney(payload) {
  const smart = payload.smart_money || {};
  const tone = smartTone(smart.state);
  el("smart-money-state").innerHTML = `<span class="badge ${tone}">${escapeHtml(smart.state || "NO_EDGE")} · ${fmt(smart.score, 0)}</span>`;
  el("smart-money-grid").innerHTML = [
    gammaTile("State", smart.state || "NO_EDGE", tone),
    gammaTile("Direction", smart.direction || "-", sideClass(smart.direction)),
    gammaTile("Trap Flags", fmt((smart.trap_flags || []).length, 0), (smart.trap_flags || []).length ? "bad" : "ok"),
    gammaTile("Alignment", fmt((smart.alignment_flags || []).length, 0), (smart.alignment_flags || []).length >= 3 ? "ok" : "warn"),
    gammaTile("Wait Flags", fmt((smart.wait_flags || []).length, 0), (smart.wait_flags || []).length ? "warn" : "ok"),
    gammaTile("Rule", "No trap + premium + sweep", tone),
  ].join("");

  const rows = [
    ...(smart.trap_flags || []).map((text) => guardrailItem("Trap", text, "bad")),
    ...(smart.alignment_flags || []).map((text) => guardrailItem("Aligned", text, "ok")),
    ...(smart.wait_flags || []).map((text) => guardrailItem("Wait", text, "warn")),
  ];
  if (!rows.length) rows.push(guardrailItem("Rule", smart.rule || "Wait for confirmation.", "warn"));
  el("smart-money-flags").innerHTML = rows.join("");
}

function renderDiscipline(payload) {
  const discipline = payload.discipline || {};
  const tone = disciplineTone(discipline.verdict);
  el("discipline-state").innerHTML = `<span class="badge ${tone}">${escapeHtml(discipline.verdict || "WAIT")}</span>`;
  el("discipline-grid").innerHTML = [
    gammaTile("Verdict", discipline.verdict || "WAIT", tone),
    gammaTile("Setup Score", fmt(discipline.setup_score, 0), discipline.setup_score >= 72 ? "ok" : discipline.setup_score >= 48 ? "warn" : ""),
    gammaTile("FOMO Risk", fmt(discipline.fomo_score, 0), discipline.fomo_score >= 50 ? "bad" : discipline.fomo_score >= 25 ? "warn" : "ok"),
    gammaTile("Quality Flags", fmt((discipline.quality_flags || []).length, 0), "ok"),
    gammaTile("FOMO Flags", fmt((discipline.fomo_flags || []).length, 0), (discipline.fomo_flags || []).length ? "bad" : "ok"),
    gammaTile("No-Trade Flags", fmt((discipline.no_trade_flags || []).length, 0), (discipline.no_trade_flags || []).length ? "bad" : "ok"),
  ].join("");

  const rows = [
    ...(discipline.no_trade_flags || []).map((text) => guardrailItem("No Trade", text, "bad")),
    ...(discipline.fomo_flags || []).map((text) => guardrailItem("FOMO", text, "bad")),
    ...(discipline.quality_flags || []).map((text) => guardrailItem("Quality", text, "ok")),
    ...(discipline.rules || []).map((text) => guardrailItem("Rule", text, "warn")),
  ];
  el("discipline-rules").innerHTML = rows.join("");
}

function disciplineTone(verdict) {
  const text = String(verdict || "").toUpperCase();
  if (text === "A_PLUS_ONLY") return "ok";
  if (text === "NO_TRADE") return "bad";
  if (text === "SMALL_SIZE_WATCH") return "warn";
  return "";
}

function renderTradeCoach(payload) {
  const coach = payload.trade_coach || {};
  const tone = coachTone(coach.action);
  el("coach-state").innerHTML = `<span class="badge ${tone}">${escapeHtml(coach.action || "MONITOR")}</span>`;
  el("trade-coach").innerHTML = `
    <div class="readiness-card ${tone}">
      <span>Coach Action</span>
      <strong>${escapeHtml(coach.action || "MONITOR")}</strong>
      <p>${escapeHtml(coach.reason || "")}</p>
    </div>
    <div class="buyer-stat">
      <span>Assumed Trade</span>
      <strong>${escapeHtml(coach.direction || "-")} ${fmt(coach.strike, 0)}</strong>
    </div>
    <div class="buyer-stat">
      <span>P&L</span>
      <strong>${fmt(coach.pnl_points)} pts</strong>
    </div>
    <div class="buyer-stat">
      <span>P&L %</span>
      <strong>${coach.pnl_pct == null ? "-" : `${fmt(coach.pnl_pct, 1)}%`}</strong>
    </div>
  `;
  el("coach-plan").innerHTML = [
    planItem("Entry", fmt(coach.assumed_entry)),
    planItem("Current", fmt(coach.current_premium)),
    planItem("Hard Stop", fmt(coach.hard_stop)),
    planItem("First Target", fmt(coach.first_target)),
    planItem("Trail Floor", fmt(coach.trail_floor)),
    planItem("Cadence", `${fmt(coach.cadence_seconds, 0)}s`),
  ].join("");
  el("coach-rules").innerHTML = (coach.rules || []).map((text) => guardrailItem("Rule", text, "warn")).join("");
  maybeShowTradeAlert(payload);
}

function maybeShowTradeAlert(payload) {
  const coach = payload.trade_coach || {};
  const key = `${payload.instrument}:${coach.alert_text || ""}:${coach.direction || ""}:${coach.strike || ""}`;
  const box = el("trade-alert");
  if (coach.alert && coach.alert_text && state.dismissedAlertKey !== key) {
    el("alert-title").textContent = coach.action === "NO_TRADE" ? "No Trade" : "Trade Alert";
    el("alert-text").textContent = coach.alert_text;
    box.classList.remove("hidden");
    box.className = `trade-alert ${coachTone(coach.action)}`;
    box.dataset.alertKey = key;
  } else if (!coach.alert) {
    box.classList.add("hidden");
  }
}

function coachTone(action) {
  const text = String(action || "").toUpperCase();
  if (["HOLD", "HOLD_TRAIL"].includes(text)) return "ok";
  if (["WATCH_CLOSELY", "BOOK_PARTIAL", "EXIT_PROTECT"].includes(text)) return "warn";
  if (["EXIT_NOW", "NO_TRADE"].includes(text)) return "bad";
  return "";
}

function smartTone(state) {
  const text = String(state || "").toUpperCase();
  if (text === "SMART_MONEY_ALIGNED") return "ok";
  if (text === "TRAP_RISK") return "bad";
  if (text === "FOLLOW_THROUGH_PENDING") return "warn";
  return "";
}

function focusTone(label) {
  const text = String(label || "").toUpperCase();
  if (text === "FOCUS") return "ok";
  if (text === "AVOID") return "bad";
  if (text === "WATCH") return "warn";
  return "";
}

function syncRiskCalculator(payload) {
  const plan = payload.buyer?.risk_plan || {};
  if (plan.stop_points != null) el("risk-stop").value = Number(plan.stop_points);
  if (plan.lot_size != null) el("risk-lot").value = Number(plan.lot_size);
  calculateRisk();
}

function calculateRisk() {
  const budget = Number(el("risk-budget")?.value || 0);
  const stop = Number(el("risk-stop")?.value || 0);
  const lot = Number(el("risk-lot")?.value || 0);
  const oneLotRisk = stop * lot;
  const lots = oneLotRisk > 0 ? Math.floor(budget / oneLotRisk) : 0;
  const capitalAtRisk = lots * oneLotRisk;
  el("risk-calc-status").textContent = oneLotRisk > 0 ? `1 lot risk ${fmt(oneLotRisk, 0)}` : "";
  el("risk-answer").innerHTML = `
    <strong>${fmt(lots, 0)} lots</strong>
    <span>Approx risk ${fmt(capitalAtRisk, 0)} on budget ${fmt(budget, 0)}</span>
    <p>Keep manual size at or below this if stop is respected.</p>
  `;
}

function newsTone(impact) {
  const text = String(impact || "").toUpperCase();
  if (text === "HIGH") return "bad";
  if (text === "MEDIUM") return "warn";
  return "";
}

function sweepTone(state) {
  const text = String(state || "").toUpperCase();
  if (text.includes("RECLAIM") || text.includes("REJECT")) return "ok";
  if (text.includes("BREAK_HOLDING")) return "warn";
  return "";
}

function gammaTone(state) {
  const text = String(state || "").toUpperCase();
  if (text.includes("ACTIVE")) return "ok";
  if (text.includes("BUILDING") || text.includes("WATCH")) return "warn";
  if (text.includes("NO_DATA")) return "";
  return text.includes("QUIET") ? "" : "bad";
}

function gammaTile(label, value, tone = "") {
  return `<div class="gamma-tile ${tone}"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function primaryBuyerMessage(buyer) {
  if (buyer.blockers?.length) return buyer.blockers[0];
  if (buyer.cautions?.length) return buyer.cautions[0];
  if (buyer.latest_entry?.reason) return buyer.latest_entry.reason;
  if (buyer.latest_signal?.confidence_summary) return buyer.latest_signal.confidence_summary;
  return "No fresh buyer setup. Wait for a clean premium and entry confirmation.";
}

function planItem(label, value) {
  return `<div class="plan-item"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function premiumCell(label, value) {
  return `<div class="pressure-cell"><strong>${escapeHtml(value)}</strong><span>${escapeHtml(label)}</span></div>`;
}

function riskTile(label, value) {
  return `<div class="risk-tile"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function checklistItem(label, ok) {
  return `<div class="check-item ${ok ? "pass" : "fail"}"><span>${ok ? "PASS" : "CHECK"}</span><strong>${escapeHtml(label)}</strong></div>`;
}

function renderGuardrails(buyer) {
  const blockers = buyer.blockers || [];
  const cautions = buyer.cautions || [];
  const guardrails = buyer.guardrails || {};
  const lines = [
    ...blockers.map((text) => guardrailItem("Blocker", text, "bad")),
    ...cautions.map((text) => guardrailItem("Caution", text, "warn")),
  ];
  lines.push(guardrailItem("No chase", `3m premium cap ${fmt(guardrails.premium_chase_max_3m_pct, 1)}%`, "ok"));
  lines.push(guardrailItem("Spread", `Max spread ${fmt(guardrails.max_spread_percent, 1)}%`, "ok"));
  lines.push(guardrailItem("Validity", `Signal ${fmt(guardrails.signal_validity_minutes, 0)}m · Entry ${fmt(guardrails.entry_validity_minutes, 0)}m`, "ok"));
  lines.push(guardrailItem("Quality", `Context ${fmt(guardrails.min_context_score, 0)} · Entry ${fmt(guardrails.min_entry_score, 0)}`, "ok"));
  return lines.join("");
}

function guardrailItem(label, text, tone) {
  return `
    <div class="guardrail-item">
      <span class="badge ${tone}">${escapeHtml(label)}</span>
      <p>${escapeHtml(text)}</p>
    </div>`;
}

function renderMarket(payload) {
  const oi = payload.market?.oi || {};
  el("generated-at").textContent = `Updated ${dateTime(payload.generated_at)}`;
  el("market-metrics").innerHTML = [
    metric("Underlying", fmt(oi.underlying_price)),
    metric("PCR", fmt(oi.pcr, 2)),
    metric("Support", fmt(oi.support_level)),
    metric("Resistance", fmt(oi.resistance_level)),
  ].join("");

  el("oi-pressure").innerHTML = [
    `<div class="pressure-cell large"><strong>${escapeHtml(oi.oi_sentiment || "-")}</strong><span>Sentiment / ${escapeHtml(oi.oi_trend || "-")}</span></div>`,
    `<div class="pressure-cell"><strong>${fmt(oi.ce_oi_change, 0)}</strong><span>CE OI Change</span></div>`,
    `<div class="pressure-cell"><strong>${fmt(oi.pe_oi_change, 0)}</strong><span>PE OI Change</span></div>`,
    `<div class="pressure-cell"><strong>${fmt(oi.ce_volume, 0)}</strong><span>CE Volume</span></div>`,
    `<div class="pressure-cell"><strong>${fmt(oi.pe_volume, 0)}</strong><span>PE Volume</span></div>`,
    `<div class="pressure-cell large"><strong>${escapeHtml(oi.data_quality || "-")}</strong><span>Data Quality / Liquidity ${fmt(oi.liquidity_score, 2)}</span></div>`,
  ].join("");

  drawPriceChart(payload.market?.candles_5m || []);
}

function drawPriceChart(candles) {
  const canvas = el("price-chart");
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#fbfcfa";
  ctx.fillRect(0, 0, width, height);

  const closes = candles.map((row) => Number(row.close)).filter(Number.isFinite);
  if (closes.length < 2) {
    ctx.fillStyle = "#657168";
    ctx.font = "14px system-ui";
    ctx.fillText("No 5m candle data", 24, 42);
    return;
  }

  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const pad = 24;
  const range = max - min || 1;
  const xStep = (width - pad * 2) / (closes.length - 1);

  ctx.strokeStyle = "#dce3da";
  ctx.lineWidth = 1;
  for (let i = 0; i < 4; i += 1) {
    const y = pad + ((height - pad * 2) * i) / 3;
    ctx.beginPath();
    ctx.moveTo(pad, y);
    ctx.lineTo(width - pad, y);
    ctx.stroke();
  }

  ctx.strokeStyle = closes[closes.length - 1] >= closes[0] ? "#0b6b57" : "#b1442f";
  ctx.lineWidth = 3;
  ctx.beginPath();
  closes.forEach((close, index) => {
    const x = pad + xStep * index;
    const y = height - pad - ((close - min) / range) * (height - pad * 2);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  ctx.fillStyle = "#17211c";
  ctx.font = "12px system-ui";
  ctx.fillText(`High ${fmt(max)}`, pad, 16);
  ctx.fillText(`Low ${fmt(min)}`, pad, height - 8);
}

function renderSignals(signals) {
  const rows = signals || [];
  el("signals-table").innerHTML =
    rows
      .map(
        (row) => `
        <tr>
          <td>${timeOnly(row.ts)}</td>
          <td><span class="badge ${sideClass(row.signal)}">${escapeHtml(row.signal)}</span></td>
          <td>${fmt(row.strike, 0)}</td>
          <td>${fmt(row.option_entry_ltp)}</td>
          <td>${fmt(row.strategy_score, 0)}</td>
          <td>${escapeHtml(row.signal_quality || "-")}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="6" class="empty">No issued signals</td></tr>`;
}

function renderDecisions(decisions) {
  el("decision-list").innerHTML =
    (decisions || [])
      .map(
        (row) => `
        <article class="decision-item">
          <div class="decision-top">
            <div class="decision-title">${timeOnly(row.ts)} · ${escapeHtml(row.signal || "NO_SIGNAL")} · ${fmt(row.price)}</div>
            <span class="badge ${badgeClass(row.tradability || row.signal_quality)}">${escapeHtml(row.signal_quality || row.tradability || "-")}</span>
          </div>
          <div class="decision-meta">Score ${fmt(row.strategy_score, 0)} · ${escapeHtml(row.setup_type || "-")} · ${escapeHtml(row.time_regime || "-")} · Strike ${fmt(row.strike, 0)}</div>
          <div class="reason">${escapeHtml(row.confidence_summary || row.reason || "")}</div>
        </article>`,
      )
      .join("") || `<div class="empty">No strategy decisions</div>`;
}

function renderCandidates(candidates) {
  el("candidates-table").innerHTML =
    (candidates || [])
      .map(
        (row) => `
        <tr>
          <td>${fmt(row.candidate_rank, 0)}${row.selected_for_signal ? " *" : ""}</td>
          <td><span class="badge ${sideClass(row.candidate_direction)}">${escapeHtml(row.candidate_direction)}</span></td>
          <td>${fmt(row.strike, 0)}</td>
          <td>${fmt(row.option_ltp)}</td>
          <td>${fmt(row.spread_percent, 2)}%</td>
          <td>${fmt(row.expected_edge)}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="6" class="empty">No candidates</td></tr>`;
}

function renderEntries(entries) {
  el("entry-list").innerHTML =
    (entries || [])
      .map(
        (row) => `
        <article class="decision-item">
          <div class="decision-top">
            <div class="decision-title">${timeOnly(row.ts)} · ${escapeHtml(row.direction)} · ${escapeHtml(row.decision)}</div>
            <span class="badge ${badgeClass(row.confidence || row.option_buyer_action)}">${escapeHtml(row.confidence || row.option_buyer_action || "-")}</span>
          </div>
          <div class="decision-meta">Entry score ${fmt(row.entry_score, 0)} · Trigger ${fmt(row.trigger_price)} · SL ${fmt(row.invalidate_price)} · T1 ${fmt(row.first_target_price)}</div>
          <div class="reason">${escapeHtml(row.reason || "")}</div>
        </article>`,
      )
      .join("") || `<div class="empty">No entry watch decisions</div>`;
}

function renderRuntime(runtime) {
  el("runtime-list").innerHTML =
    (runtime || [])
      .map(
        (row) => `
        <article class="service-item">
          <div class="service-top">
            <div class="service-title">${escapeHtml(row.service)}</div>
            <span class="badge ${badgeClass(row.severity || row.status)}">${escapeHtml(row.status)}</span>
          </div>
          <div class="service-meta">${escapeHtml(row.phase || "-")} · ${escapeHtml(row.instrument || "-")} · hb ${fmt(row.heartbeat_age, 1)}s · restarts ${fmt(row.recent_restarts, 0)}</div>
        </article>`,
      )
      .join("") || `<div class="empty">No runtime heartbeats</div>`;
}

function renderMonitor(events) {
  el("monitor-list").innerHTML =
    (events || [])
      .map(
        (row) => `
        <article class="decision-item">
          <div class="decision-top">
            <div class="decision-title">${timeOnly(row.ts)} · ${escapeHtml(row.signal)} · PnL ${fmt(row.pnl_points)}</div>
            <span class="badge ${badgeClass(row.guidance)}">${escapeHtml(row.guidance || "-")}</span>
          </div>
          <div class="decision-meta">${escapeHtml(row.quality || "-")} · ${escapeHtml(row.structure_state || "-")} · Trail ${fmt(row.dynamic_trail_pct, 1)}%</div>
          <div class="reason">${escapeHtml(row.reason || "")}</div>
        </article>`,
      )
      .join("") || `<div class="empty">No active monitor events</div>`;
}

function renderOutcomes(outcomes) {
  el("outcomes-table").innerHTML =
    (outcomes || [])
      .map(
        (row) => `
        <tr>
          <td>${fmt(row.horizon_minutes, 0)}</td>
          <td>${fmt(row.win_rate, 1)}</td>
          <td>${fmt(row.avg_points)}</td>
          <td>${fmt(row.avg_mfe)}</td>
          <td>${fmt(row.avg_mae)}</td>
        </tr>`,
      )
      .join("") || `<tr><td colspan="5" class="empty">No 30D outcome data</td></tr>`;
}

function render(payload) {
  state.latestPayload = payload;
  state.instruments = payload.instruments || state.instruments;
  renderTabs();
  renderMode(payload);
  renderGammaOverview(payload);
  renderManualFocus(payload);
  renderDiscipline(payload);
  renderTradeCoach(payload);
  renderBuyer(payload);
  renderSmartMoney(payload);
  renderGammaRadar(payload);
  renderNews(payload);
  renderMarket(payload);
  renderSignals(payload.strategy?.signals);
  renderDecisions(payload.strategy?.decisions);
  renderCandidates(payload.strategy?.candidates);
  renderEntries(payload.strategy?.entries);
  renderRuntime(payload.runtime);
  renderMonitor(payload.strategy?.monitor);
  renderOutcomes(payload.strategy?.outcomes);
  syncRiskCalculator(payload);
}

async function loadDashboard() {
  el("subtitle").textContent = `${state.instrument} · loading`;
  el("refresh-state").textContent = "Refreshing";
  const response = await fetch(`/api/dashboard?instrument=${encodeURIComponent(state.instrument)}`, {
    cache: "no-store",
  });
  const payload = await response.json();
  render(payload);
  state.nextRefreshAt = Date.now() + state.refreshSeconds * 1000;
  el("refresh-state").textContent = `Updated ${timeOnly(payload.generated_at)}`;
  el("subtitle").textContent = `${state.instrument} · option buyer cockpit`;
}

document.addEventListener("click", (event) => {
  const tab = event.target.closest("[data-instrument]");
  if (tab) {
    state.instrument = tab.dataset.instrument;
    localStorage.setItem("optioncoder.instrument", state.instrument);
    loadDashboard();
  }
  if (event.target.id === "refresh") {
    loadDashboard();
  }
  if (event.target.id === "alert-close") {
    state.dismissedAlertKey = el("trade-alert").dataset.alertKey;
    el("trade-alert").classList.add("hidden");
  }
});

document.addEventListener("input", (event) => {
  if (event.target.closest(".risk-calc")) {
    calculateRisk();
  }
});

renderTabs();
loadDashboard();
setInterval(loadDashboard, state.refreshSeconds * 1000);
setInterval(() => {
  const seconds = Math.max(0, Math.ceil((state.nextRefreshAt - Date.now()) / 1000));
  el("refresh-countdown").textContent = `${seconds}s`;
}, 1000);
