"""
Local web dashboard: FastAPI + single-page UI, JSON at /api/state.
Runs in a daemon thread; state is updated from the bot's main loop.
"""

from __future__ import annotations

import logging
import math
import socket
import threading
import time
from typing import Any, Callable, Dict, Optional

from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response
import uvicorn

logger = logging.getLogger("btc_live")

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>BTC Live Bot</title>
  <style>
    :root {
      --bg: #0d1117; --panel: #161b22; --border: #30363d;
      --text: #e6edf3; --muted: #8b949e; --green: #3fb950; --red: #f85149;
      --yellow: #d29922; --blue: #58a6ff; --violet: #a371f7;
    }
    * { box-sizing: border-box; }
    body { font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text);
      margin: 0; padding: 1rem; line-height: 1.45; }
    h1 { font-size: 1.1rem; font-weight: 600; margin: 0 0 0.75rem; }
    .meta { color: var(--muted); font-size: 0.85rem; margin-bottom: 1rem; }
    .grid { display: grid; gap: 0.75rem; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 0.85rem; }
    .card h2 { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted);
      margin: 0 0 0.5rem; }
    .row { display: flex; justify-content: space-between; gap: 0.5rem; font-size: 0.9rem; }
    .sig { font-size: 1rem; font-weight: 600; }
    .sig.wait { color: var(--yellow); }
    .sig.buy { color: var(--green); }
    .sig.block { color: var(--red); }
    .mono { font-family: ui-monospace, monospace; font-size: 0.82rem; }
    .btc { border-color: #d29922; }
    footer { margin-top: 1rem; color: var(--muted); font-size: 0.75rem; }
  </style>
</head>
<body>
  <h1>BTC up/down — live</h1>
  <div class="meta" id="meta">Loading…</div>
  <div class="grid">
    <div class="card"><h2>Session</h2><div id="session" class="mono"></div></div>
    <div class="card"><h2>Strategy</h2><div id="strategy"></div></div>
    <div class="card"><h2>Volume + Late Entry</h2><div id="modepanel" class="mono"></div></div>
    <div class="card"><h2>Indicator Controls</h2><div id="controls" class="mono"></div></div>
    <div class="card"><h2>5s / 15s Checks</h2><div id="checks" class="mono"></div></div>
    <div class="card"><h2>UP</h2><div id="up" class="mono"></div></div>
    <div class="card"><h2>DOWN</h2><div id="down" class="mono"></div></div>
    <div class="card btc"><h2>BTC / USD (Chainlink)</h2><div id="btc" class="mono"></div></div>
    <div class="card"><h2>Trading</h2><div id="trading" class="mono"></div></div>
  </div>
  <footer>Refreshes every second · <span id="err"></span></footer>
  <script>
    /* No optional chaining (?.) — must run in older browsers / Edge legacy. */
    function esc(s) {
      if (s === null || s === undefined) return "";
      var el = document.createElement("div");
      el.textContent = String(s);
      return el.innerHTML;
    }
    function sigClass(t) {
      if (!t) return "wait";
      if (t.indexOf("BUY") >= 0) return "buy";
      /* Do not use \\uD83D\\uDEAB here: Python treats \\u.... in the template as escapes and emits invalid UTF-8 surrogates. */
      if (t.indexOf("NO ENTRY") >= 0) return "block";
      return "wait";
    }
    function numFmt(n, dec) {
      if (n === null || n === undefined || typeof n !== "number" || isNaN(n)) return "\u2014";
      return n.toFixed(dec);
    }

    function boolHtml(v) {
      if (v === true) return "\u2713";
      if (v === false) return "\u2717";
      return "\u2014";
    }

    var modeCfg = {
      late: null,
      volEval: null,
      volAccel: null,
      lastFetchTs: 0,
    };

    function requestJson(method, url, payload, onDone) {
      var r = new XMLHttpRequest();
      r.open(method, url, true);
      if (payload !== null && payload !== undefined) {
        r.setRequestHeader("Content-Type", "application/json");
      }
      r.onreadystatechange = function () {
        if (r.readyState !== 4) return;
        if (r.status < 200 || r.status >= 300) return;
        try {
          var d = JSON.parse(r.responseText);
          if (onDone) onDone(d);
        } catch (e) {
          /* no-op */
        }
      };
      r.send(payload !== null && payload !== undefined ? JSON.stringify(payload) : null);
    }

    function refreshModeConfig() {
      requestJson("GET", "/api/late-modes", null, function (d) { modeCfg.late = d || {}; });
      requestJson("GET", "/api/volume-eval-mode", null, function (d) { modeCfg.volEval = d || {}; });
      requestJson("GET", "/api/volume-accel-check", null, function (d) { modeCfg.volAccel = d || {}; });
      modeCfg.lastFetchTs = Date.now();
    }

    function applyVolEval() {
      var cfg = modeCfg.volEval || {};
      var payload = {
        enabled: !!document.getElementById("ve-enabled").checked,
        time_left_sec: Number(document.getElementById("ve-time-left").value || cfg.time_left_sec || 0),
        min_contracts: Number(document.getElementById("ve-min-contracts").value || cfg.min_contracts || 1),
        max_trades: Number(document.getElementById("ve-max-trades").value || cfg.max_trades || 1),
        min_price: Number(document.getElementById("ve-min-price").value || cfg.min_price || 0),
        max_price: Number(document.getElementById("ve-max-price").value || cfg.max_price || 1),
        volume_check_enabled: !!document.getElementById("ve-vol-check").checked,
      };
      requestJson("POST", "/api/volume-eval-mode", payload, function (d) {
        modeCfg.volEval = d || payload;
      });
    }

    function applyLateModes() {
      var cfg = modeCfg.late || {};
      var payload = {
        enabled: !!document.getElementById("lm-enabled").checked,
        total_max_trades: Number(document.getElementById("lm-total-max").value || cfg.total_max_trades || 1),
        modes: (cfg.modes || []),
      };
      requestJson("POST", "/api/late-modes", payload, function (d) {
        modeCfg.late = d || payload;
      });
    }

    function applyVolAccel() {
      var cfg = modeCfg.volAccel || {};
      var basisEl = document.getElementById("va-basis");
      var basis = basisEl ? String(basisEl.value || "total") : "total";
      var payload = {
        min_current_volume_diff: Number(document.getElementById("va-min-curr-diff").value || cfg.min_current_volume_diff || 0),
        min_accel_diff: Number(document.getElementById("va-min-accel-diff").value || cfg.min_accel_diff || 0),
        volume_basis: basis,
      };
      requestJson("POST", "/api/volume-accel-check", payload, function (d) {
        modeCfg.volAccel = d || payload;
      });
    }

    function renderModePanel(st) {
      var modeEl = document.getElementById("modepanel");
      if (!modeEl) return;

      var ms = st.mode_strategies || {};
      var vms = ms.volume_eval_mode || {};
      var lms = ms.late_entry_mode || {};
      var vs = st.volume_speed || {};
      var ve = modeCfg.volEval || {};
      var lm = modeCfg.late || {};
      var va = modeCfg.volAccel || {};

      var modeLines = [];
      modeLines.push("Active volume mode: " + (vms.active ? "ON" : "OFF") + " | Ready=" + boolHtml(vms.ready));
      modeLines.push("Active late mode: " + (lms.active ? "ON" : "OFF") + " | Ready=" + boolHtml(lms.ready));
      modeLines.push("Vol speed: " + boolHtml(vs.ok) + " | fav=" + (vs.favorite || "\u2014") + " | basis=" + (vs.volume_basis || "\u2014"));
      modeLines.push("");

      modeLines.push('<label><input id="ve-enabled" type="checkbox" ' + (ve.enabled ? "checked" : "") + '> Volume eval enabled</label>');
      modeLines.push('VE timeLeft: <input id="ve-time-left" type="number" value="' + esc(ve.time_left_sec != null ? ve.time_left_sec : 0) + '" style="width:68px">s');
      modeLines.push('VE minC: <input id="ve-min-contracts" type="number" value="' + esc(ve.min_contracts != null ? ve.min_contracts : 1) + '" style="width:56px">');
      modeLines.push('VE maxTrades: <input id="ve-max-trades" type="number" value="' + esc(ve.max_trades != null ? ve.max_trades : 1) + '" style="width:56px">');
      modeLines.push('VE P: <input id="ve-min-price" type="number" step="0.001" value="' + esc(ve.min_price != null ? ve.min_price : 0) + '" style="width:70px"> - <input id="ve-max-price" type="number" step="0.001" value="' + esc(ve.max_price != null ? ve.max_price : 1) + '" style="width:70px">');
      modeLines.push('<label><input id="ve-vol-check" type="checkbox" ' + (ve.volume_check_enabled ? "checked" : "") + '> VE volume gate</label>');
      modeLines.push('<button id="ve-apply" type="button">Apply VE</button>');
      modeLines.push("");

      modeLines.push('<label><input id="lm-enabled" type="checkbox" ' + (lm.enabled ? "checked" : "") + '> Late modes enabled</label>');
      modeLines.push('Late total max: <input id="lm-total-max" type="number" value="' + esc(lm.total_max_trades != null ? lm.total_max_trades : 1) + '" style="width:64px">');
      modeLines.push('<button id="lm-apply" type="button">Apply Late</button>');

      if (lm.modes && lm.modes.length) {
        for (var i = 0; i < lm.modes.length; i++) {
          var m = lm.modes[i] || {};
          modeLines.push(' - ' + esc(m.key || ("mode" + i)) + ': en=' + boolHtml(m.enabled) + ', t=' + esc(m.time_left_sec) + 's, minC=' + esc(m.min_contracts) + ', maxT=' + esc(m.max_trades));
        }
      }
      modeLines.push("");

      modeLines.push('Vol accel minCurr: <input id="va-min-curr-diff" type="number" value="' + esc(va.min_current_volume_diff != null ? va.min_current_volume_diff : 0) + '" style="width:86px">');
      modeLines.push('Vol accel minDiff: <input id="va-min-accel-diff" type="number" value="' + esc(va.min_accel_diff != null ? va.min_accel_diff : 0) + '" style="width:86px">');
      modeLines.push('Vol basis: <select id="va-basis"><option value="total"' + (va.volume_basis === "total" ? " selected" : "") + '>total</option><option value="buy"' + (va.volume_basis === "buy" ? " selected" : "") + '>buy</option></select>');
      modeLines.push('<button id="va-apply" type="button">Apply Vol</button>');

      modeEl.innerHTML = modeLines.join("<br/>");

      var veBtn = document.getElementById("ve-apply");
      var lmBtn = document.getElementById("lm-apply");
      var vaBtn = document.getElementById("va-apply");
      if (veBtn) veBtn.onclick = applyVolEval;
      if (lmBtn) lmBtn.onclick = applyLateModes;
      if (vaBtn) vaBtn.onclick = applyVolAccel;
    }

    function applyControls() {
      var payload = {
        momentum: !!document.getElementById("ctl-momentum").checked,
        vwap_deviation: !!document.getElementById("ctl-vwap").checked,
        zscore: !!document.getElementById("ctl-zscore").checked,
      };
      var r = new XMLHttpRequest();
      r.open("POST", "/api/indicator-controls", true);
      r.setRequestHeader("Content-Type", "application/json");
      r.send(JSON.stringify(payload));
    }
    function tick() {
      var errEl = document.getElementById("err");
      if (!modeCfg.lastFetchTs || (Date.now() - modeCfg.lastFetchTs) > 5000) {
        refreshModeConfig();
      }
      var r = new XMLHttpRequest();
      r.open("GET", "/api/state", true);
      r.onreadystatechange = function () {
        if (r.readyState !== 4) return;
        try {
          if (r.status !== 200) throw new Error("HTTP " + r.status);
          var d = JSON.parse(r.responseText);
          errEl.textContent = "";
          var hdr = d.header || {};
          var slug = hdr.slug != null ? String(hdr.slug) : "\u2014";
          var ts = "";
          if (d.ts) ts = new Date(d.ts * 1000).toISOString();
          document.getElementById("meta").innerHTML = esc(slug) + " \u00b7 " + esc(ts);
          document.getElementById("session").innerHTML = [
            "Timer: " + (hdr.time_left_sec != null ? esc(Math.floor(hdr.time_left_sec) + "s left") : "\u2014"),
            "WS: " + (hdr.ws_connected ? "live" : "disconnected"),
            "Mode: " + (hdr.simulation ? "simulation" : "real"),
          ].join("<br/>");
          var st = d.strategy || {};
          var sig = st.signal_text || "\u2014";
          function chk(x) { return x === true ? "\u2713" : x === false ? "\u2717" : "\u2014"; }
          var ck = st.checks || {};
          var strategyBits = [
            "Fav: " + esc(st.favorite) + " \u00b7 WR: " + esc(st.win_rate_str),
            "Checks: P=" + chk(ck.price) + " T=" + chk(ck.time) + " D=" + chk(ck.dev) +
            " M=" + chk(ck.mom) + " Z=" + chk(ck.zscore) + " B=" + chk(ck.btc_buffer) + " cutoff=" + chk(ck.time_cutoff)
          ];
            if (st.up_line) {
              strategyBits.push("UP: " + esc(st.up_line));
            }
            if (st.down_line) {
              strategyBits.push("DOWN: " + esc(st.down_line));
            }
          if (st.btc_buffer_line) {
            strategyBits.push("BTC Buffer: " + esc(st.btc_buffer_line));
          }
          document.getElementById("strategy").innerHTML =
            '<div class="sig ' + sigClass(sig) + '">' + esc(sig) + "</div>" +
            '<div class="mono" style="margin-top:0.4rem">' +
            strategyBits.join("<br/>") +
            "</div>";

          renderModePanel(st);

          var ctl = st.indicator_controls || {};
          document.getElementById("controls").innerHTML = [
            '<label><input id="ctl-momentum" type="checkbox" ' + (ctl.momentum ? "checked" : "") + '> Price momentum</label>',
            '<label><input id="ctl-vwap" type="checkbox" ' + (ctl.vwap_deviation ? "checked" : "") + '> VWAP deviation</label>',
            '<label><input id="ctl-zscore" type="checkbox" ' + (ctl.zscore ? "checked" : "") + '> z-score</label>',
          ].join("<br/>");

          var cm = document.getElementById("ctl-momentum");
          var cv = document.getElementById("ctl-vwap");
          var cz = document.getElementById("ctl-zscore");
          if (cm) cm.onchange = applyControls;
          if (cv) cv.onchange = applyControls;
          if (cz) cz.onchange = applyControls;

          var w = st.window_checks || {};
          var w5 = w.s5 || {};
          var w15 = w.s15 || {};
          function line(label, item) {
            return label + ": M=" + boolHtml(item.momentum_ok) +
              " D=" + boolHtml(item.vwap_deviation_ok) +
              " Z=" + boolHtml(item.zscore_ok) +
              " ALL=" + boolHtml(item.all_ok) +
              " | mom=" + (item.momentum_pct != null ? numFmt(item.momentum_pct, 2) + "%" : "\u2014") +
              " dev=" + (item.deviation_pct != null ? numFmt(item.deviation_pct, 2) + "%" : "\u2014") +
              " z=" + (item.zscore != null ? numFmt(item.zscore, 2) : "\u2014");
          }
          document.getElementById("checks").innerHTML = [
            line("5s", w5),
            line("15s", w15),
          ].join("<br/>");
          function book(x, id) {
            var el = document.getElementById(id);
            if (!x) { el.textContent = "No data"; return; }
            var bk = x.book || {};
            var ind = x.indicators || {};
            el.innerHTML = [
              "Last " + esc(bk.last_price),
              "Bid " + esc(bk.best_bid) + " / Ask " + esc(bk.best_ask),
              "PM VWAP " + numFmt(ind.pm_vwap, 4) +
                " \u00b7 BTC VWAP " + (ind.btc_vwap_weighted != null ? numFmt(ind.btc_vwap_weighted, 4) : "\u2014"),
              "Dev " + (ind.deviation_pct != null ? numFmt(ind.deviation_pct, 2) + "%" : "\u2014") +
                " \u00b7 BTC Vol Bias " + (ind.btc_vol_ratio != null ? numFmt(ind.btc_vol_ratio, 1) + "%" : "\u2014"),
              "Z " + numFmt(ind.zscore, 2) +
                " \u00b7 Mom " + (ind.momentum_pct != null ? numFmt(ind.momentum_pct, 2) + "%" : "\u2014"),
              "Vol " + (bk.volume_total != null ? esc(Math.round(bk.volume_total)) : "\u2014"),
            ].join("<br/>");
          }
          book(d.up, "up");
          book(d.down, "down");
          var b = d.btc || {};
          var btcEl = document.getElementById("btc");
          if (b.btc_current_price > 0) {
            var btcBits = [
              "$" + esc(numFmt(b.btc_current_price, 2)),
              "Anchor $" + (b.btc_anchor_price > 0 ? esc(numFmt(b.btc_anchor_price, 2)) : "\u2014"),
              esc(b.deviation_line || ""),
            ];
            if (b.buffer_avg_abs_usd != null || b.buffer_avg_abs_pct != null) {
              var usdPart = b.buffer_avg_abs_usd != null ? "$" + esc(numFmt(b.buffer_avg_abs_usd, 2)) : "\u2014";
              var pctPart = b.buffer_avg_abs_pct != null ? esc(numFmt(b.buffer_avg_abs_pct, 3)) + "%" : "\u2014";
              btcBits.push("Buffer avg(5): +/-" + usdPart + " (+/-" + pctPart + ")");
            }
            if (b.buffer_windows && b.buffer_windows.length) {
              btcBits.push("\u2014 last 5 windows \u2014");
              for (var wi = 0; wi < b.buffer_windows.length; wi++) {
                var w = b.buffer_windows[wi];
                var wt = w.window_ts ? new Date(w.window_ts * 1000).toISOString().substr(11, 8) : "?";
                btcBits.push(esc(wt) + " $" + esc(numFmt(w.abs_usd, 2)) + " (" + esc(numFmt(w.abs_pct, 4)) + "%)");
              }
            }
            btcBits.push(
              "Feed: " + (b.btc_connected ? "ok" : "off") +
                (b.fresh_sec != null ? " \u00b7 " + Math.floor(b.fresh_sec) + "s" : "")
            );
            btcEl.innerHTML = [
              btcBits.join("<br/>")
            ];
            btcEl.innerHTML = btcBits.join("<br/>");
          } else {
            btcEl.textContent = "Waiting for Chainlink\u2026";
          }
          var tr = d.trading || {};
          var tHtml = "Markets " + esc(tr.markets_seen) + " \u00b7 Trades " + esc(tr.trade_count) +
            " \u00b7 PnL $" + (tr.total_pnl != null ? numFmt(tr.total_pnl, 2) : "\u2014") + "<br/>";
          if (tr.position) {
            var p = tr.position;
            tHtml += "LONG " + esc(p.token_name) + " @ " + esc(p.entry_price) +
              " \u00d7" + esc(p.contracts) + (p.hedged ? " hedged" : "") + "<br/>";
            tHtml += "Unreal $" + (p.unrealized_pnl != null ? numFmt(p.unrealized_pnl, 2) : "\u2014") + "<br/>";
          } else {
            tHtml += "No open position<br/>";
          }
          if (tr.recent_trades && tr.recent_trades.length) {
            var lines = [];
            for (var i = 0; i < tr.recent_trades.length; i++) {
              lines.push(esc(tr.recent_trades[i].line));
            }
            tHtml += "<br/>Recent:<br/>" + lines.join("<br/>");
          }
          document.getElementById("trading").innerHTML = tHtml;
        } catch (e) {
          errEl.textContent = "Poll error: " + (e && e.message ? e.message : e);
        }
      };
      r.onerror = function () {
        errEl.textContent = "Network error (is the bot running?)";
      };
      r.send();
    }
    tick();
    setInterval(tick, 1000);
  </script>
</body>
</html>
"""


def _sanitize_for_json(obj: Any) -> Any:
    """
    Starlette JSONResponse serializes with allow_nan=False; NaN/Inf break the ASGI handler.
    """
    if obj is None:
        return None
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(v) for v in obj]
    return obj


class WebSnapshotHolder:
    """Thread-safe snapshot for /api/state."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = {"status": "starting"}

    def set(self, data: Dict[str, Any]) -> None:
        with self._lock:
            self._data = dict(data)

    def get(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)


def build_app(
  holder: WebSnapshotHolder,
  *,
  get_indicator_controls: Optional[Callable[[], Dict[str, Any]]] = None,
  update_indicator_controls: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_late_modes: Optional[Callable[[], Dict[str, Any]]] = None,
  update_late_modes: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_volume_accel_check: Optional[Callable[[], Dict[str, Any]]] = None,
  update_volume_accel_check: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_volume_eval_mode: Optional[Callable[[], Dict[str, Any]]] = None,
  update_volume_eval_mode: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_timer_alert: Optional[Callable[[], Dict[str, Any]]] = None,
  update_timer_alert: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> FastAPI:
    app = FastAPI(title="BTC Live Bot", docs_url=None, redoc_url=None)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return _HTML

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/api/state")
    async def api_state():
        return JSONResponse(_sanitize_for_json(holder.get()))

    @app.get("/api/indicator-controls")
    async def api_get_indicator_controls():
      if not get_indicator_controls:
        return JSONResponse({"momentum": True, "vwap_deviation": True, "zscore": True})
      return JSONResponse(_sanitize_for_json(get_indicator_controls()))

    @app.post("/api/indicator-controls")
    async def api_update_indicator_controls(payload: Dict[str, Any] = Body(default={})):
      if not update_indicator_controls:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_indicator_controls(payload or {})))

    @app.get("/api/late-modes")
    async def api_get_late_modes():
      if not get_late_modes:
        return JSONResponse({"enabled": False, "modes": []})
      return JSONResponse(_sanitize_for_json(get_late_modes()))

    @app.post("/api/late-modes")
    async def api_update_late_modes(payload: Dict[str, Any] = Body(default={})):
      if not update_late_modes:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_late_modes(payload or {})))

    @app.get("/api/volume-accel-check")
    async def api_get_volume_accel_check():
      if not get_volume_accel_check:
        return JSONResponse({"min_current_volume_diff": 0.0, "min_accel_diff": 0.0, "volume_basis": "total"})
      return JSONResponse(_sanitize_for_json(get_volume_accel_check()))

    @app.post("/api/volume-accel-check")
    async def api_update_volume_accel_check(payload: Dict[str, Any] = Body(default={})):
      if not update_volume_accel_check:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_volume_accel_check(payload or {})))

    @app.get("/api/volume-eval-mode")
    async def api_get_volume_eval_mode():
      if not get_volume_eval_mode:
        return JSONResponse({"enabled": False})
      return JSONResponse(_sanitize_for_json(get_volume_eval_mode()))

    @app.post("/api/volume-eval-mode")
    async def api_update_volume_eval_mode(payload: Dict[str, Any] = Body(default={})):
      if not update_volume_eval_mode:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_volume_eval_mode(payload or {})))

    @app.get("/api/timer-alert")
    async def api_get_timer_alert():
      if not get_timer_alert:
        return JSONResponse({"enabled": False})
      return JSONResponse(_sanitize_for_json(get_timer_alert()))

    @app.post("/api/timer-alert")
    async def api_update_timer_alert(payload: Dict[str, Any] = Body(default={})):
      if not update_timer_alert:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_timer_alert(payload or {})))

    @app.post("/api/manual-buy")
    async def api_manual_buy(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy:
        return JSONResponse({"ok": False, "error": "Manual buy is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_buy(payload or {})))

    return app


def _client_probe_address(bind_host: str) -> str:
    """Address to test with socket.connect(); 0.0.0.0 / :: are not valid client targets."""
    if bind_host in ("0.0.0.0", ""):
        return "127.0.0.1"
    if bind_host in ("::", "[::]"):
        return "::1"
    return bind_host


def start_web_dashboard(
  host: str,
  port: int,
  holder: WebSnapshotHolder,
  *,
  get_indicator_controls: Optional[Callable[[], Dict[str, Any]]] = None,
  update_indicator_controls: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_late_modes: Optional[Callable[[], Dict[str, Any]]] = None,
  update_late_modes: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_volume_accel_check: Optional[Callable[[], Dict[str, Any]]] = None,
  update_volume_accel_check: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_volume_eval_mode: Optional[Callable[[], Dict[str, Any]]] = None,
  update_volume_eval_mode: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  get_timer_alert: Optional[Callable[[], Dict[str, Any]]] = None,
  update_timer_alert: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> bool:
    """
    Start uvicorn in a daemon thread. Returns True if the port accepts connections
    shortly after start (False if bind failed or port is in use).
    """
    app = build_app(
      holder,
      get_indicator_controls=get_indicator_controls,
      update_indicator_controls=update_indicator_controls,
      get_late_modes=get_late_modes,
      update_late_modes=update_late_modes,
      get_volume_accel_check=get_volume_accel_check,
      update_volume_accel_check=update_volume_accel_check,
      get_volume_eval_mode=get_volume_eval_mode,
      update_volume_eval_mode=update_volume_eval_mode,
      get_timer_alert=get_timer_alert,
      update_timer_alert=update_timer_alert,
      trigger_manual_buy=trigger_manual_buy,
    )

    def run() -> None:
        try:
            uvicorn.run(
                app,
                host=host,
                port=port,
                log_level="warning",
                access_log=False,
            )
        except Exception:
            logger.exception("Web dashboard: uvicorn exited with an error")

    t = threading.Thread(target=run, name="web-dashboard", daemon=True)
    t.start()

    probe = _client_probe_address(host)
    for _ in range(60):
        time.sleep(0.1)
        try:
            with socket.create_connection((probe, port), timeout=0.4):
                return True
        except OSError:
            continue
    return False
