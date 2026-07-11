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

from fastapi import FastAPI, Request
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
    .controls .row { margin-bottom: 0.35rem; align-items: center; }
    .controls label { min-width: 105px; color: var(--muted); font-size: 0.8rem; }
    .controls input[type="number"] { width: 86px; background: #0d1117; border: 1px solid var(--border); color: var(--text); border-radius: 6px; padding: 0.25rem 0.35rem; }
    .controls input[type="checkbox"] { transform: scale(1.05); }
    .mode-grid { display: grid; gap: 0.55rem; }
    .mode-box { border: 1px solid var(--border); border-radius: 8px; padding: 0.55rem; }
    .mode-title { color: var(--blue); font-size: 0.8rem; margin-bottom: 0.35rem; }
    .btn { background: #1f6feb; color: #fff; border: 0; border-radius: 7px; padding: 0.35rem 0.6rem; cursor: pointer; font-size: 0.8rem; }
    .btn.secondary { background: #30363d; }
    .status { color: var(--muted); font-size: 0.78rem; }
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
    <div class="card"><h2>UP</h2><div id="up" class="mono"></div></div>
    <div class="card"><h2>DOWN</h2><div id="down" class="mono"></div></div>
    <div class="card btc"><h2>BTC / USD Sources</h2><div id="btc" class="mono"></div></div>
    <div class="card controls"><h2>Late Entry Modes</h2><div id="lateModes" class="mono">Loading…</div></div>
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
    function numFmtSigned(n, dec) {
      if (n === null || n === undefined || typeof n !== "number" || isNaN(n)) return "\u2014";
      var fixed = n.toFixed(dec);
      if (n > 0) return "+" + fixed;
      return fixed;
    }
    function readNum(id, fallback) {
      var el = document.getElementById(id);
      if (!el) return fallback;
      var v = parseFloat(el.value);
      return isNaN(v) ? fallback : v;
    }
    function readInt(id, fallback) {
      var el = document.getElementById(id);
      if (!el) return fallback;
      var v = parseInt(el.value, 10);
      return isNaN(v) ? fallback : v;
    }

    function buildModeEditor(mode) {
      var k = mode.key;
      var h = [];
      h.push('<div class="mode-box">');
      h.push('<div class="mode-title">' + esc(k) + '</div>');
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="' + esc(k) + '_enabled" ' + (mode.enabled ? 'checked' : '') + '/></div>');
      h.push('<div class="row"><label>Time Left</label><input type="number" id="' + esc(k) + '_time_left_sec" step="1" value="' + esc(mode.time_left_sec) + '"/></div>');
      h.push('<div class="row"><label>Min Contracts</label><input type="number" id="' + esc(k) + '_min_contracts" step="1" value="' + esc(mode.min_contracts) + '"/></div>');
      h.push('<div class="row"><label>Max Trades</label><input type="number" id="' + esc(k) + '_max_trades" step="1" value="' + esc(mode.max_trades) + '"/></div>');
      h.push('<div class="row"><label>Buffer Mult</label><input type="number" id="' + esc(k) + '_buffer_avg_multiplier" step="0.01" value="' + esc(mode.buffer_avg_multiplier) + '"/></div>');
      h.push('<div class="row"><label>Min Buffer $</label><input type="number" id="' + esc(k) + '_min_buffer_threshold_usd" step="0.1" value="' + esc(mode.min_buffer_threshold_usd) + '"/></div>');
      h.push('<div class="row"><label>Min Price</label><input type="number" id="' + esc(k) + '_min_price" step="0.001" value="' + esc(mode.min_price) + '"/></div>');
      h.push('<div class="row"><label>Max Price</label><input type="number" id="' + esc(k) + '_max_price" step="0.001" value="' + esc(mode.max_price) + '"/></div>');
      h.push('</div>');
      return h.join('');
    }

    function renderLateModes(cfg) {
      var box = document.getElementById('lateModes');
      if (!box) return;
      if (!cfg || !cfg.modes) {
        box.textContent = 'Late mode config unavailable';
        return;
      }

      var html = [];
      html.push('<div class="row"><label>Enabled</label><input type="checkbox" id="late_enabled" ' + (cfg.enabled ? 'checked' : '') + '/></div>');
      html.push('<div class="row"><label>Total Max</label><input type="number" id="late_total_max_trades" step="1" value="' + esc(cfg.total_max_trades) + '"/></div>');
      html.push('<div class="mode-grid">');
      for (var i = 0; i < cfg.modes.length; i++) {
        html.push(buildModeEditor(cfg.modes[i]));
      }
      html.push('</div>');
      html.push('<div style="margin-top:0.5rem" class="row">');
      html.push('<button class="btn" onclick="saveLateModes()">Apply</button>');
      html.push('<button class="btn secondary" onclick="loadLateModes()">Reload</button>');
      html.push('</div>');
      html.push('<div id="lateStatus" class="status"></div>');
      box.innerHTML = html.join('');
    }

    function loadLateModes() {
      var r = new XMLHttpRequest();
      r.open('GET', '/api/late-modes', true);
      r.onreadystatechange = function() {
        if (r.readyState !== 4) return;
        if (r.status !== 200) {
          var box = document.getElementById('lateModes');
          if (box) box.textContent = 'Could not load late mode config (HTTP ' + r.status + ')';
          return;
        }
        try {
          var cfg = JSON.parse(r.responseText);
          renderLateModes(cfg);
        } catch (e) {
          var box2 = document.getElementById('lateModes');
          if (box2) box2.textContent = 'Late mode parse error';
        }
      };
      r.send();
    }

    function saveLateModes() {
      var status = document.getElementById('lateStatus');
      var keys = ['mode_60s', 'mode_40s', 'mode_30s', 'mode_20s'];
      var modes = [];
      for (var i = 0; i < keys.length; i++) {
        var k = keys[i];
        modes.push({
          key: k,
          enabled: !!(document.getElementById(k + '_enabled') && document.getElementById(k + '_enabled').checked),
          time_left_sec: readInt(k + '_time_left_sec', 0),
          min_contracts: readInt(k + '_min_contracts', 1),
          max_trades: readInt(k + '_max_trades', 1),
          buffer_avg_multiplier: readNum(k + '_buffer_avg_multiplier', 1.0),
          min_buffer_threshold_usd: readNum(k + '_min_buffer_threshold_usd', 0.0),
          min_price: readNum(k + '_min_price', 0.0),
          max_price: readNum(k + '_max_price', 1.0)
        });
      }

      var payload = {
        enabled: !!(document.getElementById('late_enabled') && document.getElementById('late_enabled').checked),
        total_max_trades: readInt('late_total_max_trades', 1),
        modes: modes
      };

      var r = new XMLHttpRequest();
      r.open('POST', '/api/late-modes', true);
      r.setRequestHeader('Content-Type', 'application/json');
      r.onreadystatechange = function() {
        if (r.readyState !== 4) return;
        if (r.status === 200) {
          if (status) status.textContent = 'Applied';
          loadLateModes();
        } else {
          if (status) status.textContent = 'Apply failed (HTTP ' + r.status + ')';
        }
      };
      r.send(JSON.stringify(payload));
    }

    function manualBuyWithDirection(direction) {
      var status = document.getElementById('buyStatus');
      var amount = readNum('buyAmount', 0);
      if (!(amount > 0)) {
        if (status) status.textContent = 'Amount must be > 0';
        return;
      }
      if (status) status.textContent = 'Submitting...';

      var r = new XMLHttpRequest();
      r.open('POST', '/api/manual-buy', true);
      r.setRequestHeader('Content-Type', 'application/json');
      r.onreadystatechange = function() {
        if (r.readyState !== 4) return;
        var txt = '';
        if (r.status === 200) {
          try {
            var out = JSON.parse(r.responseText);
            txt = out.message || (out.signal ? ('Queued ' + out.signal) : 'Queued');
          } catch (e) {
            txt = 'Queued';
          }
        } else {
          try {
            var err = JSON.parse(r.responseText);
            txt = (err && err.error) ? err.error : ('Request failed (HTTP ' + r.status + ')');
          } catch (e2) {
            txt = 'Request failed (HTTP ' + r.status + ')';
          }
        }
        if (status) status.textContent = txt;
      };
      r.send(JSON.stringify({ amount_usd: amount, direction: direction }));
    }

    function manualBuyUp() {
      manualBuyWithDirection('UP');
    }

    function manualBuyDown() {
      manualBuyWithDirection('DOWN');
    }

    function tick() {
      var errEl = document.getElementById("err");
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
          var existingAmountEl = document.getElementById("buyAmount");
          var buyAmountVal = existingAmountEl ? existingAmountEl.value : "";
          var existingBuyStatusEl = document.getElementById("buyStatus");
          var buyStatusVal = existingBuyStatusEl ? existingBuyStatusEl.textContent : "";
          var liveStatusVal = d.manual_buy_live_status ? String(d.manual_buy_live_status) : "idle";
          var defaultBuyAmount = (d.trading && typeof d.trading.bet_usd === "number" && !isNaN(d.trading.bet_usd))
            ? d.trading.bet_usd
            : 1;
          if (!buyAmountVal) buyAmountVal = String(defaultBuyAmount);

          document.getElementById("session").innerHTML = [
            "Timer: " + (hdr.time_left_sec != null ? esc(Math.floor(hdr.time_left_sec) + "s left") : "\u2014"),
            "WS: " + (hdr.ws_connected ? "live" : "disconnected"),
            "Mode: " + (hdr.simulation ? "simulation" : "real"),
            'Live: ' + esc(liveStatusVal),
            '<span>Amount $ <input type="number" id="buyAmount" min="0.1" step="0.1" value="' + esc(buyAmountVal) + '" style="width:86px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.2rem 0.3rem;"/> <button class="btn" onclick="manualBuyUp()">Buy UP</button> <button class="btn" onclick="manualBuyDown()">Buy DOWN</button> <span id="buyStatus" class="status">' + esc(buyStatusVal) + '</span></span>'
          ].join("<br/>");
          var st = d.strategy || {};
          var sig = st.signal_text || "\u2014";
          function chk(x) { return x === true ? "\u2713" : x === false ? "\u2717" : "\u2014"; }
          var ck = st.checks || {};
          var strategyBits = [
            "Fav: " + esc(st.favorite) + " \u00b7 WR: " + esc(st.win_rate_str),
            "Checks: P=" + chk(ck.price) + " T=" + chk(ck.time) + " D=" + chk(ck.dev) +
            " M=" + chk(ck.mom) + " R=" + chk(ck.trend) + " B=" + chk(ck.btc_buffer) + " cutoff=" + chk(ck.time_cutoff)
          ];
          var trend = st.trend || {};
          if (trend.window_sec != null) {
            strategyBits.push(
              "Trend " + esc(trend.window_sec) + "s: " +
              "UP " + (trend.up_delta != null ? esc(numFmt(trend.up_delta, 4)) : "\u2014") +
              " (" + chk(trend.up_ok) + ")" +
              " | DOWN " + (trend.down_delta != null ? esc(numFmt(trend.down_delta, 4)) : "\u2014") +
              " (" + chk(trend.down_ok) + ")"
            );
          }
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
          if ((b.btc_connected && b.btc_current_price > 0) || (b.binance_connected && b.binance_current_price > 0)) {
            var sourceMap = {
              "official_ptb": "official PTB",
              "fallback_tick": "fallback tick",
              "feed_tick": "feed tick",
              "none": "pending"
            };
            var marketAnchorSource = sourceMap[b.btc_market_anchor_source] || (b.btc_market_anchor_source || "pending");
            var selectedFeed = b.btc_feed_source || "chainlink";
            var chainlinkLabel = selectedFeed === "chainlink" ? "Selected feed (Polymarket RTDS)" : "Polymarket RTDS";
            var binanceLabel = selectedFeed === "binance" ? "Selected feed (Binance)" : "Binance";
            var chainlinkPrice = (b.btc_current_price > 0) ? ("$" + esc(numFmt(b.btc_current_price, 2))) : "\u2014";
            var binancePrice = (b.binance_current_price > 0) ? ("$" + esc(numFmt(b.binance_current_price, 2))) : "\u2014";
            var chainlinkFeedLine = chainlinkLabel + ": " + chainlinkPrice +
              " | " + (b.btc_connected ? "ok" : "off") +
              (b.fresh_sec != null ? " \u00b7 " + Math.floor(b.fresh_sec) + "s" : "");
            var binanceFeedLine = binanceLabel + ": " + binancePrice +
              " | " + (b.binance_connected ? "ok" : "off") +
              (b.binance_fresh_sec != null ? " \u00b7 " + Math.floor(b.binance_fresh_sec) + "s" : "");
            var btcBits = [
              chainlinkFeedLine,
              binanceFeedLine,
              "Anchor $" + (b.btc_anchor_price > 0 ? esc(numFmt(b.btc_anchor_price, 2)) : "\u2014"),
              "Market Anchor $" + (b.btc_market_anchor_price > 0 ? esc(numFmt(b.btc_market_anchor_price, 2)) : "\u2014") + " (" + esc(marketAnchorSource) + ")",
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
            if (b.btc_anchor_history && b.btc_anchor_history.length) {
              btcBits.push("\u2014 recent BTC / anchor \u2014");
              for (var hi = b.btc_anchor_history.length - 1; hi >= 0; hi--) {
                var h = b.btc_anchor_history[hi];
                var ht = h.window_ts ? new Date(h.window_ts * 1000).toISOString().substr(11, 8) : (h.ts ? new Date(h.ts * 1000).toISOString().substr(11, 8) : "?");
                btcBits.push(
                  esc(ht) +
                  " BTC $" + esc(numFmt(h.btc_price, 2)) +
                  " | A $" + esc(numFmt(h.anchor_price, 2))
                );
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
            btcEl.textContent = "Waiting for BTC feeds\u2026";
          }
          
          var tr = d.trading || {};
          var tHtml = "Markets " + esc(tr.markets_seen) + " \u00b7 Trades " + esc(tr.trade_count) +
            " \u00b7 PnL $" + (tr.total_pnl != null ? numFmtSigned(tr.total_pnl, 2) : "\u2014") + "<br/>";
          if (tr.position) {
            var p = tr.position;
            tHtml += "LONG " + esc(p.token_name) + " @ " + esc(p.entry_price) +
              " \u00d7" + esc(p.contracts) + (p.hedged ? " hedged" : "") + "<br/>";
            tHtml += "Unreal $" + (p.unrealized_pnl != null ? numFmtSigned(p.unrealized_pnl, 2) : "\u2014") + "<br/>";
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
    loadLateModes();
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
  get_late_modes: Optional[Callable[[], Dict[str, Any]]] = None,
  update_late_modes: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
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

    @app.get("/api/late-modes")
    async def api_late_modes_get():
      if not get_late_modes:
        return JSONResponse({"error": "Late mode controls unavailable"}, status_code=501)
      return JSONResponse(_sanitize_for_json(get_late_modes()))

    @app.post("/api/late-modes")
    async def api_late_modes_post(req: Request):
      if not update_late_modes:
        return JSONResponse({"error": "Late mode controls unavailable"}, status_code=501)
      payload = await req.json()
      updated = update_late_modes(payload if isinstance(payload, dict) else {})
      return JSONResponse(_sanitize_for_json(updated))

    @app.post("/api/manual-buy")
    async def api_manual_buy(req: Request):
      if not trigger_manual_buy:
        return JSONResponse({"error": "Manual buy unavailable"}, status_code=501)
      payload: Dict[str, Any] = {}
      try:
        parsed = await req.json()
        if isinstance(parsed, dict):
          payload = parsed
      except Exception:
        payload = {}
      result = trigger_manual_buy(payload)
      if not isinstance(result, dict):
        return JSONResponse({"error": "Manual buy failed"}, status_code=500)
      if result.get("ok"):
        return JSONResponse(_sanitize_for_json(result))
      return JSONResponse(_sanitize_for_json(result), status_code=400)

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
  get_late_modes: Optional[Callable[[], Dict[str, Any]]] = None,
  update_late_modes: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
) -> bool:
    """
    Start uvicorn in a daemon thread. Returns True if the port accepts connections
    shortly after start (False if bind failed or port is in use).
    """
    app = build_app(
        holder,
        get_late_modes=get_late_modes,
        update_late_modes=update_late_modes,
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
