"""
Local web dashboard: FastAPI + single-page UI, JSON at /api/state.
Runs in a daemon thread; state is updated from the bot's main loop.
"""

from __future__ import annotations

import math
import logging
import socket
import threading
import time
from typing import Any, Callable, Dict, Optional

import uvicorn
from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response

logger = logging.getLogger("web_dashboard")

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>BTC Live Bot</title>
  <style>
    :root { --bg:#0d1117; --panel:#161b22; --border:#30363d; --text:#e6edf3; --muted:#8b949e; --green:#3fb950; --red:#f85149; --yellow:#d29922; --blue:#58a6ff; }
    * { box-sizing: border-box; }
    body { font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 1rem; line-height: 1.45; }
    h1 { font-size: 1.1rem; font-weight: 600; margin: 0 0 0.75rem; }
    .meta { color: var(--muted); font-size: 0.85rem; margin-bottom: 1rem; }
    .grid { display: grid; gap: 0.75rem; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
    .card { background: var(--panel); border: 1px solid var(--border); border-radius: 8px; padding: 0.85rem; }
    .card h2 { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); margin: 0 0 0.5rem; }
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
  <div class="meta" id="meta">Loading...</div>
  <div class="grid">
    <div class="card"><h2>Session</h2><div id="session" class="mono"></div></div>
    <div class="card"><h2>Strategy</h2><div id="strategy"></div></div>
    <div class="card btc"><h2>BTC / USD (Chainlink)</h2><div id="btc" class="mono"></div></div>
    <div class="card"><h2>Trading</h2><div id="trading" class="mono"></div></div>
    <!-- TEMPORARILY REMOVED: Mode Performance -->
    <!-- <div class="card"><h2>Mode Performance</h2><div id="modePerf" class="mono"></div></div> -->
    <div class="card controls"><h2>Telegram Timer Alert</h2><div id="timerAlertPanel" class="mono">Loading...</div></div>
    <!-- TEMPORARILY REMOVED: Late Entry Modes -->
    <!-- <div class="card controls"><h2>Late Entry Modes</h2><div id="lateModePanel" class="mono">Loading...</div></div> -->
    <!-- TEMPORARILY REMOVED: Volume Eval Mode -->
    <!-- <div class="card controls"><h2>Volume Eval Mode</h2><div id="volumeEvalPanel" class="mono">Loading...</div></div> -->
    <div class="card controls"><h2>Telegram Streak Alert</h2><div id="streakAlertPanel" class="mono">Loading...</div></div>
    <div class="card"><h2>Streak End Counts</h2><div id="streakEnds" class="mono">Loading...</div></div>
  </div>
  <footer>Refreshes every second · <span id="err"></span></footer>
  <script>
    function esc(s){ if(s===null||s===undefined) return ''; var el=document.createElement('div'); el.textContent=String(s); return el.innerHTML; }
    function sigClass(t){ if(!t) return 'wait'; if(t.indexOf('BUY')>=0) return 'buy'; if(t.indexOf('NO ENTRY')>=0) return 'block'; return 'wait'; }
    function numFmt(n,d){ if(n===null||n===undefined||typeof n!=='number'||isNaN(n)) return '\u2014'; return n.toFixed(d); }
    function boolHtml(v){ if(v===true) return '\u2713'; if(v===false) return '\u2717'; return '\u2014'; }
    function readNum(id,f){ var el=document.getElementById(id); if(!el) return f; var v=parseFloat(el.value); return isNaN(v)?f:v; }
    function readInt(id,f){ var el=document.getElementById(id); if(!el) return f; var v=parseInt(el.value,10); return isNaN(v)?f:v; }
    function requestJson(method,url,payload,onDone){ var r=new XMLHttpRequest(); r.open(method,url,true); if(payload!==null&&payload!==undefined){ r.setRequestHeader('Content-Type','application/json'); } r.onreadystatechange=function(){ if(r.readyState!==4) return; if(r.status<200||r.status>=300) return; try{ var d=JSON.parse(r.responseText); if(onDone) onDone(d);}catch(e){} }; r.send(payload!==null&&payload!==undefined?JSON.stringify(payload):null); }

    var modeCfg={late:null,volEval:null,volAccel:null};
  var timerAlertCfg=null;
  var streakAlertCfg=null;
    window.latestModePerfData={};
    var pollTimer=null;
    var pollInFlight=false;
    var pollSeq=0;
    var isMobile=/Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent||'');
    var POLL_MS_ACTIVE=1000;
    var POLL_MS_HIDDEN=5000;
    var lastHtml={meta:'',session:'',strategy:'',btc:'',trading:'',modePerf:'',streakEnds:''};

    function setHtmlIfChanged(id,htmlKey,html){
      var el=document.getElementById(id);
      if(!el) return;
      if(lastHtml[htmlKey]===html) return;
      lastHtml[htmlKey]=html;
      el.innerHTML=html;
    }

    function scheduleTick(ms){
      if(pollTimer){ clearTimeout(pollTimer); pollTimer=null; }
      pollTimer=setTimeout(tick, Math.max(250, ms||POLL_MS_ACTIVE));
    }

    function numFmtSigned(n,d){ if(n===null||n===undefined||typeof n!=='number'||isNaN(n)) return '\u2014'; var fixed=n.toFixed(d); if(n>0) return '+'+fixed; return fixed; }

    function buildLateModeEditor(mode,modePerfData){
      var k=mode.key, h=[];
      h.push('<div class="mode-box">');
      h.push('<div class="mode-title">'+esc(k)+'</div>');
      if(modePerfData&&modePerfData[k]){ var perf=modePerfData[k]||{}; var pnl=perf.total_pnl_usd!=null?numFmtSigned(perf.total_pnl_usd,2):'\u2014'; var trades=perf.trade_count||0; var wr=perf.win_rate_pct!=null?numFmt(perf.win_rate_pct,1)+'%':'\u2014'; h.push('<div style="font-size:0.75rem;color:#8b949e;margin-bottom:0.25rem">PnL $'+esc(pnl)+' | Trades '+esc(trades)+' | WR '+esc(wr)+'</div>'); }
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="'+esc(k)+'_enabled" '+(mode.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Time Left</label><input type="number" id="'+esc(k)+'_time_left_sec" step="1" value="'+esc(mode.time_left_sec)+'"/></div>');
      h.push('<div class="row"><label>Min Contracts</label><input type="number" id="'+esc(k)+'_min_contracts" step="1" value="'+esc(mode.min_contracts)+'"/></div>');
      h.push('<div class="row"><label>Max Trades</label><input type="number" id="'+esc(k)+'_max_trades" step="1" value="'+esc(mode.max_trades)+'"/></div>');
      h.push('<div class="row"><label>Buffer Mult</label><input type="number" id="'+esc(k)+'_buffer_avg_multiplier" step="0.01" value="'+esc(mode.buffer_avg_multiplier)+'"/></div>');
      h.push('<div class="row"><label>Min Buffer $</label><input type="number" id="'+esc(k)+'_min_buffer_threshold_usd" step="0.1" value="'+esc(mode.min_buffer_threshold_usd)+'"/></div>');
      h.push('<div class="row"><label>Min Price</label><input type="number" id="'+esc(k)+'_min_price" step="0.001" value="'+esc(mode.min_price)+'"/></div>');
      h.push('<div class="row"><label>Max Price</label><input type="number" id="'+esc(k)+'_max_price" step="0.001" value="'+esc(mode.max_price)+'"/></div>');
      h.push('</div>');
      return h.join('');
    }

    function renderLateModePanelConfig(modePerfData){
      var box=document.getElementById('lateModePanel'); if(!box) return;
      var lm=modeCfg.late||{};
      if(!lm||!lm.modes){ box.textContent='Late mode config unavailable'; return; }
      var h=[];
      h.push('<div class="row"><label>Late Enabled</label><input type="checkbox" id="late_enabled" '+(lm.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Total Max</label><input type="number" id="late_total_max_trades" step="1" value="'+esc(lm.total_max_trades!=null?lm.total_max_trades:1)+'"/></div>');
      h.push('<div class="mode-grid">');
      for(var i=0;i<lm.modes.length;i++){ h.push(buildLateModeEditor(lm.modes[i]||{},modePerfData)); }
      h.push('</div>');
      h.push('<div style="margin-top:0.5rem" class="row"><button class="btn" onclick="saveLateModeConfig()">Apply</button><button class="btn secondary" onclick="loadLateModeConfig()">Reload</button></div>');
      h.push('<div id="lateModeStatus" class="status"></div>');
      box.innerHTML=h.join('<br/>');
    }

    function renderVolumeEvalPanelConfig(){
      var box=document.getElementById('volumeEvalPanel'); if(!box) return;
      var ve=modeCfg.volEval||{}, va=modeCfg.volAccel||{};
      var h=[];
      h.push('<div class="mode-box"><div class="mode-title">volume_eval_mode</div>');
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="ve_enabled" '+(ve.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Time Left</label><input type="number" id="ve_time_left_sec" step="1" value="'+esc(ve.time_left_sec!=null?ve.time_left_sec:0)+'"/></div>');
      h.push('<div class="row"><label>Min Contracts</label><input type="number" id="ve_min_contracts" step="1" value="'+esc(ve.min_contracts!=null?ve.min_contracts:1)+'"/></div>');
      h.push('<div class="row"><label>Max Trades</label><input type="number" id="ve_max_trades" step="1" value="'+esc(ve.max_trades!=null?ve.max_trades:1)+'"/></div>');
      h.push('<div class="row"><label>Buffer Mult</label><input type="number" id="ve_buffer_avg_multiplier" step="0.01" value="'+esc(ve.buffer_avg_multiplier!=null?ve.buffer_avg_multiplier:1.0)+'"/></div>');
      h.push('<div class="row"><label>Min Buffer $</label><input type="number" id="ve_min_buffer_threshold_usd" step="0.1" value="'+esc(ve.min_buffer_threshold_usd!=null?ve.min_buffer_threshold_usd:0.0)+'"/></div>');
      h.push('<div class="row"><label>Entry Vol 1</label><input type="number" id="ve_entry_vol_1" step="1" value="'+esc((ve.entry_min_current_volume_diffs&&ve.entry_min_current_volume_diffs.length>0)?ve.entry_min_current_volume_diffs[0]:1000)+'"/></div>');
      h.push('<div class="row"><label>Entry Vol 2</label><input type="number" id="ve_entry_vol_2" step="1" value="'+esc((ve.entry_min_current_volume_diffs&&ve.entry_min_current_volume_diffs.length>1)?ve.entry_min_current_volume_diffs[1]:2000)+'"/></div>');
      h.push('<div class="row"><label>Entry Vol 3</label><input type="number" id="ve_entry_vol_3" step="1" value="'+esc((ve.entry_min_current_volume_diffs&&ve.entry_min_current_volume_diffs.length>2)?ve.entry_min_current_volume_diffs[2]:3000)+'"/></div>');
      h.push('<div class="row"><label>Entry Limit 1</label><input type="number" id="ve_entry_limit_1" step="1" value="'+esc((ve.entry_trade_limits&&ve.entry_trade_limits.length>0)?ve.entry_trade_limits[0]:1)+'"/></div>');
      h.push('<div class="row"><label>Entry Limit 2</label><input type="number" id="ve_entry_limit_2" step="1" value="'+esc((ve.entry_trade_limits&&ve.entry_trade_limits.length>1)?ve.entry_trade_limits[1]:1)+'"/></div>');
      h.push('<div class="row"><label>Entry Limit 3</label><input type="number" id="ve_entry_limit_3" step="1" value="'+esc((ve.entry_trade_limits&&ve.entry_trade_limits.length>2)?ve.entry_trade_limits[2]:1)+'"/></div>');
      h.push('<div class="row"><label>Min Price</label><input type="number" id="ve_min_price" step="0.001" value="'+esc(ve.min_price!=null?ve.min_price:0.0)+'"/></div>');
      h.push('<div class="row"><label>Max Price</label><input type="number" id="ve_max_price" step="0.001" value="'+esc(ve.max_price!=null?ve.max_price:1.0)+'"/></div>');
      h.push('</div>');
      h.push('<div class="mode-box"><div class="mode-title">volume_accel_check</div>');
      h.push('<div class="row"><label>Min Curr Diff</label><input type="number" id="va_min_current_volume_diff" step="1" value="'+esc(va.min_current_volume_diff!=null?va.min_current_volume_diff:0.0)+'"/></div>');
      h.push('<div class="row"><label>Min Accel Diff</label><input type="number" id="va_min_accel_diff" step="1" value="'+esc(va.min_accel_diff!=null?va.min_accel_diff:0.0)+'"/></div>');
      h.push('<div class="row"><label>Volume Basis</label><select id="va_volume_basis"><option value="total"'+(va.volume_basis==='total'?' selected':'')+'>total</option><option value="buy"'+(va.volume_basis==='buy'?' selected':'')+'>buy</option></select></div>');
      h.push('</div>');
      h.push('<div style="margin-top:0.5rem" class="row"><button class="btn" onclick="saveVolumeEvalConfig()">Apply</button><button class="btn secondary" onclick="loadVolumeEvalConfig()">Reload</button></div>');
      h.push('<div id="volumeEvalStatus" class="status"></div>');
      box.innerHTML=h.join('<br/>');
    }

    function renderTimerAlertPanel(stateData){
      var box=document.getElementById('timerAlertPanel'); if(!box) return;
      var t=timerAlertCfg||{};
      var h=[];
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="ta_enabled" '+(t.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Timer Bot</label><span>'+(t.timer_bot_ready?'ready':'missing token/chat')+'</span></div>');
      h.push('<div class="row"><label>Alert Time Left</label><input type="number" id="ta_time_left_sec" step="1" min="0" value="'+esc(t.time_left_sec!=null?t.time_left_sec:100)+'"/></div>');
      h.push('<div class="row"><label>Min Price</label><input type="number" id="ta_min_price" step="0.001" min="0" max="1" value="'+esc(t.min_price!=null?t.min_price:0.75)+'"/></div>');
      h.push('<div class="row"><label>Max Price</label><input type="number" id="ta_max_price" step="0.001" min="0" max="1" value="'+esc(t.max_price!=null?t.max_price:0.95)+'"/></div>');
      h.push('<div class="row"><label>Buffer Mult</label><input type="number" id="ta_btc_buffer_multiplier" step="0.01" min="0" value="'+esc(t.btc_buffer_multiplier!=null?t.btc_buffer_multiplier:0)+'"/></div>');

      h.push('<div style="margin-top:0.5rem" class="row"><button class="btn" onclick="saveTimerAlertConfig()">Apply</button><button class="btn secondary" onclick="loadTimerAlertConfig()">Reload</button></div>');
      h.push('<div id="timerAlertStatus" class="status"></div>');
      box.innerHTML=h.join('<br/>');
    }

    function renderStreakAlertPanel(){
      var box=document.getElementById('streakAlertPanel'); if(!box) return;
      var s=streakAlertCfg||{};
      var h=[];
      var runTxt='none';
      var cs=parseInt(s.current_streak)||0;
      var cd=String(s.current_direction||'').trim();
      if(cs>0&&cd){ runTxt=cs+'x '+cd; }
      h.push('<div class="row"><label>Enabled</label><input type="checkbox" id="sa_enabled" '+(s.enabled?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Current Run</label><span id="sa_current_run">'+esc(runTxt)+'</span></div>');
      h.push('<div class="row"><label>Min Streak</label><input type="number" id="sa_min_streak" step="1" min="2" value="'+esc(s.min_streak!=null?s.min_streak:3)+'"/></div>');
      h.push('<div class="row"><label>Alert Each Extension</label><input type="checkbox" id="sa_notify_every_extension" '+(s.notify_every_extension?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Alert On Startup</label><input type="checkbox" id="sa_notify_on_startup" '+(s.notify_on_startup?'checked':'')+'/></div>');
      h.push('<div class="row"><label>Use Timer Bot</label><input type="checkbox" id="sa_use_timer_bot" '+(s.use_timer_bot?'checked':'')+'/></div>');
      if(s.use_timer_bot&&!s.timer_bot_ready){ h.push('<div class="row"><label>Timer Bot</label><span>missing token/chat</span></div>'); }
      h.push('<div style="margin-top:0.5rem" class="row"><button class="btn" onclick="saveStreakAlertConfig()">Apply</button><button class="btn secondary" onclick="loadStreakAlertConfig()">Reload</button></div>');
      h.push('<div id="streakAlertStatus" class="status"></div>');
      box.innerHTML=h.join('<br/>');
    }

    function loadStreakAlertConfig(onDone){
      requestJson('GET','/api/streak-alert',null,function(cfg){
        streakAlertCfg=cfg||{};
        renderStreakAlertPanel();
        if(onDone) onDone(streakAlertCfg);
      });
    }

    function saveStreakAlertConfig(){
      var status=document.getElementById('streakAlertStatus'); if(status) status.textContent='Applying...';
      var s=streakAlertCfg||{};
      var payload={
        enabled:!!(document.getElementById('sa_enabled')&&document.getElementById('sa_enabled').checked),
        min_streak:readInt('sa_min_streak',s.min_streak||3),
        notify_every_extension:!!(document.getElementById('sa_notify_every_extension')&&document.getElementById('sa_notify_every_extension').checked),
        notify_on_startup:!!(document.getElementById('sa_notify_on_startup')&&document.getElementById('sa_notify_on_startup').checked),
        use_timer_bot:!!(document.getElementById('sa_use_timer_bot')&&document.getElementById('sa_use_timer_bot').checked)
      };
      requestJson('POST','/api/streak-alert',payload,function(resp){
        streakAlertCfg=resp||payload;
        if(status) status.textContent='Applied';
        renderStreakAlertPanel();
      });
    }

    function loadLateModeConfig(){
      requestJson('GET','/api/late-modes',null,function(late){
        modeCfg.late=late||{};
        renderLateModePanelConfig(window.latestModePerfData||{});
      });
    }

    function loadVolumeEvalConfig(){
      requestJson('GET','/api/volume-eval-mode',null,function(ve){
        modeCfg.volEval=ve||{};
        requestJson('GET','/api/volume-accel-check',null,function(va){
          modeCfg.volAccel=va||{};
          renderVolumeEvalPanelConfig();
        });
      });
    }

    function loadTimerAlertConfig(onDone){
      requestJson('GET','/api/timer-alert',null,function(cfg){
        timerAlertCfg=cfg||{};
        renderTimerAlertPanel(null);
        if(onDone) onDone(timerAlertCfg);
      });
    }

    function saveTimerAlertConfig(){
      var status=document.getElementById('timerAlertStatus'); if(status) status.textContent='Applying...';
      var t=timerAlertCfg||{};
      var payload={
        enabled:!!(document.getElementById('ta_enabled')&&document.getElementById('ta_enabled').checked),
        time_left_sec:readInt('ta_time_left_sec',t.time_left_sec||100),
        min_price:readNum('ta_min_price',t.min_price||0.75),
        max_price:readNum('ta_max_price',t.max_price||0.95),
        btc_buffer_multiplier:readNum('ta_btc_buffer_multiplier',t.btc_buffer_multiplier||0)
      };
      requestJson('POST','/api/timer-alert',payload,function(resp){
        timerAlertCfg=resp||payload;
        if(status) status.textContent='Applied';
        renderTimerAlertPanel(null);
      });
    }

    function saveLateModeConfig(){
      var status=document.getElementById('lateModeStatus'); if(status) status.textContent='Applying...';
      var late=modeCfg.late||{}, lateModes=[], src=late.modes||[];
      for(var i=0;i<src.length;i++){
        var m=src[i]||{}, k=String(m.key||('mode_'+i));
        lateModes.push({ key:k, enabled:!!(document.getElementById(k+'_enabled')&&document.getElementById(k+'_enabled').checked), time_left_sec:readInt(k+'_time_left_sec',m.time_left_sec||0), min_contracts:readInt(k+'_min_contracts',m.min_contracts||1), max_trades:readInt(k+'_max_trades',m.max_trades||1), buffer_avg_multiplier:readNum(k+'_buffer_avg_multiplier',m.buffer_avg_multiplier||1.0), min_buffer_threshold_usd:readNum(k+'_min_buffer_threshold_usd',m.min_buffer_threshold_usd||0.0), min_price:readNum(k+'_min_price',m.min_price||0.0), max_price:readNum(k+'_max_price',m.max_price||1.0) });
      }
      var latePayload={ enabled:!!(document.getElementById('late_enabled')&&document.getElementById('late_enabled').checked), total_max_trades:readInt('late_total_max_trades',late.total_max_trades||1), modes:lateModes };
      requestJson('POST','/api/late-modes',latePayload,function(lateResp){
        modeCfg.late=lateResp||latePayload;
        if(status) status.textContent='Applied';
        renderLateModePanelConfig(window.latestModePerfData||{});
      });
    }

    function saveVolumeEvalConfig(){
      var status=document.getElementById('volumeEvalStatus'); if(status) status.textContent='Applying...';
      var ve=modeCfg.volEval||{};
      var vePayload={ enabled:!!(document.getElementById('ve_enabled')&&document.getElementById('ve_enabled').checked), time_left_sec:readInt('ve_time_left_sec',ve.time_left_sec||0), min_contracts:readInt('ve_min_contracts',ve.min_contracts||1), max_trades:readInt('ve_max_trades',ve.max_trades||1), buffer_avg_multiplier:readNum('ve_buffer_avg_multiplier',ve.buffer_avg_multiplier||1.0), min_buffer_threshold_usd:readNum('ve_min_buffer_threshold_usd',ve.min_buffer_threshold_usd||0.0), volume_check_enabled:(document.getElementById('ve_volume_check_enabled')?!!document.getElementById('ve_volume_check_enabled').checked:!!ve.volume_check_enabled), entry_min_current_volume_diffs:[readNum('ve_entry_vol_1',1000),readNum('ve_entry_vol_2',2000),readNum('ve_entry_vol_3',3000)], entry_trade_limits:[readInt('ve_entry_limit_1',1),readInt('ve_entry_limit_2',1),readInt('ve_entry_limit_3',1)], min_price:readNum('ve_min_price',ve.min_price||0.0), max_price:readNum('ve_max_price',ve.max_price||1.0) };
      var va=modeCfg.volAccel||{}, basisEl=document.getElementById('va_volume_basis');
      var vaPayload={ min_current_volume_diff:readNum('va_min_current_volume_diff',va.min_current_volume_diff||0.0), min_accel_diff:readNum('va_min_accel_diff',va.min_accel_diff||0.0), volume_basis:basisEl?String(basisEl.value||'total'):'total' };
      requestJson('POST','/api/volume-eval-mode',vePayload,function(veResp){
        modeCfg.volEval=veResp||vePayload;
        requestJson('POST','/api/volume-accel-check',vaPayload,function(vaResp){
          modeCfg.volAccel=vaResp||vaPayload;
          if(status) status.textContent='Applied';
          renderVolumeEvalPanelConfig();
        });
      });
    }

    function manualBuyWithDirection(direction){
      var status=document.getElementById('buyStatus');
      var amount=readNum('buyAmount',0);
      if(!(amount>0)){
        if(status) status.textContent='Amount must be > 0';
        return;
      }
      if(status) status.textContent='Submitting...';
      requestJson('POST','/api/manual-buy',{amount_usd:amount,direction:direction},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Request failed');
          return;
        }
        status.textContent=String((resp&&resp.message)||('Queued '+direction));
      });
    }

    function manualBuyUp(){ manualBuyWithDirection('UP'); }
    function manualBuyDown(){ manualBuyWithDirection('DOWN'); }

    function manualBuyNextWithDirection(direction){
      var status=document.getElementById('buyStatus');
      var amount=readNum('buyAmount',0);
      if(!(amount>0)){
        if(status) status.textContent='Amount must be > 0';
        return;
      }
      if(status) status.textContent='Queueing next window...';
      requestJson('POST','/api/manual-buy-next',{amount_usd:amount,direction:direction},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Request failed');
          return;
        }
        status.textContent=String((resp&&resp.message)||('Queued next '+direction));
      });
    }

    function manualBuyNextUp(){ manualBuyNextWithDirection('UP'); }
    function manualBuyNextDown(){ manualBuyNextWithDirection('DOWN'); }

    function manualBuyNextNowWithDirection(direction){
      var status=document.getElementById('buyStatus');
      var amount=readNum('buyAmount',0);
      if(!(amount>0)){
        if(status) status.textContent='Amount must be > 0';
        return;
      }
      if(status) status.textContent='Buying next window NOW...';
      requestJson('POST','/api/manual-buy-next-now',{amount_usd:amount,direction:direction},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Request failed');
          return;
        }
        status.textContent=String((resp&&resp.message)||('Bought next '+direction+' NOW'));
      });
    }

    function manualBuyNextNowUp(){ manualBuyNextNowWithDirection('UP'); }
    function manualBuyNextNowDown(){ manualBuyNextNowWithDirection('DOWN'); }

    function manualSell(){
      var status=document.getElementById('buyStatus');
      if(status) status.textContent='Submitting sell...';
      requestJson('POST','/api/manual-sell',{},function(resp){
        if(!status) return;
        if(resp&&resp.ok===false){
          status.textContent=String(resp.error||'Sell request failed');
          scheduleTick(50);
          return;
        }
        status.textContent=String((resp&&resp.message)||'Queued manual sell');
        scheduleTick(50);
      });
    }

    function setBuyAmount(v){
      var el=document.getElementById('buyAmount');
      if(!el) return;
      el.value=String(v);
    }

    function applyControls(){
      var payload={ momentum:!!document.getElementById('ctl-momentum').checked, vwap_deviation:!!document.getElementById('ctl-vwap').checked, zscore:!!document.getElementById('ctl-zscore').checked };
      requestJson('POST','/api/indicator-controls',payload,function(){});
    }

    function tick(){
      if(pollInFlight) return;
      pollInFlight=true;
      var seq=++pollSeq;
      var startedAt=Date.now();
      var errEl=document.getElementById('err');
      var r=new XMLHttpRequest();
      r.open('GET','/api/state',true);
      r.onreadystatechange=function(){
        if(r.readyState!==4) return;
        if(seq!==pollSeq) return;
        pollInFlight=false;
        try{
          if(r.status!==200) throw new Error('HTTP '+r.status);
          var d=JSON.parse(r.responseText);
          errEl.textContent='';
          var hdr=d.header||{};
          var slug=hdr.slug!=null?String(hdr.slug):'\u2014';
          var ts='';
          if(d.ts) ts=new Date(d.ts*1000).toISOString();
          var metaHtml=esc(slug)+' \u00b7 '+esc(ts);
          setHtmlIfChanged('meta','meta',metaHtml);
          var existingAmountEl=document.getElementById('buyAmount');
          var buyAmountVal=existingAmountEl?existingAmountEl.value:'';
          var existingBuyStatusEl=document.getElementById('buyStatus');
          var buyStatusVal=existingBuyStatusEl?existingBuyStatusEl.textContent:'';
          var liveStatusVal=d.manual_buy_live_status?String(d.manual_buy_live_status):'idle';
          var nextBuy=d.manual_buy_next||{};
          var nextPendingText='none';
          if(nextBuy&&nextBuy.pending&&nextBuy.signal){
            var nbAmt=(nextBuy.amount_usd!=null&&typeof nextBuy.amount_usd==='number'&&!isNaN(nextBuy.amount_usd))?numFmt(nextBuy.amount_usd,2):'\u2014';
            nextPendingText=String(nextBuy.signal)+' $'+String(nbAmt);
          }
          var defaultBuyAmount=10;
          if(!buyAmountVal) buyAmountVal=String(defaultBuyAmount);
          var upPrice=((d.up&&d.up.book&&typeof d.up.book.last_price==='number')?numFmt(d.up.book.last_price,3):'\u2014');
          var downPrice=((d.down&&d.down.book&&typeof d.down.book.last_price==='number')?numFmt(d.down.book.last_price,3):'\u2014');
          var nextUpPrice=((d.next_up&&d.next_up.book&&typeof d.next_up.book.last_price==='number')?numFmt(d.next_up.book.last_price,3):'\u2014');
          var nextDownPrice=((d.next_down&&d.next_down.book&&typeof d.next_down.book.last_price==='number')?numFmt(d.next_down.book.last_price,3):'\u2014');
          var sessionHtml=[
            'Timer: '+(hdr.time_left_sec!=null?esc(Math.floor(hdr.time_left_sec)+'s left'):'\u2014'),
            'WS: '+(hdr.ws_connected?'live':'disconnected'),
            'Mode: '+(hdr.simulation?'simulation':'real'),
            'Live: '+esc(liveStatusVal),
            'Current: UP '+esc(upPrice)+' | DOWN '+esc(downPrice),
            'Next window: UP '+esc(nextUpPrice)+' | DOWN '+esc(nextDownPrice),
            'Next queued: '+esc(nextPendingText),
            '<span>Amount $ <input type="number" id="buyAmount" min="0.1" step="0.1" value="'+esc(buyAmountVal)+'" style="width:86px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:0.2rem 0.3rem;"/> <button class="btn secondary" onclick="setBuyAmount(39)">$39</button> <button class="btn secondary" onclick="setBuyAmount(49)">$49</button> <button class="btn secondary" onclick="setBuyAmount(99)">$99</button> <button class="btn" onclick="manualBuyUp()">UP</button> <button class="btn secondary" onclick="manualBuyDown()">DOWN</button> <button class="btn secondary" style="background:#1f6feb80" onclick="manualBuyNextNowUp()">NOW UP</button> <button class="btn secondary" style="background:#1f6feb80" onclick="manualBuyNextNowDown()">NOW DOWN</button> <button class="btn secondary" onclick="manualSell()">SELL</button> <span id="buyStatus" class="status">'+esc(buyStatusVal)+'</span></span>'
          ].join('<br/>');
          setHtmlIfChanged('session','session',sessionHtml);

          var st=d.strategy||{}, sig=st.signal_text||'\u2014';
          function chk(x){ return x===true?'\u2713':x===false?'\u2717':'\u2014'; }
          var favKey='';
          if(st.favorite){
            var favText=String(st.favorite).toUpperCase();
            if(favText.indexOf('UP')===0) favKey='up';
            else if(favText.indexOf('DOWN')===0) favKey='down';
          }
          var favBook=(favKey&&(d[favKey]&&d[favKey].book))?d[favKey].book:null;
          var buyVol=(favBook&&typeof favBook.volume_buy==='number'&&!isNaN(favBook.volume_buy))?Math.round(favBook.volume_buy):null;
          var totalVol=(favBook&&typeof favBook.volume_total==='number'&&!isNaN(favBook.volume_total))?Math.round(favBook.volume_total):null;
          var strategyBits=['Fav: '+esc(st.favorite)+' \u00b7 WR: '+esc(st.win_rate_str),'Volume: Buy '+esc(buyVol!=null?buyVol:'\u2014')+' | Total '+esc(totalVol!=null?totalVol:'\u2014')];
          var btcNow=d.btc||{};
          var currMin=(typeof btcNow.current_window_min_usd==='number'&&!isNaN(btcNow.current_window_min_usd))?numFmtSigned(btcNow.current_window_min_usd,2):'\u2014';
          var currMax=(typeof btcNow.current_window_max_usd==='number'&&!isNaN(btcNow.current_window_max_usd))?numFmtSigned(btcNow.current_window_max_usd,2):'\u2014';
          strategyBits.push('Current BTC move min/max: $'+esc(currMin)+' / $'+esc(currMax));
          var vs=st.volume_speed||null;
          if(vs){
            var buyDiff=(vs.curr_diff_buy!=null&&typeof vs.curr_diff_buy==='number'&&!isNaN(vs.curr_diff_buy))?numFmtSigned(vs.curr_diff_buy,0):'\u2014';
            var totalDiff=(vs.curr_diff_total!=null&&typeof vs.curr_diff_total==='number'&&!isNaN(vs.curr_diff_total))?numFmtSigned(vs.curr_diff_total,0):'\u2014';
            var minDiff=(vs.min_current_volume_diff!=null&&typeof vs.min_current_volume_diff==='number'&&!isNaN(vs.min_current_volume_diff))?numFmt(vs.min_current_volume_diff,0):'\u2014';
            strategyBits.push('Vol cmp: buy '+buyDiff+' | total '+totalDiff+' | min '+minDiff+' | ok '+chk(vs.ok));
          }
          if(st.btc_buffer_line) strategyBits.push('BTC Buffer: '+esc(st.btc_buffer_line));
          var modeBtcLines=st.btc_buffer_mode_lines||[];
          if(modeBtcLines&&modeBtcLines.length){
            for(var bi=0;bi<modeBtcLines.length;bi++){
              strategyBits.push('BTC Buffer '+esc(String(modeBtcLines[bi]||'')));
            }
          }
          var strategyHtml='<div class="sig '+sigClass(sig)+'">'+esc(sig)+'</div><div class="mono" style="margin-top:0.4rem">'+strategyBits.join('<br/>')+'</div>';
          setHtmlIfChanged('strategy','strategy',strategyHtml);

          function book(x,id){ var el=document.getElementById(id); if(!el) return; if(!x){ el.textContent='No data'; return; } var bk=x.book||{}, ind=x.indicators||{}; el.innerHTML=['Last '+esc(bk.last_price),'Bid '+esc(bk.best_bid)+' / Ask '+esc(bk.best_ask),'PM VWAP '+numFmt(ind.pm_vwap,4)+' \u00b7 BTC VWAP '+(ind.btc_vwap_weighted!=null?numFmt(ind.btc_vwap_weighted,4):'\u2014'),'Dev '+(ind.deviation_pct!=null?numFmt(ind.deviation_pct,2)+'%':'\u2014')+' \u00b7 BTC Vol Bias '+(ind.btc_vol_ratio!=null?numFmt(ind.btc_vol_ratio,1)+'%':'\u2014'),'Z '+numFmt(ind.zscore,2)+' \u00b7 Mom '+(ind.momentum_pct!=null?numFmt(ind.momentum_pct,2)+'%':'\u2014'),'Vol '+(bk.volume_total!=null?esc(Math.round(bk.volume_total)):'\u2014')].join('<br/>'); }
          book(d.up,'up'); book(d.down,'down');

          var b=d.btc||{}, btcEl=document.getElementById('btc');
          if(b.btc_current_price>0){
            var bits=['$'+esc(numFmt(b.btc_current_price,2)),'Anchor $'+(b.btc_anchor_price>0?esc(numFmt(b.btc_anchor_price,2)):'\u2014'),esc(b.deviation_line||'')];
            if(b.buffer_avg_abs_usd!=null||b.buffer_avg_abs_pct!=null){ var usd=b.buffer_avg_abs_usd!=null?'$'+esc(numFmt(b.buffer_avg_abs_usd,2)):'\u2014'; var pct=b.buffer_avg_abs_pct!=null?esc(numFmt(b.buffer_avg_abs_pct,3))+'%':'\u2014'; bits.push('Buffer avg(5): +/-'+usd+' (+/-'+pct+')'); }
            if(b.buffer_windows&&b.buffer_windows.length){ bits.push('\u2014 last 5 windows \u2014'); for(var wi=0;wi<b.buffer_windows.length;wi++){ var ww=b.buffer_windows[wi]; var wt=ww.window_ts?new Date(ww.window_ts*1000).toISOString().substr(11,8):'?'; var sUsd=(ww.signed_usd!=null&&typeof ww.signed_usd==='number'&&!isNaN(ww.signed_usd))?numFmtSigned(ww.signed_usd,2):numFmtSigned((ww.abs_usd!=null?ww.abs_usd:0),2); var minUsd=(ww.min_usd!=null&&typeof ww.min_usd==='number'&&!isNaN(ww.min_usd))?numFmtSigned(ww.min_usd,2):'\u2014'; var maxUsd=(ww.max_usd!=null&&typeof ww.max_usd==='number'&&!isNaN(ww.max_usd))?numFmtSigned(ww.max_usd,2):'\u2014'; bits.push(esc(wt)+' $'+esc(sUsd)+' | min $'+esc(minUsd)+' | max $'+esc(maxUsd)); } }
            bits.push('Feed: '+(b.btc_connected?'ok':'off')+(b.fresh_sec!=null?' \u00b7 '+Math.floor(b.fresh_sec)+'s':''));
            var btcHtml=bits.join('<br/>');
            if(lastHtml.btc!==btcHtml){
              lastHtml.btc=btcHtml;
              btcEl.innerHTML=btcHtml;
            }
          } else { btcEl.textContent='Waiting for Chainlink...'; }

          var saRun=document.getElementById('sa_current_run');
          if(saRun){ var ds=b.direction_streak||{}; saRun.textContent=(ds.length>0&&ds.direction)?(ds.length+'x '+ds.direction):'none'; }

          var streakEnds=(d.streak_end_counts&&d.streak_end_counts.length)?d.streak_end_counts:[];
          var streakEndLines=[];
          var summaryData=null;
          for(var sei=0;sei<streakEnds.length;sei++){
            var row=streakEnds[sei]||{};
            if(row._summary){
              summaryData=row.by_length||[];
              continue;
            }
            var lenVal=(row.length!=null)?String(row.length):'\u2014';
            var dirVal=row.direction?String(row.direction):'?';
            var cntVal=(row.ended_count!=null)?String(row.ended_count):'0';
            streakEndLines.push(esc(lenVal+'x '+dirVal+' ended: '+cntVal));
          }
          if(summaryData&&summaryData.length){
            streakEndLines.push('---');
            for(var ssi=0;ssi<summaryData.length;ssi++){
              var srow=summaryData[ssi]||{};
              var slLen=(srow.length!=null)?String(srow.length):'\u2014';
              var slTotal=(srow.total_ends!=null)?String(srow.total_ends):'0';
              var slPct=(srow.pct!=null)?String(srow.pct):'0';
              streakEndLines.push(esc(slLen+'x: '+slTotal+' ('+slPct+'%)'));
            }
          }
          var streakEndsHtml=streakEndLines.length?streakEndLines.join('<br/>'):'No streak endings yet';
          setHtmlIfChanged('streakEnds','streakEnds',streakEndsHtml);

          var tr=d.trading||{};
          var tHtml='Markets '+esc(tr.markets_seen)+' \u00b7 Trades '+esc(tr.trade_count)+' \u00b7 PnL $'+(tr.total_pnl!=null?numFmt(tr.total_pnl,2):'\u2014')+'<br/>';
          var liveLower=String(liveStatusVal||'').toLowerCase();
          var sellPhase=(liveLower.indexOf('sell')>=0);
          if(sellPhase){
            tHtml+='Sell status: '+esc(liveStatusVal)+'<br/>';
          }
          if(tr.position){ var p=tr.position; var posPrefix=sellPhase?'SELLING LONG ':'LONG '; tHtml+=posPrefix+esc(p.token_name)+' @ '+esc(p.entry_price)+' \u00d7'+esc(p.contracts)+(p.hedged?' hedged':'')+'<br/>'; tHtml+='Unreal $'+(p.unrealized_pnl!=null?numFmt(p.unrealized_pnl,2):'\u2014')+'<br/>'; } else { tHtml+='No open position<br/>'; }
          var nextBuyInfo=d.manual_buy_next||{}; if(nextBuyInfo.pending){ var nextDir=String(nextBuyInfo.direction||'?'); var nextAmt=(nextBuyInfo.amount_usd!=null)?numFmt(nextBuyInfo.amount_usd,2):'\u2014'; tHtml+='<br/>Next window order: BUY '+esc(nextDir)+' $'+esc(nextAmt)+'<br/>'; }
          var nextWinResult=d.next_window_order_result||null; if(nextWinResult){ var nwrStatus=String(nextWinResult.status||'?'); var nwrDir=String(nextWinResult.direction||'?'); var nwrAmt=(nextWinResult.amount_usd!=null)?numFmt(nextWinResult.amount_usd,2):'\u2014'; var nwrLine=''; if(nwrStatus==='success'){ var nwrContracts=(nextWinResult.contracts!=null)?numFmt(nextWinResult.contracts,2):'\u2014'; var nwrPrice=(nextWinResult.price!=null)?numFmt(nextWinResult.price,4):'\u2014'; nwrLine='\u2705 Sent: BUY '+esc(nwrDir)+' $'+esc(nwrAmt)+' | '+esc(nwrContracts)+' contracts @ '+esc(nwrPrice); }else{ var nwrErr=String(nextWinResult.error||'Unknown'); nwrLine='\u274c Failed: BUY '+esc(nwrDir)+' $'+esc(nwrAmt)+' - '+esc(nwrErr); } tHtml+='<br/>Next Window Order Result: '+nwrLine+'<br/>'; }
          if(tr.recent_trades&&tr.recent_trades.length){ var lines=[]; for(var ri=0;ri<tr.recent_trades.length;ri++){ lines.push(esc(tr.recent_trades[ri].line)); } tHtml+='<br/>Recent:<br/>'+lines.join('<br/>'); }
          /* TEMPORARILY DISABLED: Mode Performance rendering
          var tr=d.trading||{}, modePerfEl=document.getElementById('modePerf');
          if(modePerfEl){ var modePerfLines=[], modePerfData=tr.win_rate_by_mode||{}; window.latestModePerfData=modePerfData; var panelHandledModes={}; var panelModeOrder=['normal','manual','mode_60s','mode_40s','mode_30s','mode_20s','unknown']; function pushPanelModeLine(modeKey){ if(!Object.prototype.hasOwnProperty.call(modePerfData,modeKey)) return; panelHandledModes[modeKey]=true; var ms=modePerfData[modeKey]||{}; var wrVal=(ms.win_rate_pct!=null&&typeof ms.win_rate_pct==='number'&&!isNaN(ms.win_rate_pct))?(numFmt(ms.win_rate_pct,1)+'%'):'\u2014'; var pnlVal=(ms.total_pnl_usd!=null&&typeof ms.total_pnl_usd==='number'&&!isNaN(ms.total_pnl_usd))?('$'+numFmtSigned(ms.total_pnl_usd,2)):'$\u2014'; var countVal=(ms.trade_count!=null)?String(ms.trade_count):'0'; modePerfLines.push(esc(modeKey)+' | PnL '+esc(pnlVal)+' | Triggers '+esc(countVal)+' | WR '+esc(wrVal)); } for(var pmo=0;pmo<panelModeOrder.length;pmo++){ pushPanelModeLine(panelModeOrder[pmo]); } for(var pmk in modePerfData){ if(!Object.prototype.hasOwnProperty.call(modePerfData,pmk)) continue; if(panelHandledModes[pmk]) continue; pushPanelModeLine(pmk); } var modePerfHtml=modePerfLines.length?modePerfLines.join('<br/>'):'No mode stats yet'; setHtmlIfChanged('modePerf','modePerf',modePerfHtml); }
          */
          window.latestModePerfData={};
          setHtmlIfChanged('trading','trading',tHtml);
          var targetMs=document.hidden?POLL_MS_HIDDEN:POLL_MS_ACTIVE;
          var elapsedMs=Math.max(0, Date.now()-startedAt);
          scheduleTick(Math.max(250, targetMs-elapsedMs));
        } catch(e){ errEl.textContent='Poll error: '+((e&&e.message)?e.message:e); scheduleTick(2000); }
      };
      r.onerror=function(){ pollInFlight=false; errEl.textContent='Network error (is the bot running?)'; scheduleTick(2000); };
      r.send();
    }

    document.addEventListener('visibilitychange', function(){
      scheduleTick(document.hidden?POLL_MS_HIDDEN:250);
    });

    // TEMPORARILY DISABLED: loadLateModeConfig();
    // TEMPORARILY DISABLED: loadVolumeEvalConfig();
    loadTimerAlertConfig();
    loadStreakAlertConfig();
    tick();
  </script>
</body>
</html>
"""


def _sanitize_for_json(obj: Any) -> Any:
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
  get_streak_alert: Optional[Callable[[], Dict[str, Any]]] = None,
  update_streak_alert: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy_next: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_sell: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
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
      # TEMPORARILY DISABLED: Return enabled=False regardless of backend state
      return JSONResponse({"enabled": False, "modes": []})

    @app.post("/api/late-modes")
    async def api_update_late_modes(payload: Dict[str, Any] = Body(default={})):
      # TEMPORARILY DISABLED: Always return enabled=False
      return JSONResponse({"enabled": False, "modes": []})

    @app.get("/api/volume-accel-check")
    async def api_get_volume_accel_check():
      # TEMPORARILY DISABLED: Return defaults regardless of backend state
      return JSONResponse({"min_current_volume_diff": 0.0, "min_accel_diff": 0.0, "volume_basis": "total"})

    @app.post("/api/volume-accel-check")
    async def api_update_volume_accel_check(payload: Dict[str, Any] = Body(default={})):
      # TEMPORARILY DISABLED: Always return defaults
      return JSONResponse({"min_current_volume_diff": 0.0, "min_accel_diff": 0.0, "volume_basis": "total"})

    @app.get("/api/volume-eval-mode")
    async def api_get_volume_eval_mode():
      # TEMPORARILY DISABLED: Return enabled=False regardless of backend state
      return JSONResponse({"enabled": False})

    @app.post("/api/volume-eval-mode")
    async def api_update_volume_eval_mode(payload: Dict[str, Any] = Body(default={})):
      # TEMPORARILY DISABLED: Always return enabled=False
      return JSONResponse({"enabled": False})

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

    @app.get("/api/streak-alert")
    async def api_get_streak_alert():
      if not get_streak_alert:
        return JSONResponse({"enabled": False})
      return JSONResponse(_sanitize_for_json(get_streak_alert()))

    @app.post("/api/streak-alert")
    async def api_update_streak_alert(payload: Dict[str, Any] = Body(default={})):
      if not update_streak_alert:
        return JSONResponse(_sanitize_for_json(payload or {}))
      return JSONResponse(_sanitize_for_json(update_streak_alert(payload or {})))

    @app.post("/api/manual-buy")
    async def api_manual_buy(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy:
        return JSONResponse({"ok": False, "error": "Manual buy is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_buy(payload or {})))

    @app.post("/api/manual-buy-next")
    async def api_manual_buy_next(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy_next:
        return JSONResponse({"ok": False, "error": "Next-window manual buy is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_buy_next(payload or {})))

    @app.post("/api/manual-buy-next-now")
    async def api_manual_buy_next_now(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_buy_next:
        return JSONResponse({"ok": False, "error": "Next-window manual buy is not enabled"})
      payload_copy = dict(payload or {})
      payload_copy["_buy_next_now"] = True
      return JSONResponse(_sanitize_for_json(trigger_manual_buy_next(payload_copy)))

    @app.post("/api/manual-sell")
    async def api_manual_sell(payload: Dict[str, Any] = Body(default={})):
      if not trigger_manual_sell:
        return JSONResponse({"ok": False, "error": "Manual sell is not enabled"})
      return JSONResponse(_sanitize_for_json(trigger_manual_sell(payload or {})))

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
  get_streak_alert: Optional[Callable[[], Dict[str, Any]]] = None,
  update_streak_alert: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_buy_next: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
  trigger_manual_sell: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
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
      get_streak_alert=get_streak_alert,
      update_streak_alert=update_streak_alert,
      trigger_manual_buy=trigger_manual_buy,
      trigger_manual_buy_next=trigger_manual_buy_next,
      trigger_manual_sell=trigger_manual_sell,
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
