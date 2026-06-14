"""
FastAPI dashboard — real-time WebSocket log stream + REST endpoints.
Imported by main.py; never imports from main.py (avoids circular import).
"""
import asyncio
import json
import threading
from collections import deque
from typing import Callable

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from logger import get_log_queue, get_stats, scan_signal
from db import get_all_tokens

app = FastAPI(title="Uniswap Token Monitor", docs_url=None, redoc_url=None)

# Callback registered by main.py so /api/scan can trigger a scan
_scan_cb: Callable | None = None

def register_scan_callback(fn: Callable) -> None:
    global _scan_cb
    _scan_cb = fn


# ── WebSocket connection manager ─────────────────────────────────────────────

class _Manager:
    def __init__(self):
        self._clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self._clients.discard(ws)

    async def broadcast(self, payload: dict):
        dead: set = set()
        msg = json.dumps(payload)
        for ws in self._clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.add(ws)
        self._clients -= dead

    @property
    def count(self) -> int:
        return len(self._clients)

_mgr = _Manager()

# Ring buffer — last 300 log lines kept so late-connecting browsers get history
_log_buffer: deque = deque(maxlen=300)


# ── Background async tasks ───────────────────────────────────────────────────

@app.on_event("startup")
async def _startup():
    asyncio.create_task(_log_broadcaster())
    asyncio.create_task(_stats_broadcaster())


async def _log_broadcaster():
    """Drain the log queue, buffer every entry, and push to all WebSocket clients."""
    q = get_log_queue()
    while True:
        while True:
            try:
                item = q.get_nowait()
                _log_buffer.append(item)          # keep in ring buffer for late clients
                await _mgr.broadcast({"type": "log", **item})
            except Exception:
                break
        await asyncio.sleep(0.05)


async def _stats_broadcaster():
    """Push stats snapshot to all clients every 5 seconds."""
    while True:
        if _mgr.count > 0:
            await _mgr.broadcast({"type": "stats", **get_stats()})
        await asyncio.sleep(5)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def _ws(ws: WebSocket):
    await _mgr.connect(ws)
    # Replay buffered log history so late-connecting browsers see past output
    for item in list(_log_buffer):
        await ws.send_text(json.dumps({"type": "log", **item}))
    # Send current stats immediately on connect
    await ws.send_text(json.dumps({"type": "stats", **get_stats()}))
    try:
        while True:
            await ws.receive_text()   # keep connection alive
    except WebSocketDisconnect:
        _mgr.disconnect(ws)


@app.get("/api/stats")
def api_stats():
    return get_stats()


@app.get("/api/tokens")
def api_tokens():
    return get_all_tokens()


@app.post("/api/scan")
def api_scan():
    if _scan_cb:
        threading.Thread(target=_scan_cb, daemon=True).start()
    else:
        scan_signal.set()
    return {"status": "scan triggered"}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return _HTML


# ── Dashboard HTML ───────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Uniswap Token Monitor</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@tabler/icons-webfont@2.47.0/tabler-icons.min.css">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d1117;--bg2:#161b22;--bg3:#21262d;--bg4:#30363d;
  --br:rgba(255,255,255,0.08);
  --t1:#c9d1d9;--t2:#8b949e;--t3:#484f58;
  --green:#3fb950;--amber:#d29922;--red:#f85149;--blue:#58a6ff;--purple:#bc8cff;
}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:var(--bg);color:var(--t1);min-height:100vh;font-size:14px}
a{color:inherit;text-decoration:none}
.wrap{max-width:1400px;margin:0 auto;padding:20px 24px}

.topbar{display:flex;align-items:center;justify-content:space-between;
        margin-bottom:22px;padding-bottom:14px;border-bottom:1px solid var(--br)}
.logo{display:flex;align-items:center;gap:9px;font-size:16px;font-weight:600}
.logo i{font-size:20px;color:var(--green)}
.logo em{color:var(--green);font-style:normal}
.badge{display:inline-flex;align-items:center;gap:5px;font-size:11px;
       background:rgba(63,185,80,.12);color:var(--green);
       padding:3px 10px;border-radius:20px;border:1px solid rgba(63,185,80,.25)}
.dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:blink 1.5s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.15}}
.tr{display:flex;align-items:center;gap:10px}
.ws-badge{font-size:11px;color:var(--t2);padding:3px 10px;
          border:1px solid var(--br);border-radius:20px;transition:color .3s}
.ws-badge.ok{color:var(--green)}
.ws-badge.warn{color:var(--amber)}
.btn{background:var(--bg3);color:var(--t1);border:1px solid var(--br);
     border-radius:8px;padding:7px 16px;font-size:13px;cursor:pointer;
     display:inline-flex;align-items:center;gap:6px;transition:background .15s}
.btn:hover{background:var(--bg4)}
.btn i{font-size:15px;color:var(--green)}

.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}
.stat{background:var(--bg2);border:1px solid var(--br);border-radius:10px;padding:14px 16px}
.slabel{font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:.06em;
        margin-bottom:8px;display:flex;align-items:center;gap:5px}
.slabel i{font-size:14px}
.sval{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums;color:var(--t1)}
.ssub{font-size:11px;color:var(--t3);margin-top:3px}

.panels{display:grid;grid-template-columns:1fr 1fr;gap:14px;min-width:0}
.panel{background:var(--bg2);border:1px solid var(--br);border-radius:10px;
       display:flex;flex-direction:column;height:440px;min-width:0;overflow:hidden}
.ph{display:flex;align-items:center;justify-content:space-between;
    padding:11px 15px;border-bottom:1px solid var(--br);flex-shrink:0}
.ptitle{display:flex;align-items:center;gap:7px;font-size:11px;color:var(--t2);
        text-transform:uppercase;letter-spacing:.06em;font-weight:500}
.ptitle i{font-size:15px;color:var(--green)}
.pmeta{font-size:11px;color:var(--t3)}

.feed{flex:1;overflow-y:auto;overflow-x:hidden;padding:8px 14px;font-family:'Fira Code',monospace,ui-monospace;font-size:12px;line-height:1.85}
.feed::-webkit-scrollbar{width:3px}
.feed::-webkit-scrollbar-thumb{background:var(--bg3);border-radius:2px}
.ll{white-space:pre-wrap;word-break:break-all}
.li{color:var(--t1)}.lw{color:var(--amber)}.le{color:var(--red)}

.tw{flex:1;overflow-y:auto}
.tw::-webkit-scrollbar{width:3px}
.tw::-webkit-scrollbar-thumb{background:var(--bg3);border-radius:2px}
table{width:100%;border-collapse:collapse}
th{font-size:10px;color:var(--t3);text-transform:uppercase;letter-spacing:.05em;
   padding:8px 14px;border-bottom:1px solid var(--br);text-align:left;
   position:sticky;top:0;background:var(--bg2);font-weight:500}
td{font-size:12px;padding:9px 14px;border-bottom:1px solid var(--br);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.02)}
.sym{font-weight:600;font-size:13px}
.tname{font-size:10px;color:var(--t3);margin-top:2px}
.addr{font-family:monospace;font-size:11px;color:var(--blue)}
.addr:hover{text-decoration:underline}
.tb{display:inline-block;padding:2px 9px;border-radius:20px;font-size:10px;font-weight:600}
.t-mild{background:rgba(63,185,80,.12);color:var(--green);border:1px solid rgba(63,185,80,.25)}
.t-strong{background:rgba(210,153,34,.12);color:var(--amber);border:1px solid rgba(210,153,34,.25)}
.t-dead{background:rgba(248,81,73,.12);color:var(--red);border:1px solid rgba(248,81,73,.25)}
.at{color:var(--t3);font-size:11px}

.footer{margin-top:12px;display:flex;align-items:center;justify-content:space-between}
.ftxt{font-size:11px;color:var(--t3);display:flex;align-items:center;gap:6px}
.ftxt i{font-size:13px}
</style>
</head>
<body>
<div class="wrap">

<div class="topbar">
  <div class="logo">
    <i class="ti ti-currency-ethereum"></i>
    Uniswap <em>Token Monitor</em>
    <div class="badge"><div class="dot"></div>Live</div>
  </div>
  <div class="tr">
    <div class="ws-badge" id="wsbadge">Connecting...</div>
    <button class="btn" onclick="triggerScan()">
      <i class="ti ti-refresh"></i>Scan Now
    </button>
  </div>
</div>

<div class="stats">
  <div class="stat">
    <div class="slabel"><i class="ti ti-circle-dot"></i>Tokens Scanned</div>
    <div class="sval" id="s0">—</div>
    <div class="ssub">this session</div>
  </div>
  <div class="stat">
    <div class="slabel"><i class="ti ti-flame"></i>Revivals Found</div>
    <div class="sval" id="s1">—</div>
    <div class="ssub">gap &ge; threshold</div>
  </div>
  <div class="stat">
    <div class="slabel"><i class="ti ti-bell-ringing"></i>Alerts Sent</div>
    <div class="sval" id="s2">—</div>
    <div class="ssub">via Telegram</div>
  </div>
  <div class="stat">
    <div class="slabel"><i class="ti ti-clock"></i>Last Scan</div>
    <div class="sval" id="s3" style="font-size:15px;padding-top:4px">—</div>
    <div class="ssub" id="s3b"></div>
  </div>
  <div class="stat">
    <div class="slabel"><i class="ti ti-hourglass"></i>Next Scan</div>
    <div class="sval" id="s4">—</div>
    <div class="ssub">countdown</div>
  </div>
</div>

<div class="panels">
  <div class="panel">
    <div class="ph">
      <div class="ptitle"><i class="ti ti-terminal-2"></i>Live logs</div>
      <div style="display:flex;align-items:center;gap:8px">
        <div class="pmeta" id="lcount">0 lines</div>
        <button class="btn" style="padding:4px 10px;font-size:11px" onclick="clearFeed()">
          <i class="ti ti-trash" style="font-size:13px"></i>Clear
        </button>
      </div>
    </div>
    <div class="feed" id="feed"></div>
  </div>

  <div class="panel">
    <div class="ph">
      <div class="ptitle"><i class="ti ti-bell-ringing"></i>Alerted tokens</div>
      <div class="pmeta" id="tcount">— tokens</div>
    </div>
    <div class="tw">
      <table>
        <thead><tr>
          <th>Token</th><th>Address</th><th>Tier</th><th>Alerted at</th>
        </tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<div class="footer">
  <div class="ftxt"><i class="ti ti-database"></i><span id="fdb">revival_bot.db</span></div>
  <div class="ftxt"><i class="ti ti-clock" style="color:var(--green)"></i><span id="fnext">next scan in —</span></div>
</div>

</div><!-- /wrap -->
<script>
var ws, logN = 0, nextSecs = 0, cdTimer = null;

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function connect() {
  ws = new WebSocket('ws://' + location.host + '/ws');
  ws.onopen = function() {
    var b = document.getElementById('wsbadge');
    b.textContent = 'Connected'; b.className = 'ws-badge ok';
  };
  ws.onclose = function() {
    var b = document.getElementById('wsbadge');
    b.textContent = 'Reconnecting...'; b.className = 'ws-badge warn';
    setTimeout(connect, 3000);
  };
  ws.onerror = function() { ws.close(); };
  ws.onmessage = function(e) {
    var d = JSON.parse(e.data);
    if (d.type === 'log') addLog(d);
    if (d.type === 'stats') updStats(d);
  };
}

function addLog(d) {
  var feed = document.getElementById('feed');
  var div = document.createElement('div');
  div.className = 'll';
  var cls = d.level === 'warn' ? 'lw' : d.level === 'error' ? 'le' : 'li';
  div.innerHTML = '<span class="' + cls + '">' + esc(d.msg || '') + '</span>';
  feed.appendChild(div);
  while (feed.children.length > 300) feed.removeChild(feed.firstChild);
  logN++;
  document.getElementById('lcount').textContent = logN + ' lines';
  feed.scrollTop = feed.scrollHeight;
}

function clearFeed() {
  document.getElementById('feed').innerHTML = '';
  logN = 0;
  document.getElementById('lcount').textContent = '0 lines';
}

function updStats(d) {
  if (d.tokens_scanned !== undefined) document.getElementById('s0').textContent = Number(d.tokens_scanned).toLocaleString();
  if (d.revivals_found !== undefined) document.getElementById('s1').textContent = d.revivals_found;
  if (d.alerts_sent !== undefined)    document.getElementById('s2').textContent = d.alerts_sent;
  if (d.last_scan) {
    var parts = d.last_scan.split(' ');
    document.getElementById('s3').textContent  = parts[1] || d.last_scan;
    document.getElementById('s3b').textContent = parts[0] || '';
  }
  if (d.next_scan_secs > 0) startCountdown(d.next_scan_secs);
}

function startCountdown(secs) {
  nextSecs = secs;           // always sync server value
  if (cdTimer) return;       // timer already running — just updated nextSecs, don't restart
  cdTimer = setInterval(function() {
    nextSecs = Math.max(0, nextSecs - 1);
    var m = String(Math.floor(nextSecs / 60)).padStart(2,'0');
    var s = String(nextSecs % 60).padStart(2,'0');
    var txt = m + ':' + s;
    document.getElementById('s4').textContent = txt;
    document.getElementById('fnext').textContent = 'next scan in ' + txt;
    if (nextSecs === 0) { clearInterval(cdTimer); cdTimer = null; loadTokens(); }
  }, 1000);
}

function tierClass(name) {
  if (!name) return 't-mild';
  if (name.indexOf('Dead') >= 0)   return 't-dead';
  if (name.indexOf('Strong') >= 0) return 't-strong';
  return 't-mild';
}

function loadTokens() {
  fetch('/api/tokens').then(function(r){ return r.json(); }).then(function(list) {
    var body = document.getElementById('tbody');
    document.getElementById('tcount').textContent = list.length + ' tokens';
    document.getElementById('fdb').textContent = 'revival_bot.db  ·  ' + list.length + ' tokens tracked';
    if (!list.length) {
      body.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--t3);padding:24px">No tokens alerted yet</td></tr>';
      return;
    }
    body.innerHTML = list.map(function(t) {
      var a = t.token_address || '';
      var short = a.slice(0,6) + '...' + a.slice(-4);
      var at = (t.alerted_at || '').replace(' UTC','');
      var tc = tierClass(t.tier_name);
      var gap = t.gap_days ? t.gap_days + 'd' : '';
      var tier = t.tier_name || 'Revival';
      return '<tr>' +
        '<td><div class="sym">' + esc(t.token_symbol || '???') + '</div>' +
            '<div class="tname">' + esc(t.token_name || '') + '</div></td>' +
        '<td><a class="addr" href="https://etherscan.io/token/' + a + '" target="_blank">' + short + '</a></td>' +
        '<td><span class="tb ' + tc + '">' + esc(tier) + (gap ? '  ' + gap : '') + '</span></td>' +
        '<td class="at">' + esc(at) + '</td>' +
        '</tr>';
    }).join('');
  }).catch(function(){});
}

function triggerScan() {
  fetch('/api/scan', {method:'POST'}).then(function() {
    addLog({msg: '[Manual] Scan triggered from dashboard', level: 'info'});
  });
}

connect();
loadTokens();
setInterval(loadTokens, 30000);
</script>
</body>
</html>"""
