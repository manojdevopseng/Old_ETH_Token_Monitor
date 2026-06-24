import os
import re
from dotenv import load_dotenv

load_dotenv()

# ── Etherscan key pool ────────────────────────────────────────────────────────
ETHERSCAN_API_KEYS: list[str] = [
    k for k in [
        os.getenv("ETHERSCAN_API_KEY_1", ""),
        os.getenv("ETHERSCAN_API_KEY_2", ""),
        os.getenv("ETHERSCAN_API_KEY_3", ""),
        os.getenv("ETHERSCAN_API_KEY_4", ""),
        os.getenv("ETHERSCAN_API_KEY_5", ""),
    ]
    if k.strip()
]

if not ETHERSCAN_API_KEYS:
    import warnings
    warnings.warn("No ETHERSCAN_API_KEY_* set — Etherscan features disabled (scanner uses Alchemy)")

# ── Alchemy ───────────────────────────────────────────────────────────────────
# Add as many keys as you want in .env — no code changes needed.
#
# HTTP keys:  ALCHEMY_API_KEY, ALCHEMY_API_KEY_2, ALCHEMY_API_KEY_3, ...
# WS  keys:   ALCHEMY_WS_URL,  ALCHEMY_WS_URL_2,  ALCHEMY_WS_URL_3,  ...
#
# Both https:// and wss:// are accepted — auto-converted as needed.
# If WS keys are omitted, they are derived from the matching HTTP key.

def _to_http(url: str) -> str:
    return url.replace("wss://", "https://").replace("ws://", "http://")

def _to_ws(url: str) -> str:
    return url.replace("https://", "wss://").replace("http://", "ws://")

def _collect_keys(prefix: str) -> dict[int, str]:
    """
    Scan os.environ for all variables matching <prefix> or <prefix>_<N>.
    Returns {index: raw_value} sorted by index (base key = index 1).
    """
    pattern = re.compile(rf'^{re.escape(prefix)}(_(\d+))?$')
    found: dict[int, str] = {}
    for name, val in os.environ.items():
        m = pattern.match(name)
        if m and val.strip():
            idx = int(m.group(2)) if m.group(2) else 1
            found[idx] = val.strip()
    return found

_http_raw = _collect_keys("ALCHEMY_API_KEY")
_ws_raw   = _collect_keys("ALCHEMY_WS_URL")

if not _http_raw:
    raise ValueError("ALCHEMY_API_KEY must be set in .env")

# HTTP rotation list — sorted by key index (ALCHEMY_API_KEY=1, _2=2, ...)
ALCHEMY_HTTP_URLS: list[str] = [
    _to_http(v) for _, v in sorted(_http_raw.items())
]
ALCHEMY_HTTP_URL: str = ALCHEMY_HTTP_URLS[0]  # primary (backward compat)

# WS rotation list — use explicit WS keys if set, else derive from HTTP keys
ALCHEMY_WS_URLS: list[str] = []
for idx, http_url in sorted(_http_raw.items()):
    ws_url = _ws_raw.get(idx, "")           # explicit WS key for this account
    ALCHEMY_WS_URLS.append(_to_ws(ws_url or http_url))

ALCHEMY_WS_URL: str = ALCHEMY_WS_URLS[0]  # primary (backward compat)

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Scanning ──────────────────────────────────────────────────────────────────
SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "30"))

# ── Revival detection ─────────────────────────────────────────────────────────
REVIVAL_GAP_DAYS      = int(os.getenv("REVIVAL_GAP_DAYS", "3"))
MIN_LIQUIDITY_USD     = float(os.getenv("MIN_LIQUIDITY_USD", "500"))

# ── Alert behaviour ───────────────────────────────────────────────────────────
ALERT_COOLDOWN_DAYS   = int(os.getenv("ALERT_COOLDOWN_DAYS", "7"))

# ── Performance ───────────────────────────────────────────────────────────────
MAX_WORKERS           = int(os.getenv("MAX_WORKERS", "4"))

# ── Dashboard ─────────────────────────────────────────────────────────────────
WEB_PORT              = int(os.getenv("WEB_PORT", "8001"))
