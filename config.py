import os
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
# Accepts both https:// and wss:// URLs — wss:// is auto-converted to https://
# for HTTP calls. Paste whichever URL Alchemy shows you.
def _to_http(url: str) -> str:
    return url.replace("wss://", "https://").replace("ws://", "http://")

def _to_ws(url: str) -> str:
    return url.replace("https://", "wss://").replace("http://", "ws://")

_raw_primary = os.getenv("ALCHEMY_API_KEY", "").strip()
if not _raw_primary:
    raise ValueError("ALCHEMY_API_KEY must be set in .env")

ALCHEMY_HTTP_URL: str = _to_http(_raw_primary)

ALCHEMY_HTTP_URLS: list[str] = [
    _to_http(url) for url in [
        _raw_primary,
        os.getenv("ALCHEMY_API_KEY_2", "").strip(),
        os.getenv("ALCHEMY_API_KEY_3", "").strip(),
        os.getenv("ALCHEMY_API_KEY_4", "").strip(),
        os.getenv("ALCHEMY_API_KEY_5", "").strip(),
    ]
    if url
]

# WebSocket URLs — rotated on each reconnect across all configured accounts
# Supports ALCHEMY_WS_URL, ALCHEMY_WS_URL_2 ... ALCHEMY_WS_URL_5
# Falls back to deriving from HTTP keys if WS URLs not explicitly set
def _ws_fallback(idx: int) -> str:
    http_keys = [
        _raw_primary,
        os.getenv("ALCHEMY_API_KEY_2", "").strip(),
        os.getenv("ALCHEMY_API_KEY_3", "").strip(),
        os.getenv("ALCHEMY_API_KEY_4", "").strip(),
        os.getenv("ALCHEMY_API_KEY_5", "").strip(),
    ]
    k = http_keys[idx] if idx < len(http_keys) else ""
    return _to_ws(k) if k else ""

_raw_ws_keys = [
    os.getenv("ALCHEMY_WS_URL",   "").strip() or _ws_fallback(0),
    os.getenv("ALCHEMY_WS_URL_2", "").strip() or _ws_fallback(1),
    os.getenv("ALCHEMY_WS_URL_3", "").strip() or _ws_fallback(2),
    os.getenv("ALCHEMY_WS_URL_4", "").strip() or _ws_fallback(3),
    os.getenv("ALCHEMY_WS_URL_5", "").strip() or _ws_fallback(4),
]
ALCHEMY_WS_URLS: list[str] = [_to_ws(u) for u in _raw_ws_keys if u]
ALCHEMY_WS_URL:  str       = ALCHEMY_WS_URLS[0]  # primary (for backward compat)

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
