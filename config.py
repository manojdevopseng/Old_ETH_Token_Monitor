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
ALCHEMY_HTTP_URL: str = os.getenv("ALCHEMY_API_KEY", "").strip()
if not ALCHEMY_HTTP_URL:
    raise ValueError("ALCHEMY_API_KEY must be set in .env")

# Use ALCHEMY_WS_URL if explicitly set, else derive from HTTP URL
ALCHEMY_WS_URL: str = os.getenv("ALCHEMY_WS_URL", "").strip() or (
    ALCHEMY_HTTP_URL
    .replace("https://", "wss://")
    .replace("http://",  "ws://")
)

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
WEB_PORT              = int(os.getenv("WEB_PORT", "8000"))
