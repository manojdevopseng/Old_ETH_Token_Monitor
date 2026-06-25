"""
Shared state module — imported by scanner, alert, web, and main.
Keeps circular imports out of the picture.
"""
import queue
import threading
import logging
import os
from datetime import datetime, timezone, timedelta
from logging.handlers import TimedRotatingFileHandler

IST = timezone(timedelta(hours=5, minutes=30))

# ── File logger setup ────────────────────────────────────────────────────────
_LOG_DIR  = "logs"
_LOG_FILE = os.path.join(_LOG_DIR, "bot.log")

os.makedirs(_LOG_DIR, exist_ok=True)

_file_logger = logging.getLogger("bot")
_file_logger.setLevel(logging.DEBUG)

# Monthly rotation — one file per month, keep last 12 months
_handler = TimedRotatingFileHandler(
    filename    = _LOG_FILE,
    when        = "midnight",
    interval    = 30,           # rotate every 30 days (~monthly)
    backupCount = 12,           # keep last 12 rotated files
    encoding    = "utf-8",
    utc         = False,
)
_handler.suffix  = "%Y-%m"
_handler.setFormatter(logging.Formatter("%(message)s"))
_file_logger.addHandler(_handler)


# ── In-memory ring buffer — persists across WebSocket reconnects ─────────────
# Loaded from log file on startup so dashboard shows history after bot restart.
_HISTORY_LINES = 500
_log_history: list[dict] = []
_log_history_lock = threading.Lock()


def _load_history_from_file() -> None:
    """Read last _HISTORY_LINES lines from bot.log into memory on startup."""
    if not os.path.exists(_LOG_FILE):
        return
    try:
        with open(_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for raw in lines[-_HISTORY_LINES:]:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            level = "warn" if "[ERROR]" in raw or "error" in raw.lower() else \
                    "warn"  if "[WARN]"  in raw or "warn"  in raw.lower() else "info"
            # Strip timestamp prefix for display (keep it readable in dashboard)
            _log_history.append({"msg": raw, "level": level})
    except Exception:
        pass


_load_history_from_file()


# ── Log queue (scanner/main → WebSocket broadcaster) ────────────────────────
_log_q: queue.Queue = queue.Queue()

# Pre-fill queue with history so late-connecting dashboard gets past logs too
for _entry in _log_history:
    _log_q.put_nowait(_entry)


def log(msg: str, level: str = "info") -> None:
    ts_long  = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")
    ts_short = datetime.now(tz=IST).strftime("%H:%M:%S")
    line     = f"[{ts_short}] {msg}"
    full     = f"[{ts_long}] {msg}"

    print(full)                          # console / systemd journal
    _file_logger.info(full)             # logs/bot.log  (monthly rotation)

    entry = {"msg": full, "level": level}
    with _log_history_lock:
        _log_history.append(entry)
        if len(_log_history) > _HISTORY_LINES:
            _log_history.pop(0)
    _log_q.put_nowait({"msg": line, "level": level})  # WebSocket dashboard


def get_log_queue() -> queue.Queue:
    return _log_q


def get_log_history() -> list[dict]:
    with _log_history_lock:
        return list(_log_history)


# ── Live stats (updated by main, read by /api/stats) ────────────────────────
_stats_lock = threading.Lock()
_stats: dict = {
    "tokens_scanned": 0,
    "revivals_found": 0,
    "alerts_sent":    0,
    "last_scan":      "",
    "next_scan_secs": 0,
}


def update_stats(**kwargs) -> None:
    with _stats_lock:
        _stats.update(kwargs)


def inc_stat(key: str, by: int = 1) -> None:
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + by


def get_stats() -> dict:
    with _stats_lock:
        return dict(_stats)


# ── Scan-now signal (web /api/scan → scheduler loop) ────────────────────────
scan_signal: threading.Event = threading.Event()

# ── Bot on/off flag (Stop/Start button → scanner + scheduler) ────────────────
bot_enabled: threading.Event = threading.Event()
bot_enabled.set()   # scanning active by default


def get_bot_running() -> bool:
    return bot_enabled.is_set()
