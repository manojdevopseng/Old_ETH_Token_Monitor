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

_handler = TimedRotatingFileHandler(
    filename    = _LOG_FILE,
    when        = "midnight",   # rotate at midnight IST (close enough — UTC midnight)
    interval    = 1,
    backupCount = 7,            # keep last 7 days
    encoding    = "utf-8",
    utc         = False,
)
_handler.suffix  = "%Y-%m-%d"
_handler.setFormatter(logging.Formatter("%(message)s"))
_file_logger.addHandler(_handler)


# ── Log queue (scanner/main → WebSocket broadcaster) ────────────────────────
_log_q: queue.Queue = queue.Queue()


def log(msg: str, level: str = "info") -> None:
    ts_long  = datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S IST")
    ts_short = datetime.now(tz=IST).strftime("%H:%M:%S")
    line     = f"[{ts_short}] {msg}"
    full     = f"[{ts_long}] {msg}"

    print(full)                          # console
    _file_logger.info(full)              # logs/bot.log  (with daily rotation)
    _log_q.put_nowait({"msg": line, "level": level})  # WebSocket dashboard


def get_log_queue() -> queue.Queue:
    return _log_q


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
