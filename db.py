import sqlite3
from datetime import datetime, timezone, timedelta

DB_FILE = "revival_bot.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create table + migrate old schema if upgrading from an earlier version."""
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerted_tokens (
                token_address TEXT PRIMARY KEY,
                token_name    TEXT,
                token_symbol  TEXT,
                tier_name     TEXT,
                gap_days      INTEGER,
                alerted_at    TEXT
            )
        """)
        # Safe migrations for users upgrading from v1 schema
        for col, typedef in [
            ("token_symbol", "TEXT"),
            ("tier_name",    "TEXT"),
            ("gap_days",     "INTEGER"),
        ]:
            try:
                conn.execute(f"ALTER TABLE alerted_tokens ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # Column already exists
        conn.commit()


def is_alerted(token_address: str) -> bool:
    """
    Return True if this token was alerted within the cooldown window.
    Replaces the old daily-reset approach — tokens can re-alert after
    ALERT_COOLDOWN_DAYS days without any DB wipe.
    """
    from config import ALERT_COOLDOWN_DAYS
    cutoff = (
        datetime.now(tz=timezone.utc) - timedelta(days=ALERT_COOLDOWN_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM alerted_tokens WHERE token_address = ? AND alerted_at >= ?",
            (token_address.lower(), cutoff),
        ).fetchone()
    return row is not None


def mark_alerted(
    token_address: str,
    token_name:    str,
    token_symbol:  str = "",
    tier_name:     str = "Revival",
    gap_days:      int = 0,
) -> None:
    """Insert or refresh a token record (resets the cooldown clock)."""
    alerted_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO alerted_tokens
                (token_address, token_name, token_symbol, tier_name, gap_days, alerted_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (token_address.lower(), token_name, token_symbol, tier_name, gap_days, alerted_at),
        )
        conn.commit()


def get_all_tokens() -> list[dict]:
    """Return all alerted tokens for the dashboard (most recent first)."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM alerted_tokens ORDER BY alerted_at DESC LIMIT 200"
        ).fetchall()
    return [dict(r) for r in rows]
