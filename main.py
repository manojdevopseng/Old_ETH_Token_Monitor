"""
Entry point — three concurrent threads:

  Thread 1  ws-listener   Alchemy WebSocket, Uniswap V4 PoolManager + V2 pair Swap events
  Thread 2  ws-worker     token lookup + alchemy_getAssetTransfers gap check (parallel)
  Thread 3  scheduler     Periodic backup scan + Scan-Now signal handler
  Main      uvicorn       FastAPI dashboard + WebSocket log stream
"""
import threading
import time
import schedule
import uvicorn
from datetime import datetime

from config import SCAN_INTERVAL_MINUTES, WEB_PORT
from logger import IST, log, update_stats, inc_stat, scan_signal
from db import init_db, is_alerted, mark_alerted
from scanner import scan_for_revivals, start_ws_listener
from alert import send_revival_alert
from web import app, register_scan_callback


# ── Shared revival handler (used by BOTH real-time and backup paths) ─────────

def _on_revival(candidate: dict) -> None:
    """
    Called whenever a revival candidate is found — from WebSocket or backup scan.
    Checks cooldown, sends alert, marks in DB.
    """
    addr = candidate["token_address"]

    if is_alerted(addr):
        return  # Within cooldown window — skip silently

    send_revival_alert(candidate)
    mark_alerted(
        token_address=addr,
        token_name=candidate["token_name"],
        token_symbol=candidate["token_symbol"],
        tier_name=candidate["tier_name"],
        gap_days=candidate["gap_days"],
    )
    inc_stat("alerts_sent")
    inc_stat("revivals_found")
    log(
        f"[ALERT] Sent: {candidate['token_name']} "
        f"({candidate['token_symbol']}) — {addr}"
    )


# ── Backup / manual scan cycle ───────────────────────────────────────────────

def run_scan() -> None:
    """
    Full scan via Alchemy HTTP getLogs.
    Triggered by: periodic schedule (every 30 min) OR Scan Now button.
    Real-time detection happens independently via WebSocket.
    """
    log("[BOT] Backup scan started")
    update_stats(last_scan=datetime.now(tz=IST).strftime("%Y-%m-%d %H:%M:%S"))

    try:
        candidates, buy_count = scan_for_revivals()

        log(f"[SCAN] Unique tokens with swaps: {buy_count}")
        inc_stat("tokens_scanned", buy_count)
        log(f"[SCAN] Revival candidates (buy only): {len(candidates)}")

        for candidate in candidates:
            _on_revival(candidate)

        log("[BOT] Backup scan completed")

    except Exception as e:
        log(f"[ERROR] Scan failed: {e}", "error")
        log("[BOT] Scan skipped — will retry next cycle", "warn")


# ── Scheduler thread ─────────────────────────────────────────────────────────

def _scheduler_loop() -> None:
    log("[BOT] Scheduler thread started")

    def _launch_scan() -> None:
        # Run in its own thread so the countdown loop is never blocked by a scan
        threading.Thread(target=run_scan, daemon=True, name="scan").start()

    job = schedule.every(SCAN_INTERVAL_MINUTES).minutes.do(_launch_scan)

    # Startup scan — background thread so the while loop starts immediately
    # (countdown is visible from the very first second, not after scan finishes)
    _launch_scan()

    while True:
        # Manual scan requested from dashboard
        if scan_signal.is_set():
            scan_signal.clear()
            _launch_scan()

        # Update next_scan_secs every second from actual job schedule
        if job.next_run:
            remaining = (job.next_run - datetime.now()).total_seconds()
            update_stats(next_scan_secs=max(0, int(remaining)))

        try:
            schedule.run_pending()
        except Exception as e:
            log(f"[ERROR] Scheduler error: {e}", "error")
        time.sleep(1)


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    log("[BOT] Uniswap Token Monitor starting...")

    init_db()
    log("[DB] Database ready")

    # Register callback so /api/scan (dashboard) can trigger run_scan
    register_scan_callback(run_scan)

    # ── Thread 1 & 2: Alchemy WebSocket (real-time path) ────────────────────
    start_ws_listener(on_revival=_on_revival)

    # ── Thread 3: Periodic backup scan ──────────────────────────────────────
    threading.Thread(
        target=_scheduler_loop,
        daemon=True,
        name="scheduler",
    ).start()

    log(f"[BOT] Dashboard -> http://0.0.0.0:{WEB_PORT}")

    # ── Main thread: FastAPI + WebSocket log stream ──────────────────────────
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=WEB_PORT,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
