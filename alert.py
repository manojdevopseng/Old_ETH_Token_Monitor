import time
import requests

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from logger import log

_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"


def _send(text: str) -> bool:
    try:
        resp = requests.post(
            _API,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
        if not resp.json().get("ok"):
            log(f"[Alert] Telegram ok=false: {resp.json()}", "warn")
            return False
        return True
    except Exception as e:
        log(f"[Alert] Send error: {e}", "warn")
        return False


def send_revival_alert(candidate: dict) -> None:
    """
    Send tier-aware revival alert to Telegram.
    Retries once after 5 s on failure. Never raises.
    """
    tier_emoji = candidate.get("tier_emoji", "🚨")
    tier_name  = candidate.get("tier_name",  "Revival Detected")

    text = (
        f"{tier_emoji} <b>{tier_name}!</b>\n"
        "\n"
        f"📌 Token: {candidate['token_name']} ({candidate['token_symbol']})\n"
        f"📍 Address: <code>{candidate['token_address']}</code>\n"
        "\n"
        f"⏱ Gap: {candidate['gap_days']} days {candidate['gap_hours']} hours\n"
        f"📅 Last Activity: {candidate['tx1_datetime']}\n"
        f"🟢 Revival TX:    {candidate['tx0_datetime']}\n"
        "\n"
        f"🔗 Etherscan: https://etherscan.io/token/{candidate['token_address']}"
    )

    if _send(text):
        return

    log("[Alert] Retrying in 5 s...", "warn")
    time.sleep(5)

    if _send(text):
        return

    log(
        f"[Alert] FAILED permanently for {candidate['token_name']} "
        f"({candidate['token_address']})",
        "error",
    )
