"""
Alchemy-only detection — no Etherscan in the hot path.

  V4 (dominant)  WebSocket → V4 PoolManager Swap events
                             → eth_getTransactionReceipt → ERC-20 Transfer logs → token
                             → alchemy_getAssetTransfers (last 2 txns) → gap check

  V2 (small)     WebSocket → V2 Pair Swap events
                             → eth_call token0/token1 on pair → non-stablecoin token
                             → alchemy_getAssetTransfers → gap check

  V3             EXCLUDED (nearly dead on mainnet as of June 2026)

  Backup scan    Alchemy eth_getLogs last 10 blocks (free-tier max):
                   V4: address=PoolManager  V2: topic0 filter (no address)

Rate limits:
  Alchemy free tier 300M CU/month — alchemy_getAssetTransfers = 150 CU per call
  Alchemy free tier 330 CU/second — _alchemy_sem caps concurrent calls to 2 (300 CU/sec)
  DexScreener free API (no key) — liquidity check only
"""
import json
import asyncio
import queue as _queue
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from config import (
    ALCHEMY_HTTP_URL,
    ALCHEMY_WS_URL,
    REVIVAL_GAP_DAYS,
    MIN_LIQUIDITY_USD,
    MAX_WORKERS,
)
from logger import log

# ── Event topics ──────────────────────────────────────────────────────────────
TRANSFER_TOPIC  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

V4_POOL_MANAGER = "0x000000000004444c5dc75cb358380d2e3de08a90"
V4_SWAP_TOPIC   = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"

V2_SWAP_TOPIC   = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

# ── Known Uniswap router / pool addresses — used for buy-vs-sell detection ────
# Buy:  pool/router is the Transfer `from` (it sends token to buyer)
# Sell: pool/router is the Transfer `to`   (it receives token from seller)
_UNISWAP_ROUTERS: frozenset[str] = frozenset({
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",  # Universal Router (current)
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Universal Router v1
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # V2 Router 02
    "0xf164fc0ec4e93095b804a4795bbe1e041497b92a",  # V2 Router 01
})

# ── Stablecoin / base-asset skip list (never revival candidates) ──────────────
SKIP_TOKENS: set[str] = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
    "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC
    "0x0000000000000000000000000000000000000000",  # native ETH (V4 currency0)
}

DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens"
V4_LOOKBACK     = 9   # blocks — Alchemy free tier: max 10-block range for eth_getLogs

# Alchemy free tier: 330 CU/sec max. alchemy_getAssetTransfers = 150 CU each.
# 2 concurrent × 150 CU = 300 CU/sec — safely under the limit.
_alchemy_sem = threading.Semaphore(2)

log("[Scanner] Alchemy-only mode — V4 PoolManager + V2 pairs")


# ═══════════════════════════════════════════════════════════════════════════════
#  ALCHEMY HTTP RPC  (all on-chain calls go through here)
# ═══════════════════════════════════════════════════════════════════════════════

def _rpc(method: str, params: list):
    resp = requests.post(
        ALCHEMY_HTTP_URL,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise Exception(f"Alchemy {method}: {data['error']}")
    return data.get("result")


def _alchemy_logs(params: dict) -> list:
    """
    eth_getLogs via Alchemy HTTP.
    Free tier limit: max 10-block range per request.
    Non-200 responses are logged and return empty (transient errors handled upstream).
    """
    try:
        resp = requests.post(
            ALCHEMY_HTTP_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": [params]},
            timeout=20,
        )
        data = resp.json()
        if "error" in data:
            log(f"[Scanner] getLogs: {data['error']}", "warn")
            return []
        return data.get("result", []) or []
    except Exception as e:
        log(f"[Scanner] getLogs exception: {e}", "warn")
        return []


# ═══════════════════════════════════════════════════════════════════════════════
#  V4 TOKEN DISCOVERY — tx receipt → ERC-20 Transfer events
# ═══════════════════════════════════════════════════════════════════════════════

_receipt_cache:      dict[str, list[str]] = {}
_receipt_cache_lock: threading.Lock       = threading.Lock()


def _tokens_from_receipt(tx_hash: str) -> list[str]:
    """
    Fetch tx receipt and collect ERC-20 token addresses from Transfer logs.
    Used for V4: PoolManager Swap event → tx_hash → actual token addresses.
    Cached per tx_hash — each transaction receipt is fetched at most once.
    """
    with _receipt_cache_lock:
        if tx_hash in _receipt_cache:
            return _receipt_cache[tx_hash]
    try:
        receipt = _rpc("eth_getTransactionReceipt", [tx_hash]) or {}
        tokens: set[str] = set()
        for entry in receipt.get("logs", []):
            topics = entry.get("topics", [])
            if topics and topics[0].lower() == TRANSFER_TOPIC:
                addr = entry.get("address", "").lower()
                if addr and addr not in SKIP_TOKENS:
                    tokens.add(addr)
        result = list(tokens)
    except Exception as e:
        log(f"[Scanner] Receipt fetch failed {tx_hash[:10]}...: {e}", "warn")
        result = []

    with _receipt_cache_lock:
        _receipt_cache[tx_hash] = result
    return result


# ═══════════════════════════════════════════════════════════════════════════════
#  V2 TOKEN DISCOVERY — pair address → token0() / token1() via eth_call
# ═══════════════════════════════════════════════════════════════════════════════
# token0() selector: 0x0dfe1681   token1() selector: 0xd21220a7

_pair_cache:      dict[str, tuple[str, str]] = {}
_pair_cache_lock: threading.Lock             = threading.Lock()


def _get_pair_tokens(pair_address: str) -> tuple[str, str] | None:
    """Call token0()/token1() on a V2 pair contract. Result cached per pair."""
    with _pair_cache_lock:
        if pair_address in _pair_cache:
            return _pair_cache[pair_address]
    try:
        def _call(selector: str) -> str | None:
            r = _rpc("eth_call", [{"to": pair_address, "data": selector}, "latest"])
            return ("0x" + r[-40:]).lower() if r and len(r) >= 42 else None

        t0 = _call("0x0dfe1681")
        t1 = _call("0xd21220a7")
        if t0 and t1:
            with _pair_cache_lock:
                _pair_cache[pair_address] = (t0, t1)
            return t0, t1
    except Exception:
        pass
    return None


def _revival_candidates_from_pair(pair_address: str) -> list[str]:
    """Non-stablecoin tokens from a V2 pair (potential revival candidates)."""
    tokens = _get_pair_tokens(pair_address)
    if not tokens:
        return []
    return [t for t in tokens if t not in SKIP_TOKENS]


# ═══════════════════════════════════════════════════════════════════════════════
#  TOKEN METADATA — name via eth_call ERC-20 name()
# ═══════════════════════════════════════════════════════════════════════════════

def _get_token_name(address: str) -> str | None:
    """
    Call ERC-20 name() (selector 0x06fdde03) and ABI-decode the string result.
    Falls back to bytes32 decoding for older tokens (e.g. MKR, SNX).
    """
    try:
        raw = _rpc("eth_call", [{"to": address, "data": "0x06fdde03"}, "latest"])
        if not raw or raw == "0x":
            return None

        # Standard ABI string: offset(32) + length(32) + data
        if len(raw) >= 130:
            length = int(raw[66:130], 16)
            if 0 < length <= 256:
                name_hex = raw[130: 130 + length * 2]
                try:
                    return bytes.fromhex(name_hex).decode("utf-8").strip("\x00").strip()
                except Exception:
                    pass

        # bytes32 fallback (older tokens)
        if len(raw) >= 66:
            try:
                return bytes.fromhex(raw[2:66]).rstrip(b"\x00").decode("utf-8").strip()
            except Exception:
                pass
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  TOKEN PROCESSING — alchemy_getAssetTransfers replaces Etherscan tokentx
# ═══════════════════════════════════════════════════════════════════════════════

def _gap_tier(gap_days: int) -> tuple[str, str]:
    if gap_days >= 30:
        return "Dead Token Revival", "🔴"
    if gap_days >= 7:
        return "Strong Revival", "🟠"
    return "Mild Revival", "🟡"


def _check_liquidity(token_address: str) -> float:
    try:
        resp = requests.get(f"{DEXSCREENER_URL}/{token_address}", timeout=10)
        resp.raise_for_status()
        pairs     = resp.json().get("pairs") or []
        eth_pairs = [p for p in pairs if p.get("chainId") == "ethereum"] or pairs
        return float(max(
            (p.get("liquidity", {}).get("usd") or 0 for p in eth_pairs),
            default=0.0,
        ))
    except Exception:
        return 0.0


def _is_buy_transfer(transfer: dict) -> bool:
    """
    True if the ERC-20 transfer is a BUY (pool/router → user).
    False if it's a SELL (user → pool/router).
    Defaults to True when direction cannot be determined (avoids missing alerts).

    Logic:
      Buy:  Transfer.from  ∈ { V4 PoolManager, V2 pair, Uniswap router }
      Sell: Transfer.to    ∈ { V4 PoolManager, V2 pair, Uniswap router }
    """
    from_addr = transfer.get("from", "").lower()
    to_addr   = transfer.get("to",   "").lower()

    # V4: PoolManager is the singleton pool contract
    if from_addr == V4_POOL_MANAGER:
        return True    # V4 buy  — pool sent token to recipient
    if to_addr == V4_POOL_MANAGER:
        return False   # V4 sell — user sent token into pool

    # V2: pair addresses cached from earlier token0()/token1() calls
    with _pair_cache_lock:
        known_pairs = set(_pair_cache.keys())
    if from_addr in known_pairs:
        return True    # V2 buy  — pair sent token to user
    if to_addr in known_pairs:
        return False   # V2 sell — user sent token to pair

    # Uniswap routers (multi-hop paths — router relays token between pool and user)
    if from_addr in _UNISWAP_ROUTERS:
        return True    # router delivered token to user = buy
    if to_addr in _UNISWAP_ROUTERS:
        return False   # token entered router heading to pool = sell

    return True        # unknown direction — include by default


def _process_token(token_address: str) -> dict | None:
    """
    Use alchemy_getAssetTransfers to get last 2 ERC-20 transfer events for
    this token.  Compute gap between them.  If gap >= REVIVAL_GAP_DAYS and
    liquidity >= MIN_LIQUIDITY_USD: return revival candidate dict.

    This replaces the old Etherscan tokentx call entirely — no rate limits.
    """
    try:
        _payload = {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "alchemy_getAssetTransfers",
            "params":  [{
                "fromBlock":        "0x0",
                "toBlock":          "latest",
                "contractAddresses": [token_address],
                "category":         ["erc20"],
                "order":            "desc",
                "maxCount":         "0x2",
                "withMetadata":     True,
                "excludeZeroValue": True,
            }],
        }
        with _alchemy_sem:
            for _attempt in range(2):
                try:
                    resp = requests.post(ALCHEMY_HTTP_URL, json=_payload, timeout=30)
                    break
                except requests.exceptions.Timeout:
                    if _attempt == 0:
                        time.sleep(3)
                    else:
                        raise
        resp.raise_for_status()
        data      = resp.json()
        if "error" in data:
            return None

        result    = data.get("result") or {}        # guard against result:null
        transfers = result.get("transfers", [])
        if len(transfers) < 2:
            return None

        t0, t1 = transfers[0], transfers[1]

        if not _is_buy_transfer(t0):
            return None   # most recent transfer is a sell — skip, not a revival

        ts0_str = t0.get("metadata", {}).get("blockTimestamp", "")
        ts1_str = t1.get("metadata", {}).get("blockTimestamp", "")
        if not ts0_str or not ts1_str:
            return None

        ts0 = datetime.fromisoformat(ts0_str.replace("Z", "+00:00"))
        ts1 = datetime.fromisoformat(ts1_str.replace("Z", "+00:00"))

        gap_secs = (ts0 - ts1).total_seconds()
        if gap_secs <= 0:
            return None

        gap_days  = int(gap_secs // 86400)
        gap_hours = int((gap_secs % 86400) // 3600)

        if gap_days < REVIVAL_GAP_DAYS:
            return None

        if MIN_LIQUIDITY_USD > 0:
            liquidity = _check_liquidity(token_address)
            if liquidity < MIN_LIQUIDITY_USD:
                log(f"[Scanner] {token_address[:10]}... skipped — "
                    f"liquidity ${liquidity:,.0f} < ${MIN_LIQUIDITY_USD:,.0f}")
                return None

        symbol         = t0.get("asset") or "???"
        name           = _get_token_name(token_address) or symbol
        tier_name, tier_emoji = _gap_tier(gap_days)

        return {
            "token_address": token_address,
            "token_name":    name,
            "token_symbol":  symbol,
            "tx0_datetime":  ts0.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "tx1_datetime":  ts1.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "gap_days":      gap_days,
            "gap_hours":     gap_hours,
            "tier_name":     tier_name,
            "tier_emoji":    tier_emoji,
        }

    except Exception as e:
        log(f"[Scanner] Skipping {token_address[:10]}...: {e}", "warn")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  REAL-TIME PATH — Alchemy WebSocket (V4 PoolManager + V2 pairs)
# ═══════════════════════════════════════════════════════════════════════════════

_seen:      dict[str, float] = {}
_seen_lock: threading.Lock   = threading.Lock()
_SEEN_TTL   = 300  # 5-min dedup per token


def _should_process(token_address: str) -> bool:
    now = time.monotonic()
    with _seen_lock:
        if now - _seen.get(token_address, 0) < _SEEN_TTL:
            return False
        _seen[token_address] = now
        return True


# Queue item formats:
#   ("v4",    tx_hash,       on_revival)  → fetch receipt → tokens
#   ("token", token_address, on_revival)  → process directly
_ws_queue: _queue.Queue = _queue.Queue()

# One subscription catches both V4 PoolManager and V2 pair Swap events.
# Handler differentiates by log.address (V4) vs topic0 == V2_SWAP_TOPIC.
_WS_SUBSCRIBE = json.dumps({
    "jsonrpc": "2.0",
    "id":      1,
    "method":  "eth_subscribe",
    "params":  [
        "logs",
        {"topics": [[V4_SWAP_TOPIC, V2_SWAP_TOPIC]]},
    ],
})


async def _ws_listen(on_revival: callable) -> None:
    import websockets

    while True:
        try:
            async with websockets.connect(
                ALCHEMY_WS_URL,
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                await ws.send(_WS_SUBSCRIBE)
                log("[Scanner] WebSocket connected — V4 PoolManager + V2 pair Swap events")

                async for raw in ws:
                    msg    = json.loads(raw)
                    result = msg.get("params", {}).get("result", {})
                    if not isinstance(result, dict):
                        continue

                    log_address = result.get("address", "").lower()
                    topics      = result.get("topics", [])
                    if not topics:
                        continue

                    if log_address == V4_POOL_MANAGER:
                        # V4: resolve tokens via tx receipt (V4 is a singleton)
                        tx_hash = result.get("transactionHash")
                        if tx_hash:
                            _ws_queue.put_nowait(("v4", tx_hash, on_revival))

                    elif topics[0].lower() == V2_SWAP_TOPIC:
                        # V2: log.address IS the pair contract
                        for token in _revival_candidates_from_pair(log_address):
                            if _should_process(token):
                                _ws_queue.put_nowait(("token", token, on_revival))

        except Exception as e:
            log(f"[Scanner] WebSocket error: {e} — reconnecting in 5s", "warn")
            await asyncio.sleep(5)


def _ws_listener_thread(on_revival: callable) -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_ws_listen(on_revival))


def _ws_worker_thread(on_revival: callable) -> None:
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="ws-proc")
    while True:
        try:
            item = _ws_queue.get(timeout=1)
            if item[0] == "v4":
                _, tx_hash, callback = item
                pool.submit(_process_v4_tx, tx_hash, callback)
            else:
                _, token_address, callback = item
                pool.submit(_process_and_callback, token_address, callback)
        except _queue.Empty:
            continue
        except Exception as e:
            log(f"[Scanner] WS worker error: {e}", "error")


def _process_v4_tx(tx_hash: str, on_revival: callable) -> None:
    """V4 path: find tokens from tx receipt, check each for revival."""
    for token in _tokens_from_receipt(tx_hash):
        if _should_process(token):
            _process_and_callback(token, on_revival)


def _process_and_callback(token_address: str, on_revival: callable) -> None:
    result = _process_token(token_address)
    if result:
        try:
            on_revival(result)
        except Exception as e:
            log(f"[Scanner] Revival callback error: {e}", "error")


def start_ws_listener(on_revival: callable) -> None:
    threading.Thread(
        target=_ws_listener_thread,
        args=(on_revival,),
        daemon=True,
        name="ws-listener",
    ).start()
    threading.Thread(
        target=_ws_worker_thread,
        args=(on_revival,),
        daemon=True,
        name="ws-worker",
    ).start()
    log("[Scanner] Real-time WebSocket listener started")


# ═══════════════════════════════════════════════════════════════════════════════
#  BACKUP SCAN — Alchemy eth_getLogs (last 10 blocks, V4 + V2)
# ═══════════════════════════════════════════════════════════════════════════════

def scan_for_revivals() -> tuple[list, int]:
    """
    Periodic / manual backup scan.

    V4: Alchemy eth_getLogs on V4 PoolManager (10 blocks)
        Etherscan does not index V4 PoolManager as of June 2026.

    V2: Alchemy eth_getLogs topic filter (10 blocks)
        V2 volume is very low; WebSocket handles real-time.

    Both: alchemy_getAssetTransfers for gap check (no Etherscan).
    """
    current_block = int(_rpc("eth_blockNumber", []), 16)
    to_block      = hex(current_block)                      # pin — chain advances during scan
    from_block    = hex(current_block - V4_LOOKBACK)

    token_addresses: set[str] = set()

    # ── V4 ──────────────────────────────────────────────────────────────────
    try:
        v4_logs = _alchemy_logs({
            "address":   V4_POOL_MANAGER,
            "topics":    [V4_SWAP_TOPIC],
            "fromBlock": from_block,
            "toBlock":   to_block,
        })
        tx_hashes_v4 = list({lg["transactionHash"] for lg in v4_logs
                              if lg.get("transactionHash")})
        log(f"[SCAN] V4 swap txns (last {V4_LOOKBACK+1} blocks): {len(tx_hashes_v4)}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for tokens in pool.map(_tokens_from_receipt, tx_hashes_v4):
                token_addresses.update(t for t in tokens if t not in SKIP_TOKENS)

    except Exception as e:
        log(f"[SCAN] V4 scan error: {e}", "warn")

    # ── V2 ──────────────────────────────────────────────────────────────────
    try:
        v2_logs = _alchemy_logs({
            "topics":    [[V2_SWAP_TOPIC]],
            "fromBlock": from_block,
            "toBlock":   to_block,
        })
        pairs_v2 = {lg["address"].lower() for lg in v2_logs if lg.get("address")}
        log(f"[SCAN] V2 pairs (last {V4_LOOKBACK+1} blocks): {len(pairs_v2)}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for tokens in pool.map(_revival_candidates_from_pair, pairs_v2):
                token_addresses.update(tokens)

    except Exception as e:
        log(f"[SCAN] V2 scan error: {e}", "warn")

    # ── Gap check on all discovered tokens ───────────────────────────────────
    buy_count = len(token_addresses)
    if not token_addresses:
        return [], 0

    candidates: list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_process_token, addr): addr for addr in token_addresses}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    candidates.append(result)
            except Exception as e:
                log(f"[Scanner] Worker error: {e}", "error")

    return candidates, buy_count
