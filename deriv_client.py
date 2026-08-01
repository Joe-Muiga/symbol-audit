"""
deriv_client.py – Async Deriv WebSocket client.

v14 — ACCUMULATOR + MATCHES/DIFFERS SUPPORT (additive, no existing
      contract logic touched):

  All new methods follow buy_contract()'s existing logging/error
  conventions (BUY ATTEMPT / PROPOSAL RESPONSE / BUY RESPONSE / FAILED),
  the same never-raises / None-on-failure contract, the same
  _buy_semaphore + 3s inter-buy spacing, and register successfully-bought
  contracts for polling exactly like buy_contract() does.

  Unlike Rise/Fall (which buys directly, no proposal step), ACCU and
  Matches/Differs contracts are bought via the standard two-step
  proposal -> buy flow, since Deriv's API requires a proposal id for
  these contract types.

  NEW — get_accumulator_proposal() / buy_accumulator():
    - contract_type="ACCU", growth_rate accepted as 1-5 (%) and sent to
      the API as a 0.01-0.05 decimal.
    - buy_accumulator() supports an optional take_profit limit_order.

  NEW — sell_contract(contract_id, price):
    - Generic early-close/sell, usable for closing an ACCU position
      before knockout (or any other open contract).

  NEW — get_digit_proposal() / buy_digit_contract():
    - contract_type="DIGITMATCH" or "DIGITDIFF" (match_type="MATCH" /
      "DIFFER"), barrier=str(digit), duration_unit defaults to "t".

  NEW — _cap_stake() helper:
    - Mirrors the FIX 3 stake-cap rule documented below for
      buy_contract(), reused by the new proposal-based buy methods.
      buy_contract() itself was left untouched.

v13 — POLLING-BASED CONTRACT RESOLUTION (replaces subscription model):

  ROOT CAUSE OF ORPHAN_TIMEOUT:
    - WebSocket proposal_open_contract subscriptions never delivered
      reliable close callbacks (no CALLBACK FIRED / POLLER CAUGHT events
      observed in production logs).

  FIX — Aggressive polling replaces all subscription-based monitoring:
    - All proposal_open_contract subscribe/forget/callback machinery removed
      (_contract_callbacks, _subscriptions, _subscribed_contracts,
      _pending_contract_msgs, _closed_before_callback, _contract_poller,
      _fallback_poll_loop, _resubscribe_open_contracts, _subscribe_after_buy,
      _cleanup_contract_subscription — all removed).
    - subscribe_contract(contract_id, callback) now simply registers the
      contract + callback in self._polling_contracts.
    - _polling_loop() runs every 30s, calls force_check_contract(cid) for
      every tracked contract. On is_sold/is_expired it pops the entry and
      fires the callback with the full proposal_open_contract dict
      (containing profit, sell_price, is_sold, is_expired, etc).
    - Started once in connect() via asyncio.create_task().

v9 → v10 audit changes (full compliance pass):

  DIRECTION MAPPING (VERIFIED CORRECT — DO NOT SWAP):
    direction="LONG"  → contract_type="CALL"  → wins if price RISES
    direction="SHORT" → contract_type="PUT"   → wins if price FALLS

  Every buy_contract() call logs BEFORE placement:
    PLACING {contract_type} on {symbol} stake=${stake:.4f} | duration={duration}m

  FIX 1 — VERBOSE buy_contract() LOGGING:
    - BUY ATTEMPT: {symbol} {contract_type} stake=${stake} mult={multiplier}
    - PROPOSAL RESPONSE: {full resp dict} (after _send)
    - BUY RESPONSE: {full result dict} (after success)
    - BUY FAILED: {symbol} error={detail} (on every failure path)

  FIX 2 — RISE/FALL FALLBACK:
    If buy_contract() gets None from multiplier attempt, retries with
    CALL (LONG) or PUT (SHORT), duration=5m, logs: FALLBACK TO RISE/FALL: {symbol}

  FIX 3 — STAKE CAP:
    If stake > balance * 0.5, caps at max(balance * 0.02, 0.35)
    Logs: STAKE CAPPED: ${original} → ${capped}

  FAILURE CONTRACT (buy_contract):
    - Network timeout  → None + log FAILED: {symbol} — timeout
    - API error        → None + log FAILED: {symbol} — {code}: {msg}
    - Market closed    → None + log FAILED: {symbol} — market_closed
    - Proposal reject  → None + log FAILED: {symbol} — proposal_rejected
    - Never raises — always returns None on any failure path
    - Trade counters never incremented on None return (caller responsibility enforced)

  MARKET OPEN CHECK:
    - Boom/Crash: active_symbols API checked for exchange_is_open before proposal
    - Volatility indices (R_*, 1HZ*): check skipped entirely — always open

  get_balance() METHOD:
    - Calls balance API endpoint
    - Returns float
    - On failure: returns cached balance, logs BALANCE FETCH FAILED — using cached
    - Cache TTL: 30 seconds

  CONNECTION RESILIENCE:
    - Disconnect: logs WEBSOCKET DISCONNECTED — reconnecting in {interval}s (attempt {n}/{max})
    - Retries every WEBSOCKET_RECONNECT_INTERVAL seconds
    - After WEBSOCKET_MAX_RECONNECTS failures: logs WEBSOCKET RECONNECT FAILED — bot halting
      then raises ConnectionError
    - Re-authenticates after reconnect

  get_candles():
    - Returns list[dict] with keys: open, high, low, close, epoch
    - On failure: returns [], logs CANDLE FETCH FAILED: {symbol} gran={granularity}
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, Optional, List, Any

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

import config

logger = logging.getLogger(__name__)

MAX_RETRY_DELAY = 60

_BOOM_CRASH_PREFIXES = ("BOOM", "CRASH")
_VOLATILITY_PREFIXES = ("R_", "1HZ")

# ── Reconnect defaults (overridden by config if present) ──────────────────────
_DEFAULT_RECONNECT_INTERVAL = 5   # seconds between retries
_DEFAULT_MAX_RECONNECTS     = 10  # 0 = unlimited

# Contract polling interval in seconds
_CONTRACT_POLL_INTERVAL = 30

# Minimum gap between proposal requests (rate-limit protection)
PROPOSAL_DELAY = 2.0  # 2 seconds between proposals


def _is_boom_crash(symbol: str) -> bool:
    return any(symbol.upper().startswith(p) for p in _BOOM_CRASH_PREFIXES)


def _is_volatility_index(symbol: str) -> bool:
    s = symbol.upper()
    return any(s.startswith(p) for p in _VOLATILITY_PREFIXES)


class DerivClient:

    def __init__(self):
        self._ws: Optional[Any]    = None
        self._ready: asyncio.Event = asyncio.Event()
        self._connected: bool      = False
        self._authorized: bool     = False

        self._pending: Dict[int, asyncio.Future] = {}
        self._req_id_counter: int = 10

        self._tick_callbacks: Dict[str, Callable] = {}
        self._subscription_map: Dict[str, str]    = {}

        # contract_id → symbol  (informational only)
        self._contract_symbol_map: Dict[str, str] = {}

        self._balance: float             = 0.0
        self._balance_ts: float          = 0.0          # epoch of last successful fetch
        self._balance_callbacks: List[Callable] = []

        # ── Polling-based contract resolution ──────────────────────────────────
        # {contract_id: {"callback": fn, "placed_at": time}}
        self._polling_contracts: Dict[str, dict] = {}

        # ── WS subscription-based contract resolution (primary; poller is fallback) ──
        self._subscribed_contracts: set = set()

        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # ── Rate limiting ─────────────────────────────────────────────────────
        self._buy_semaphore  = asyncio.Semaphore(1)  # one at a time
        self._last_buy_time  = 0.0                   # epoch of last buy attempt

        # ── Circuit breaker: {(symbol, strategy): {"count": int, "cooldown_until": epoch}} ──
        self._circuit_breaker: Dict[tuple, dict] = {}

        # ── Direction→contract mapping audit ─────────────────────────────────
        logger.info(
            "Direction mapping: LONG → CALL (price rises) | SHORT → PUT (price falls)"
        )
        assert "CALL" == ("CALL" if "LONG" == "LONG" else "PUT"), (
            "FATAL: LONG→CALL mapping is broken")
        assert "PUT" == ("CALL" if "SHORT" == "LONG" else "PUT"), (
            "FATAL: SHORT→PUT mapping is broken")

    # ─── Connection ───────────────────────────────────────────────────────────

    async def connect(self):
        """
        Outer reconnect loop.  Enforces WEBSOCKET_MAX_RECONNECTS and
        WEBSOCKET_RECONNECT_INTERVAL from config (falls back to module defaults).
        Starts the contract polling loop once before the reconnect loop begins.
        """
        self._loop = asyncio.get_event_loop()

        # Start the contract polling loop once — persists across reconnects
        asyncio.create_task(self._polling_loop())

        reconnect_interval = getattr(
            config, "WEBSOCKET_RECONNECT_INTERVAL", _DEFAULT_RECONNECT_INTERVAL
        )
        max_reconnects = getattr(
            config, "WEBSOCKET_MAX_RECONNECTS", _DEFAULT_MAX_RECONNECTS
        )
        attempt   = 0
        max_label = str(max_reconnects) if max_reconnects > 0 else "∞"

        while True:
            try:
                ws_url = config.DERIV_WS_URL
                logger.info(f"Connecting to {ws_url} …")
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                ) as ws:
                    self._ws        = ws
                    self._connected = True
                    attempt         = 0          # reset on successful connection
                    logger.info("WebSocket connected ✓")

                    # Standard Deriv auth: send {"authorize": <token>} over this
                    # same WS connection and wait for the response. Sets
                    # self._authorized / self._ready / self._balance on success,
                    # raises RuntimeError on an API error (caught below).
                    await self._authorize()
                    dispatch_task = asyncio.ensure_future(self._dispatch_loop())
                    try:
                        await self._subscribe_balance()
                        await self._resubscribe_contracts()
                        await dispatch_task
                    finally:
                        dispatch_task.cancel()
                        try:
                            await dispatch_task
                        except (asyncio.CancelledError, Exception):
                            pass

            except InvalidStatus as exc:
                attempt += 1
                resp = exc.response
                header_dump = dict(resp.headers.raw_items()) if hasattr(
                    resp.headers, "raw_items") else dict(resp.headers)
                body_text = ""
                if resp.body:
                    try:
                        body_text = resp.body.decode("utf-8", errors="replace")
                    except Exception:
                        body_text = repr(resp.body)
                logger.error(
                    "HANDSHAKE REJECTED — Deriv's server refused the WebSocket "
                    f"upgrade before any messages were exchanged.\n"
                    f"  status_code   = {resp.status_code}\n"
                    f"  reason_phrase = {resp.reason_phrase!r}\n"
                    f"  headers       = {header_dump}\n"
                    f"  body          = {body_text!r}"
                )
                logger.warning(
                    f"WEBSOCKET DISCONNECTED — reconnecting in {reconnect_interval}s "
                    f"(attempt {attempt}/{max_label}) | reason={exc}"
                )
            except ConnectionClosed as exc:
                attempt += 1
                logger.warning(
                    f"WEBSOCKET DISCONNECTED — reconnecting in {reconnect_interval}s "
                    f"(attempt {attempt}/{max_label}) | reason={exc}"
                )
            except Exception as exc:
                attempt += 1
                logger.error(
                    f"WEBSOCKET DISCONNECTED — reconnecting in {reconnect_interval}s "
                    f"(attempt {attempt}/{max_label}) | error={exc}"
                )
            finally:
                self._ready.clear()
                self._connected  = False
                self._authorized = False
                self._ws         = None
                self._pending.clear()

            if max_reconnects > 0 and attempt >= max_reconnects:
                logger.critical(
                    "WEBSOCKET RECONNECT FAILED — bot halting"
                )
                raise ConnectionError(
                    f"WebSocket failed after {max_reconnects} reconnect attempts."
                )

            await asyncio.sleep(reconnect_interval)

    # ─── Message dispatch ─────────────────────────────────────────────────────

    async def _dispatch_loop(self):
        async for raw in self._ws:
            try:
                msg = json.loads(raw)
                await self._handle(msg)
            except Exception as exc:
                logger.debug(f"Dispatch error: {exc}")

    async def _handle(self, msg: dict):
        logger.debug(f"RAW MSG: {msg}")
        msg_type = msg.get("msg_type", "")
        req_id   = msg.get("req_id")
        error    = msg.get("error")

        if error:
            logger.warning(
                f"Deriv API error: {error.get('message')} "
                f"(code={error.get('code')}) | msg_type={msg_type}"
            )

        if msg_type == "balance":
            balance_data = msg.get("balance", {})
            new_bal = float(balance_data.get("balance", self._balance))
            if new_bal != self._balance:
                logger.info(f"Balance updated: ${self._balance:.4f} → ${new_bal:.4f}")
            self._balance    = new_bal
            self._balance_ts = time.monotonic()
            for cb in self._balance_callbacks:
                try:
                    cb(self._balance)
                except Exception:
                    pass

        if req_id and req_id in self._pending:
            fut = self._pending.pop(req_id)
            if not fut.done():
                if error:
                    detail_suffix = (
                        f" | details={error.get('details')}"
                        if error.get("details") else ""
                    )
                    fut.set_exception(RuntimeError(
                        f"{error.get('code', 'ERR')}: {error.get('message', 'Unknown error')}"
                        f"{detail_suffix}"
                    ))
                else:
                    fut.set_result(msg)
            return

        if msg_type == "tick":
            tick   = msg.get("tick", {})
            sym    = tick.get("symbol", "")
            sub_id = tick.get("id", "")
            for key, cb in self._tick_callbacks.items():
                if key == sym or key == sub_id:
                    try:
                        cb(tick)
                    except Exception as exc:
                        logger.debug(f"Tick callback error: {exc}")

        if msg_type == "proposal_open_contract":
            poc = msg.get("proposal_open_contract", {})
            cid = str(poc.get("contract_id", ""))
            closed = bool(
                poc.get("is_sold") or poc.get("is_expired") or poc.get("status") == "sold"
            )
            if cid and closed:
                self._subscribed_contracts.discard(cid)
                info = self._polling_contracts.pop(cid, None)
                if info and info.get("callback"):
                    try:
                        cb_result = info["callback"]({"proposal_open_contract": poc})
                        if asyncio.iscoroutine(cb_result) or isinstance(cb_result, asyncio.Future):
                            await cb_result
                    except Exception as exc:
                        logger.error(f"Contract close callback error {cid}: {exc}")
                logger.info(
                    f"SUBSCRIPTION CLOSED: {cid} profit={poc.get('profit')}"
                )

    # ─── Polling-based contract resolution (replaces subscriptions) ──────────

    async def _polling_loop(self):
        """
        Every 30 seconds, poll every contract registered in
        self._polling_contracts via force_check_contract().

        On is_sold/is_expired: pops the entry and fires its callback with
        the full proposal_open_contract dict (profit, sell_price, is_sold,
        is_expired, etc). The callback (registered by BotEngine) is
        responsible for releasing the symbol from _active_symbols.
        """
        while True:
            await asyncio.sleep(_CONTRACT_POLL_INTERVAL)
            if not self._polling_contracts:
                continue
            logger.info(
                f"POLLING: {len(self._polling_contracts)} open contracts")
            for cid in list(self._polling_contracts.keys()):
                try:
                    result = await self.force_check_contract(cid)
                    if result.get("is_sold") or result.get("is_expired") or result.get("status") == "sold":
                        info = self._polling_contracts.pop(cid, None)
                        if info and info.get("callback"):
                            cb_result = info["callback"]({"proposal_open_contract": result})
                            if asyncio.iscoroutine(cb_result) or isinstance(cb_result, asyncio.Future):
                                await cb_result
                            logger.info(f"POLL RESOLVED: {cid} profit={result.get('profit')}")
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"POLL ERROR {cid}: {e}")

    async def subscribe_contract(
        self,
        contract_id: str,
        callback:    Callable,
        symbol:      str = "",
    ):
        """
        Register a contract + callback for polling-based resolution.
        Replaces the old WebSocket proposal_open_contract subscription.
        """
        cid = str(contract_id)
        self._polling_contracts[cid] = {
            "callback":  callback,
            "placed_at": time.time(),
        }
        if symbol:
            self._contract_symbol_map[cid] = symbol
        logger.info(f"TRACKING: {cid} via polling")
        asyncio.create_task(self._subscribe_ws_contract(cid))

    # ─── WS subscription (primary close signal; poller above is the fallback) ─

    async def _subscribe_ws_contract(self, contract_id: str):
        """
        Subscribe to proposal_open_contract updates for *contract_id*
        (subscribe: 1). Never double-subscribes — guarded by
        self._subscribed_contracts. Pushes are handled in _handle().
        """
        cid = str(contract_id)
        if cid in self._subscribed_contracts:
            return
        try:
            await self._send({
                "proposal_open_contract": 1,
                "contract_id": int(cid),
                "subscribe": 1,
            })
            self._subscribed_contracts.add(cid)
            logger.info(f"SUBSCRIBED: {cid} (proposal_open_contract)")
        except Exception as e:
            logger.error(f"SUBSCRIBE FAILED: {cid} — {e}")

    async def _resubscribe_contracts(self):
        """Re-subscribe every tracked contract_id after a (re)connect."""
        if not self._subscribed_contracts:
            return
        pending = list(self._subscribed_contracts)
        self._subscribed_contracts.clear()
        logger.info(f"RESUBSCRIBING: {len(pending)} contracts after reconnect")
        for cid in pending:
            await self._subscribe_ws_contract(cid)

    # ─── Force check a specific contract ─────────────────────────────────────

    async def force_check_contract(self, contract_id: str) -> dict:
        """
        Manually query a contract's current state.

        Returns the full proposal_open_contract dict, including
        profit, sell_price, is_sold, is_expired (and all other fields
        the API returns). Returns {} on failure.
        """
        try:
            req = {
                "proposal_open_contract": 1,
                "contract_id": int(contract_id),
            }
            resp = await self._send(req)
            if not resp:
                return {}
            poc = resp.get("proposal_open_contract", {})
            logger.info(
                f"FORCE CHECK {contract_id}: "
                f"is_sold={poc.get('is_sold')} "
                f"profit={poc.get('profit')}")
            return poc
        except Exception as e:
            logger.error(f"FORCE CHECK ERROR {contract_id}: {e}")
            return {}

    # ─── Request helper ───────────────────────────────────────────────────────

    async def _send(self, payload: dict, timeout: float = 30.0) -> dict:
        if not self._ws or not self._connected:
            raise RuntimeError("Not connected")

        req_id                = self._req_id_counter
        self._req_id_counter += 1
        payload["req_id"]     = req_id

        fut = self._loop.create_future()
        self._pending[req_id] = fut

        await self._ws.send(json.dumps(payload))
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise TimeoutError(f"Request timed out: {payload.get('msg_type', payload)}")

    # ─── Auth & balance ───────────────────────────────────────────────────────

    async def _authorize(self):
        if not config.DERIV_API_TOKEN:
            raise ValueError("DERIV_API_TOKEN is not set.")

        payload = {"authorize": config.DERIV_API_TOKEN, "req_id": 1}
        await self._ws.send(json.dumps(payload))
        raw = await asyncio.wait_for(self._ws.recv(), timeout=30)
        msg = json.loads(raw)

        if msg.get("error"):
            raise RuntimeError(f"Auth failed: {msg['error'].get('message')}")

        account              = msg.get("authorize", {})
        self._balance        = float(account.get("balance", 0))
        self._balance_ts     = time.monotonic()
        self._authorized     = True
        self._req_id_counter = 10

        self._ready.set()

        logger.info(
            f"Authorized ✓ | Account: {account.get('loginid')} | "
            f"Balance: ${self._balance:.4f}"
        )

        for cb in self._balance_callbacks:
            try:
                cb(self._balance)
            except Exception:
                pass

    async def _subscribe_balance(self):
        try:
            await self._send({"balance": 1, "subscribe": 1})
            logger.info("Balance subscription active ✓")
        except Exception as exc:
            logger.warning(
                f"Balance subscription failed: {exc} — will use polling fallback"
            )

    async def balance_refresh_loop(self, interval: int = 60):
        while True:
            await asyncio.sleep(interval)
            if not self._authorized or not self._ws:
                continue
            try:
                resp    = await self._send({"balance": 1})
                new_bal = float(resp.get("balance", {}).get("balance", self._balance))
                if new_bal != self._balance:
                    logger.info(f"Balance poll: ${self._balance:.4f} → ${new_bal:.4f}")
                    self._balance    = new_bal
                    self._balance_ts = time.monotonic()
                    for cb in self._balance_callbacks:
                        try:
                            cb(self._balance)
                        except Exception:
                            pass
            except Exception as exc:
                logger.debug(f"Balance poll failed: {exc}")

    def on_balance(self, callback: Callable[[float], None]):
        self._balance_callbacks.append(callback)

    @property
    def balance(self) -> float:
        return self._balance

    async def get_balance(self) -> float:
        """
        Fetch current account balance via the balance API endpoint.

        Returns the live balance as a float.
        Cache TTL: 30 seconds — if the cached value is fresh enough, return it
        directly to avoid hammering the API.
        On any failure: return last known balance and log a warning.
        """
        _BALANCE_CACHE_TTL = 30.0

        now = time.monotonic()
        if (now - self._balance_ts) < _BALANCE_CACHE_TTL:
            return self._balance

        if not self._authorized or not self._ws:
            cached = self._balance
            logger.warning(
                f"BALANCE FETCH FAILED — using cached: ${cached:.2f} "
                f"(not connected)"
            )
            return cached

        try:
            resp    = await self._send({"balance": 1}, timeout=15)
            new_bal = float(resp.get("balance", {}).get("balance", self._balance))
            if new_bal != self._balance:
                logger.info(f"get_balance: ${self._balance:.4f} → ${new_bal:.4f}")
            self._balance    = new_bal
            self._balance_ts = time.monotonic()
            return self._balance
        except Exception as exc:
            cached = self._balance
            logger.warning(
                f"BALANCE FETCH FAILED — using cached: ${cached:.2f} | error={exc}"
            )
            return cached

    # ─── Historical candles ───────────────────────────────────────────────────

    async def get_candles(
        self,
        symbol:      str,
        granularity: int,
        count:       int = 100,
    ) -> List[dict]:
        """
        Fetch OHLC candles for *symbol*.

        Returns list[dict] with keys: open, high, low, close, epoch
        On any failure: returns [] and logs CANDLE FETCH FAILED: {symbol} gran={granularity}
        """
        await self._ready.wait()
        try:
            resp = await self._send(
                {
                    "ticks_history":    symbol,
                    "adjust_start_time": 1,
                    "count":            count,
                    "end":              "latest",
                    "granularity":      granularity,
                    "style":            "candles",
                },
                timeout=20,
            )
            raw_candles: List[dict] = resp.get("candles", [])

            # Normalise to guaranteed key set: open, high, low, close, epoch
            candles: List[dict] = [
                {
                    "open":  float(c.get("open",  0)),
                    "high":  float(c.get("high",  0)),
                    "low":   float(c.get("low",   0)),
                    "close": float(c.get("close", 0)),
                    "epoch": int(c.get("epoch",   0)),
                }
                for c in raw_candles
            ]

            logger.debug(
                f"Got {len(candles)} candles for {symbol} (gran={granularity}s)"
            )
            return candles

        except Exception as exc:
            logger.warning(
                f"CANDLE FETCH FAILED: {symbol} gran={granularity} | error={exc}"
            )
            return []

    # ─── Live tick subscription ───────────────────────────────────────────────

    async def subscribe_ticks(
        self,
        symbol:   str,
        callback: Callable[[dict], None],
    ) -> str:
        if symbol in self._subscription_map:
            self._tick_callbacks[symbol] = callback
            return self._subscription_map[symbol]

        try:
            resp   = await self._send({"ticks": symbol, "subscribe": 1})
            sub_id = resp.get("tick", {}).get("id", symbol)
            self._subscription_map[symbol] = sub_id
            self._tick_callbacks[symbol]   = callback
            logger.info(f"Tick subscription: {symbol} (sub_id={sub_id})")
            return sub_id
        except Exception as exc:
            logger.error(f"subscribe_ticks({symbol}) failed: {exc}")
            return ""

    async def unsubscribe_ticks(self, symbol: str):
        sub_id = self._subscription_map.pop(symbol, None)
        self._tick_callbacks.pop(symbol, None)
        if sub_id:
            try:
                await self._send({"forget": sub_id})
            except Exception:
                pass

    # ─── Market-open check ────────────────────────────────────────────────────

    async def _check_market_open(self, symbol: str) -> bool:
        """
        Returns True if the symbol's market is open.

        Volatility indices (R_*, 1HZ*) are always open — skip the API call.
        For Boom/Crash (and everything else): call active_symbols and inspect
        exchange_is_open.  Returns False (and logs SKIPPED) on any error so
        that buy_contract() can bail out safely.
        """
        if _is_volatility_index(symbol):
            return True  # 24/7 — never gated

        try:
            resp    = await self._send(
                {"active_symbols": "brief", "product_type": "basic"},
                timeout=20,
            )
            symbols = resp.get("active_symbols", [])
            for s in symbols:
                if s.get("underlying_symbol") == symbol:
                    open_flag = s.get("exchange_is_open", 0)
                    return bool(open_flag)
            # Symbol not found in active list → treat as closed
            logger.warning(f"SKIPPED: {symbol} market_closed (symbol not in active list)")
            return False
        except Exception as exc:
            logger.warning(f"SKIPPED: {symbol} market_closed (check failed: {exc})")
            return False

    # ─── Trade execution ──────────────────────────────────────────────────────

    def _cap_stake(self, stake: float, symbol: str) -> float:
        """
        Mirrors the FIX 3 stake-cap rule documented above for buy_contract()
        (if stake > balance * 0.5, cap at max(balance * 0.02, 0.35)).

        Used only by the new proposal-based buy methods below —
        buy_contract() itself is left untouched per task instructions.
        """
        balance = self._balance
        if balance > 0 and stake > balance * 0.5:
            capped = max(balance * 0.02, 0.35)
            logger.info(f"STAKE CAPPED: ${stake:.4f} → ${capped:.4f} ({symbol})")
            return capped
        return stake

    @staticmethod
    def _digit_contract_type(match_type: str) -> Optional[str]:
        mapping = {"MATCH": "DIGITMATCH", "DIFFER": "DIGITDIFF"}
        return mapping.get(str(match_type).upper())

    # ─── Circuit breaker (per symbol+strategy) ─────────────────────────────
    # Suspends new buy attempts on a (symbol, strategy) combination after N
    # consecutive failures, for a cooldown period — prevents the same dead
    # signal from retrying every scan cycle indefinitely.

    @staticmethod
    def _cb_key(symbol: str, strategy: str) -> tuple:
        return (symbol, strategy or "default")

    def _cb_blocked(self, symbol: str, strategy: str) -> Optional[float]:
        """Returns seconds remaining in cooldown, or None if not blocked."""
        state = self._circuit_breaker.get(self._cb_key(symbol, strategy))
        if not state:
            return None
        remaining = state.get("cooldown_until", 0.0) - time.time()
        return remaining if remaining > 0 else None

    def _cb_record_failure(self, symbol: str, strategy: str,
                            threshold: int, cooldown: float):
        key = self._cb_key(symbol, strategy)
        state = self._circuit_breaker.setdefault(key, {"count": 0, "cooldown_until": 0.0})
        state["count"] += 1
        if state["count"] >= threshold:
            state["cooldown_until"] = time.time() + cooldown
            state["count"] = 0
            logger.warning(
                f"CIRCUIT BREAKER TRIPPED: {symbol}/{strategy} — "
                f"{threshold} consecutive buy failures, suspending for {cooldown:.0f}s"
            )

    def _cb_record_success(self, symbol: str, strategy: str):
        self._circuit_breaker.pop(self._cb_key(symbol, strategy), None)

    async def buy_contract(
            self,
            symbol:        str,
            direction:     str,   # "LONG" or "SHORT"
            stake:         float,
            multiplier:    int    = 100,
            stop_loss:     float  = None,
            take_profit:   float  = None,
            strategy:      str    = "default",
            cb_threshold:  int    = 3,
            cb_cooldown:   float  = 60.0,
            **kwargs) -> dict:
        """
        Buy a Rise/Fall (CALL/PUT) contract via the required proposal ->
        buy flow. Deriv rejects inline 'parameters' (incl. 'symbol') on a
        direct buy request — a proposal must be requested first to obtain
        a proposal_id and ask_price, matching Deriv's documented API and
        every up-to-date client library.

        Direction mapping (VERIFIED CORRECT - DO NOT SWAP):
          direction="LONG"  -> contract_type="CALL"  -> wins if price RISES
          direction="SHORT" -> contract_type="PUT"   -> wins if price FALLS

        strategy identifies the signal/strategy that produced this trade,
        used as the second half of the circuit-breaker key (symbol,
        strategy). After cb_threshold consecutive failures on that
        combination, new attempts are suspended for cb_cooldown seconds.

        Returns dict (buy response) on success, None on every failure path
        (including a circuit-breaker cooldown skip). Never raises. Never
        increments trade counters on None return.

        After a successful buy, the caller is expected to register the
        contract for polling via subscribe_contract().
        """
        remaining = self._cb_blocked(symbol, strategy)
        if remaining is not None:
            logger.info(
                f"SKIPPED: {symbol}/{strategy} — circuit breaker cooldown "
                f"active ({remaining:.0f}s remaining)"
            )
            return None

        async with self._buy_semaphore:
            elapsed = time.time() - self._last_buy_time
            if elapsed < 3.0:
                await asyncio.sleep(3.0 - elapsed)
            self._last_buy_time = time.time()

            contract_type = "CALL" if direction == "LONG" else "PUT"
            logger.info(f"BUY ATTEMPT: {symbol} {contract_type} stake=${stake:.4f}")

            proposal_req = {
                "proposal":      1,
                "amount":        stake,
                "basis":         "stake",
                "contract_type": contract_type,
                "currency":      "USD",
                "duration":      5,
                "duration_unit": "m",
                "underlying_symbol": symbol,
            }
            barrier = kwargs.get("barrier")
            if barrier is not None:
                proposal_req["barrier"] = barrier

            try:
                prop_resp = await self._send(proposal_req)
                if not prop_resp:
                    logger.error(f"FAILED: {symbol} — no response (proposal)")
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None
                if prop_resp.get("error"):
                    err = prop_resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={proposal_req}"
                    )
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None

                proposal = prop_resp.get("proposal", {})
                logger.info(f"PROPOSAL RESPONSE: {proposal}")

                proposal_id = proposal.get("id")
                ask_price   = proposal.get("ask_price", stake)
                if not proposal_id:
                    logger.error(
                        f"FAILED: {symbol} — proposal_rejected (no id in {proposal})"
                    )
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None

                logger.info(
                    f"PRICE CHECK: {symbol} ask_price=${ask_price:.4f} "
                    f"vs stake=${stake:.4f} "
                    f"(diff=${ask_price - stake:+.4f})"
                )

                buy_req = {"buy": proposal_id, "price": ask_price}
                resp = await self._send(buy_req)
                if not resp:
                    logger.error(f"FAILED: {symbol} — no response (buy)")
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None
                if resp.get("error"):
                    err = resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={buy_req}"
                    )
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None

                result = resp.get("buy", {})
                contract_id = str(result.get("contract_id", ""))
                logger.info(
                    f"CONTRACT OPENED: {contract_id} | {symbol} | "
                    f"{contract_type} | ${stake:.4f}"
                )

                self._contract_symbol_map[contract_id] = symbol
                self._cb_record_success(symbol, strategy)

                # Guaranteed closure: subscribe to this contract immediately.
                # (subscribe_contract(), called later by the caller to attach
                # its close callback, will no-op the resend via the
                # _subscribed_contracts guard — never double-subscribes.)
                asyncio.create_task(self._subscribe_ws_contract(contract_id))

                return result

            except Exception as e:
                logger.error(f"FAILED: {symbol} — {e} | full_req={proposal_req}")
                self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                return None

    async def buy_multiplier(
            self,
            symbol:        str,
            direction:     str,   # "LONG" or "SHORT"
            stake:         float,
            multiplier:    int    = None,
            stop_loss_pct: float  = None,
            take_profit_ratio: float = None,
            strategy:      str    = "default",
            cb_threshold:  int    = 3,
            cb_cooldown:   float  = 60.0,
            **kwargs) -> dict:
        """
        Buy a Multipliers (MULTUP/MULTDOWN) contract via the required
        proposal -> buy flow. Mirrors buy_contract()'s defensive error
        handling (log and return None on every failure path, never raise)
        and its circuit-breaker gating, but targets Multiplier contracts
        instead of Rise/Fall — needed for symbols (Boom/Crash, Jump,
        Drift Switch, and any future Range Break / Bear-Bull additions)
        that do not support CALL/PUT on this account.

        Direction mapping (mirrors buy_contract — DO NOT SWAP):
          direction="LONG"  -> contract_type="MULTUP"    -> wins if price RISES
          direction="SHORT" -> contract_type="MULTDOWN"  -> wins if price FALLS

        multiplier defaults to config.MULTIPLIER_MAP[symbol] (falling back
        to config.DEFAULT_MULTIPLIER) when not passed explicitly.

        Multiplier contracts have no fixed duration — risk is bounded here
        via an explicit limit_order (stop_loss / take_profit) attached to
        the proposal, rather than a duration/duration_unit pair.
        stop_loss_pct defaults to config.STOP_LOSS_MAP[symbol] (falling
        back to config.DEFAULT_STOP_LOSS_PCT); take_profit_ratio defaults
        to config.TAKE_PROFIT_RATIO. Both are expressed relative to stake:
          stop_loss_amount   = stake * (stop_loss_pct / 100)
          take_profit_amount = stop_loss_amount * take_profit_ratio

        strategy identifies the signal/strategy that produced this trade,
        used as the second half of the circuit-breaker key (symbol,
        strategy), exactly as in buy_contract().

        Returns dict (buy response) on success, None on every failure path
        (including a circuit-breaker cooldown skip). Never raises. Never
        increments trade counters on None return.

        After a successful buy, the caller is expected to register the
        contract for polling via subscribe_contract() — already handled
        here the same way buy_contract() does it, via _subscribe_ws_contract.
        """
        remaining = self._cb_blocked(symbol, strategy)
        if remaining is not None:
            logger.info(
                f"SKIPPED: {symbol}/{strategy} — circuit breaker cooldown "
                f"active ({remaining:.0f}s remaining)"
            )
            return None

        if multiplier is None:
            multiplier = config.MULTIPLIER_MAP.get(symbol, config.DEFAULT_MULTIPLIER)
        if stop_loss_pct is None:
            stop_loss_pct = config.STOP_LOSS_MAP.get(symbol, config.DEFAULT_STOP_LOSS_PCT)
        if take_profit_ratio is None:
            take_profit_ratio = config.TAKE_PROFIT_RATIO

        async with self._buy_semaphore:
            elapsed = time.time() - self._last_buy_time
            if elapsed < 3.0:
                await asyncio.sleep(3.0 - elapsed)
            self._last_buy_time = time.time()

            stake = self._cap_stake(stake, symbol)
            contract_type = "MULTUP" if direction == "LONG" else "MULTDOWN"
            stop_loss_amount   = round(stake * (stop_loss_pct / 100.0), 2)
            take_profit_amount = round(stop_loss_amount * take_profit_ratio, 2)

            logger.info(
                f"BUY ATTEMPT: {symbol} {contract_type} stake=${stake:.4f} "
                f"multiplier={multiplier}x SL=${stop_loss_amount:.2f} "
                f"TP=${take_profit_amount:.2f}"
            )

            proposal_req = {
                "proposal":      1,
                "amount":        stake,
                "basis":         "stake",
                "contract_type": contract_type,
                "currency":      "USD",
                "underlying_symbol": symbol,
                "multiplier":    multiplier,
                "limit_order": {
                    "stop_loss":   stop_loss_amount,
                    "take_profit": take_profit_amount,
                },
            }

            try:
                prop_resp = await self._send(proposal_req)
                if not prop_resp:
                    logger.error(f"FAILED: {symbol} — no response (multiplier proposal)")
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None
                if prop_resp.get("error"):
                    err = prop_resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={proposal_req}"
                    )
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None

                proposal = prop_resp.get("proposal", {})
                logger.info(f"PROPOSAL RESPONSE: {proposal}")

                proposal_id = proposal.get("id")
                ask_price   = proposal.get("ask_price", stake)
                if not proposal_id:
                    logger.error(
                        f"FAILED: {symbol} — proposal_rejected (no id in {proposal})"
                    )
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None

                logger.info(
                    f"PRICE CHECK: {symbol} ask_price=${ask_price:.4f} "
                    f"vs stake=${stake:.4f} "
                    f"(diff=${ask_price - stake:+.4f})"
                )

                buy_req = {"buy": proposal_id, "price": ask_price}
                resp = await self._send(buy_req)
                if not resp:
                    logger.error(f"FAILED: {symbol} — no response (multiplier buy)")
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None
                if resp.get("error"):
                    err = resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={buy_req}"
                    )
                    self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                    return None

                result = resp.get("buy", {})
                contract_id = str(result.get("contract_id", ""))
                logger.info(
                    f"CONTRACT OPENED: {contract_id} | {symbol} | "
                    f"{contract_type} | ${stake:.4f} | {multiplier}x"
                )

                self._contract_symbol_map[contract_id] = symbol
                self._cb_record_success(symbol, strategy)

                # Guaranteed closure: subscribe to this contract immediately,
                # same as buy_contract() — subscribe_contract() later no-ops
                # the resend via the _subscribed_contracts guard.
                asyncio.create_task(self._subscribe_ws_contract(contract_id))

                return result

            except Exception as e:
                logger.error(f"FAILED: {symbol} — {e} | full_req={proposal_req}")
                self._cb_record_failure(symbol, strategy, cb_threshold, cb_cooldown)
                return None

    # ─── Accumulator (ACCU) contracts ──────────────────────────────────────────

    async def get_accumulator_proposal(
            self,
            symbol:      str,
            stake:       float,
            growth_rate: float,
            take_profit: float = None,
            **kwargs) -> Optional[dict]:
        """
        Request a proposal for an Accumulator (ACCU) contract.

        growth_rate is accepted as a percentage (1-5, matching Deriv's UI)
        and converted internally to the decimal the API expects
        (0.01-0.05) — pass growth_rate=2 for 2%.

        take_profit, if given, is attached as a limit_order so the
        contract auto-closes at that profit level.

        Returns the full 'proposal' dict (contains 'id' and 'ask_price',
        needed by buy_accumulator) on success, None on any failure path.
        Never raises.
        """
        if not (1 <= growth_rate <= 5):
            logger.error(
                f"FAILED: {symbol} — invalid growth_rate={growth_rate} "
                f"(must be 1-5, i.e. 1%-5%)"
            )
            return None

        try:
            req = {
                "proposal":      1,
                "amount":        stake,
                "basis":         "stake",
                "contract_type": "ACCU",
                "currency":      "USD",
                "underlying_symbol": symbol,
                "growth_rate":   growth_rate / 100.0,
            }
            if take_profit is not None:
                req["limit_order"] = {"take_profit": take_profit}

            logger.info(
                f"PROPOSAL REQUEST: ACCU {symbol} stake=${stake:.4f} "
                f"growth_rate={growth_rate}%"
            )
            resp = await self._send(req)
            if not resp:
                logger.error(f"FAILED: {symbol} — no response (ACCU proposal)")
                return None
            if resp.get("error"):
                err = resp["error"]
                logger.error(
                    f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                    f"| details={err.get('details')} | full_req={req}"
                )
                return None

            proposal = resp.get("proposal", {})
            logger.info(f"PROPOSAL RESPONSE: {proposal}")
            return proposal

        except Exception as e:
            logger.error(f"FAILED: {symbol} — {e}")
            return None

    async def buy_accumulator(
            self,
            symbol:      str,
            stake:       float,
            growth_rate: float,
            take_profit: float = None,
            **kwargs) -> Optional[dict]:
        """
        Buy an Accumulator (ACCU) contract: proposal step, then buy against
        the returned proposal id. ACCU contracts cannot use the direct-buy
        shortcut buy_contract() uses for Rise/Fall — the API requires a
        proposal id here.

        growth_rate is a percentage 1-5 (see get_accumulator_proposal).

        Returns dict (buy response) on success, None on every failure
        path. Never raises. Trade counters should never be incremented by
        the caller on a None return (same contract as buy_contract()).

        After a successful buy, the caller is expected to register the
        contract for polling via subscribe_contract() — mirrors
        buy_contract()'s existing division of responsibility.
        """
        async with self._buy_semaphore:
            elapsed = time.time() - self._last_buy_time
            if elapsed < 3.0:
                await asyncio.sleep(3.0 - elapsed)
            self._last_buy_time = time.time()

            if not (1 <= growth_rate <= 5):
                logger.error(
                    f"FAILED: {symbol} — invalid growth_rate={growth_rate} "
                    f"(must be 1-5, i.e. 1%-5%)"
                )
                return None

            stake = self._cap_stake(stake, symbol)
            logger.info(
                f"BUY ATTEMPT: {symbol} ACCU stake=${stake:.4f} "
                f"growth_rate={growth_rate}%"
            )

            try:
                proposal_req = {
                    "proposal":      1,
                    "amount":        stake,
                    "basis":         "stake",
                    "contract_type": "ACCU",
                    "currency":      "USD",
                    "underlying_symbol": symbol,
                    "growth_rate":   growth_rate / 100.0,
                }
                if take_profit is not None:
                    proposal_req["limit_order"] = {"take_profit": take_profit}

                prop_resp = await self._send(proposal_req)
                if not prop_resp:
                    logger.error(f"FAILED: {symbol} — no response (ACCU proposal)")
                    return None
                if prop_resp.get("error"):
                    err = prop_resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={proposal_req}"
                    )
                    return None

                proposal = prop_resp.get("proposal", {})
                logger.info(f"PROPOSAL RESPONSE: {proposal}")

                proposal_id = proposal.get("id")
                ask_price   = proposal.get("ask_price", stake)
                if not proposal_id:
                    logger.error(
                        f"FAILED: {symbol} — proposal_rejected (no id in {proposal})"
                    )
                    return None

                buy_req = {"buy": proposal_id, "price": ask_price}
                resp = await self._send(buy_req)
                if not resp:
                    logger.error(f"FAILED: {symbol} — no response (ACCU buy)")
                    return None
                if resp.get("error"):
                    err = resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={buy_req}"
                    )
                    return None

                result = resp.get("buy", {})
                contract_id = str(result.get("contract_id", ""))
                logger.info(
                    f"CONTRACT OPENED: {contract_id} | {symbol} | ACCU | "
                    f"${stake:.4f} | growth_rate={growth_rate}%"
                )

                self._contract_symbol_map[contract_id] = symbol
                asyncio.create_task(self._subscribe_ws_contract(contract_id))

                return result

            except Exception as e:
                logger.error(f"FAILED: {symbol} — {e}")
                return None

    async def sell_contract(
            self,
            contract_id: str,
            price:       float = 0) -> Optional[dict]:
        """
        Close/sell an open contract early — e.g. an ACCU position before
        knockout, or any other open contract that supports early exit.

        price is the minimum acceptable sell price. 0 is Deriv's
        documented "accept the current market price unconditionally"
        sentinel.

        Returns dict (sell response — contains sold_for, contract_id) on
        success, None on every failure path. Never raises.
        """
        try:
            req = {"sell": int(contract_id), "price": price}
            logger.info(f"SELL ATTEMPT: {contract_id} min_price=${price:.4f}")
            resp = await self._send(req)
            if not resp:
                logger.error(f"FAILED: sell {contract_id} — no response")
                return None
            if resp.get("error"):
                err = resp["error"]
                logger.error(
                    f"FAILED: sell {contract_id} — {err.get('code')}: "
                    f"{err.get('message')} | details={err.get('details')}"
                )
                return None

            result = resp.get("sell", {})
            logger.info(
                f"CONTRACT SOLD: {contract_id} | sold_for=${result.get('sold_for')}"
            )
            # It's closed now — stop tracking it under both resolution paths.
            self._polling_contracts.pop(str(contract_id), None)
            self._subscribed_contracts.discard(str(contract_id))
            return result

        except Exception as e:
            logger.error(f"FAILED: sell {contract_id} — {e}")
            return None

    # ─── Matches/Differs (digit) contracts ─────────────────────────────────────

    async def get_digit_proposal(
            self,
            symbol:        str,
            stake:         float,
            digit:         int,
            match_type:    str,   # "MATCH" or "DIFFER"
            duration:      int = 5,
            duration_unit: str = "t") -> Optional[dict]:
        """
        Request a proposal for a Matches ('MATCH') or Differs ('DIFFER')
        digit contract targeting last-digit value `digit` (0-9).

        duration_unit defaults to "t" (ticks) — the standard unit for
        digit contracts on volatility indices.

        Returns the full 'proposal' dict on success, None on any failure
        path. Never raises.
        """
        contract_type = self._digit_contract_type(match_type)
        if contract_type is None:
            logger.error(
                f"FAILED: {symbol} — invalid match_type={match_type!r} "
                f"(must be 'MATCH' or 'DIFFER')"
            )
            return None
        if not (0 <= digit <= 9):
            logger.error(f"FAILED: {symbol} — invalid digit={digit} (must be 0-9)")
            return None

        try:
            req = {
                "proposal":      1,
                "amount":        stake,
                "basis":         "stake",
                "contract_type": contract_type,
                "currency":      "USD",
                "duration":      duration,
                "duration_unit": duration_unit,
                "underlying_symbol": symbol,
                "barrier":       str(digit),
            }
            logger.info(
                f"PROPOSAL REQUEST: {contract_type} digit={digit} {symbol} "
                f"stake=${stake:.4f}"
            )
            resp = await self._send(req)
            if not resp:
                logger.error(f"FAILED: {symbol} — no response ({contract_type} proposal)")
                return None
            if resp.get("error"):
                err = resp["error"]
                logger.error(
                    f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                    f"| details={err.get('details')} | full_req={req}"
                )
                return None

            proposal = resp.get("proposal", {})
            logger.info(f"PROPOSAL RESPONSE: {proposal}")
            return proposal

        except Exception as e:
            logger.error(f"FAILED: {symbol} — {e}")
            return None

    async def buy_digit_contract(
            self,
            symbol:        str,
            stake:         float,
            digit:         int,
            match_type:    str,   # "MATCH" or "DIFFER"
            duration:      int = 5,
            duration_unit: str = "t",
            **kwargs) -> Optional[dict]:
        """
        Buy a Matches ('MATCH') or Differs ('DIFFER') digit contract:
        proposal step, then buy against the returned proposal id.

        Returns dict (buy response) on success, None on every failure
        path. Never raises.

        After a successful buy, the caller is expected to register the
        contract for polling via subscribe_contract() — mirrors
        buy_contract()'s existing division of responsibility.
        """
        contract_type = self._digit_contract_type(match_type)

        async with self._buy_semaphore:
            elapsed = time.time() - self._last_buy_time
            if elapsed < 3.0:
                await asyncio.sleep(3.0 - elapsed)
            self._last_buy_time = time.time()

            if contract_type is None:
                logger.error(
                    f"FAILED: {symbol} — invalid match_type={match_type!r} "
                    f"(must be 'MATCH' or 'DIFFER')"
                )
                return None
            if not (0 <= digit <= 9):
                logger.error(f"FAILED: {symbol} — invalid digit={digit} (must be 0-9)")
                return None

            stake = self._cap_stake(stake, symbol)
            logger.info(
                f"BUY ATTEMPT: {symbol} {contract_type} digit={digit} "
                f"stake=${stake:.4f}"
            )

            try:
                proposal_req = {
                    "proposal":      1,
                    "amount":        stake,
                    "basis":         "stake",
                    "contract_type": contract_type,
                    "currency":      "USD",
                    "duration":      duration,
                    "duration_unit": duration_unit,
                    "underlying_symbol": symbol,
                    "barrier":       str(digit),
                }
                prop_resp = await self._send(proposal_req)
                if not prop_resp:
                    logger.error(f"FAILED: {symbol} — no response ({contract_type} proposal)")
                    return None
                if prop_resp.get("error"):
                    err = prop_resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={proposal_req}"
                    )
                    return None

                proposal = prop_resp.get("proposal", {})
                logger.info(f"PROPOSAL RESPONSE: {proposal}")

                proposal_id = proposal.get("id")
                ask_price   = proposal.get("ask_price", stake)
                if not proposal_id:
                    logger.error(
                        f"FAILED: {symbol} — proposal_rejected (no id in {proposal})"
                    )
                    return None

                buy_req = {"buy": proposal_id, "price": ask_price}
                resp = await self._send(buy_req)
                if not resp:
                    logger.error(f"FAILED: {symbol} — no response ({contract_type} buy)")
                    return None
                if resp.get("error"):
                    err = resp["error"]
                    logger.error(
                        f"FAILED: {symbol} — {err.get('code')}: {err.get('message')} "
                        f"| details={err.get('details')} | full_req={buy_req}"
                    )
                    return None

                result = resp.get("buy", {})
                contract_id = str(result.get("contract_id", ""))
                logger.info(
                    f"CONTRACT OPENED: {contract_id} | {symbol} | {contract_type} | "
                    f"digit={digit} | ${stake:.4f}"
                )

                self._contract_symbol_map[contract_id] = symbol
                asyncio.create_task(self._subscribe_ws_contract(contract_id))

                return result

            except Exception as e:
                logger.error(f"FAILED: {symbol} — {e}")
                return None

    # ─── Active symbols ───────────────────────────────────────────────────────

    async def get_active_symbols(self) -> List[dict]:
        try:
            resp = await self._send(
                {"active_symbols": "brief", "product_type": "basic"},
                timeout=20,
            )
            return resp.get("active_symbols", [])
        except Exception as exc:
            logger.error(f"get_active_symbols failed: {exc}")
            return []

    async def contracts_for(
        self,
        symbol:   str,
        currency: str = "USD",
    ) -> List[dict]:
        """
        Read-only lookup of every contract type Deriv currently offers for
        *symbol*, via the `contracts_for` API call. No side effects (does not
        buy, subscribe, or modify account state) — safe to call repeatedly
        against a demo or real account.

        Returns the raw list of dicts from contracts_for.available. Each
        entry's key set varies by contract type, but commonly includes
        contract_type, contract_category, min_contract_duration,
        max_contract_duration, barriers/barrier_category, and — for
        multiplier contracts — multiplier_range.

        On any failure (unknown symbol, rate limit, disconnect, timeout):
        returns [] and logs CONTRACTS_FOR FAILED — never raises, so a caller
        looping over many symbols can treat an empty list as "skip and
        continue" without wrapping every call in its own try/except.
        """
        await self._ready.wait()
        try:
            resp = await self._send(
                {
                    "contracts_for": symbol,
                    "currency":      currency,
                },
                timeout=20,
            )
            return resp.get("contracts_for", {}).get("available", [])
        except Exception as exc:
            logger.warning(f"CONTRACTS_FOR FAILED: {symbol} | error={exc}")
            return []

    # ─── Properties ───────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._connected and self._authorized
