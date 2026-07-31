"""
symbol_audit.py — ground-truth contract audit against the live Deriv API.

Queries `contracts_for` for every symbol in symbols.SYNTHETIC plus
symbols.ALWAYS_AVAILABLE, and prints which contract types (CALL/PUT,
MULTUP/MULTDOWN, ACCU, DIGIT*, etc) each one actually supports on this
account right now — settling, empirically, questions like:

  - Which of BOOM300N/BOOM500/BOOM1000/CRASH300N/CRASH500/CRASH1000
    support MULTUP/MULTDOWN, and their multiplier ranges
  - Which of JD10/JD25/JD50/JD75/JD100 support Multipliers
  - Which of DSHIFT10/DSHIFT20/DSHIFT30 support Multipliers
  - What RDBEAR/RDBULL actually support via this API (settling the
    Range Break vs Bear/Bull labeling disagreement between config.py
    and symbols.py — whatever contracts_for returns for these two
    symbols is the answer, not either file's comment)

Read-only. Uses DerivClient.connect() to authenticate exactly the way the
bot does (standard {"authorize": token} over the WS connection — see the
deriv_client.py fix), then calls contracts_for() per symbol, which issues
a single read-only API request and has no side effects: no buys, no
subscriptions, no order placement.

USAGE
-----
Run locally with the same environment variables the bot uses in
production (DERIV_API_TOKEN, DERIV_APP_ID, and optionally DERIV_WS_URL —
config.py already defaults DERIV_WS_URL from DERIV_APP_ID if unset):

    DERIV_API_TOKEN=xxx DERIV_APP_ID=xxxxx python3 symbol_audit.py

Or as a Render one-off job in the same service/environment as the bot
(Render dashboard -> service -> "Run job" / one-off job with this command)
so it picks up the existing env vars without needing to duplicate them —
that's simpler than copying the token to a local shell. Either way it
exits on its own once the audit table has printed; it does not loop or
stay resident like the bot does.
"""

import asyncio
import logging
import sys

import config
import symbols
from deriv_client import DerivClient

logging.basicConfig(
    level=logging.WARNING,  # keep connection chatter out of the way of the table
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("symbol_audit")

# Gap between contracts_for calls so ~25+ symbols in a row don't trip
# Deriv's rate limiting.
REQUEST_DELAY_SECS = 0.5

# How long to wait for the client to finish authorizing before giving up.
AUTH_TIMEOUT_SECS = 20


def _audit_symbol_list() -> list:
    """
    symbols.SYNTHETIC already contains RDBEAR/RDBULL plus every volatility,
    step, Boom/Crash, Jump, and Drift Switch code. ALWAYS_AVAILABLE is
    currently an alias for SYNTHETIC, so this dedupes while preserving
    order in case that ever changes.
    """
    combined = list(symbols.SYNTHETIC) + list(symbols.ALWAYS_AVAILABLE)
    return list(dict.fromkeys(combined))


def _summarize(symbol: str, available: list) -> dict:
    """
    Reduce one contracts_for.available list down to what the table needs:
    distinct contract types, and — for any multiplier entries — the
    min/max multiplier allowed. Barrier info is included per-entry only
    when the API actually returns it, since not every contract type has
    barriers.
    """
    if not available:
        return {
            "symbol": symbol,
            "contract_types": [],
            "multiplier_range": None,
            "barrier_notes": [],
            "error": None,
        }

    contract_types = sorted({
        c.get("contract_type", "?") for c in available if c.get("contract_type")
    })

    # Multiplier range: scan every MULTUP/MULTDOWN-style entry and take the
    # widest min/max seen, since Deriv sometimes reports slightly different
    # ranges per contract_type within the same symbol.
    mult_lo, mult_hi = None, None
    for c in available:
        rng = c.get("multiplier_range")
        if not rng:
            continue
        try:
            nums = [float(x) for x in rng]
        except (TypeError, ValueError):
            continue
        if not nums:
            continue
        lo, hi = min(nums), max(nums)
        mult_lo = lo if mult_lo is None else min(mult_lo, lo)
        mult_hi = hi if mult_hi is None else max(mult_hi, hi)

    barrier_notes = []
    for c in available:
        if c.get("barrier_category") or c.get("barriers"):
            note = f"{c.get('contract_type', '?')}: barrier_category={c.get('barrier_category', '?')}"
            if note not in barrier_notes:
                barrier_notes.append(note)

    return {
        "symbol": symbol,
        "contract_types": contract_types,
        "multiplier_range": (mult_lo, mult_hi) if mult_lo is not None else None,
        "barrier_notes": barrier_notes,
        "error": None,
    }


def _print_table(results: list):
    col_symbol = max(len("Symbol"), max((len(r["symbol"]) for r in results), default=0))
    col_types  = max(len("Contract Types"), max(
        (len(", ".join(r["contract_types"])) for r in results if not r["error"]),
        default=0,
    ))
    col_types  = min(col_types, 60)  # keep the table pasteable, wrap long ones below

    header = f"{'Symbol':<{col_symbol}}  {'Contract Types':<{col_types}}  Notes"
    print(header)
    print("-" * len(header))

    for r in results:
        if r["error"]:
            print(f"{r['symbol']:<{col_symbol}}  {'—':<{col_types}}  ERROR: {r['error']}")
            continue

        types_str = ", ".join(r["contract_types"]) or "(none returned)"
        notes = []
        if r["multiplier_range"]:
            lo, hi = r["multiplier_range"]
            notes.append(f"multiplier x{lo:g}-x{hi:g}")
        notes.extend(r["barrier_notes"])
        notes_str = "; ".join(notes)

        if len(types_str) <= col_types:
            print(f"{r['symbol']:<{col_symbol}}  {types_str:<{col_types}}  {notes_str}")
        else:
            # Wrap: print symbol + notes on the first line, full type list
            # indented below it, so nothing gets silently truncated.
            print(f"{r['symbol']:<{col_symbol}}  {'(see below)':<{col_types}}  {notes_str}")
            print(f"{'':<{col_symbol}}    -> {types_str}")


async def run_audit():
    client = DerivClient()

    connect_task = asyncio.create_task(client.connect())
    try:
        await asyncio.wait_for(client._ready.wait(), timeout=AUTH_TIMEOUT_SECS)
    except asyncio.TimeoutError:
        logger.error(
            f"Timed out after {AUTH_TIMEOUT_SECS}s waiting for auth. "
            "Check DERIV_API_TOKEN / DERIV_APP_ID / DERIV_WS_URL."
        )
        connect_task.cancel()
        return 1

    print(f"Authorized ✓ — auditing {len(_audit_symbol_list())} symbols "
          f"against {config.DERIV_WS_URL}\n", file=sys.stderr)

    results = []
    for symbol in _audit_symbol_list():
        try:
            available = await client.contracts_for(symbol)
            results.append(_summarize(symbol, available))
        except Exception as exc:
            # contracts_for() already never raises internally, but this
            # belt-and-suspenders catch keeps one unexpected failure from
            # killing the rest of a 25+ symbol run.
            logger.warning(f"AUDIT SKIP: {symbol} | error={exc}")
            results.append({
                "symbol": symbol,
                "contract_types": [],
                "multiplier_range": None,
                "barrier_notes": [],
                "error": str(exc),
            })
        await asyncio.sleep(REQUEST_DELAY_SECS)

    print()
    _print_table(results)

    connect_task.cancel()
    try:
        await connect_task
    except (asyncio.CancelledError, Exception):
        pass

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(run_audit())
    sys.exit(exit_code)
