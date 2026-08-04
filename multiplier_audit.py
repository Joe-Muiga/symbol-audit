"""
multiplier_audit.py — focused Multiplier-eligibility audit against the
live Deriv API.

symbol_audit.py already gives a ground-truth contract_types table for
every symbol, but answering "which of my Rise/Fall symbols could also
trade as Multipliers?" means eyeballing that table by hand and cross-
referencing it against config.RISE_FALL_SYMBOLS yourself. This script
does that cross-reference for you.

Reuses symbol_audit.py's audit universe and per-symbol summarization
directly (imports _audit_symbol_list() and _summarize(), same
AUTH_TIMEOUT_SECS/REQUEST_DELAY_SECS) so results stay consistent with
the general audit — same symbols (symbols.SYNTHETIC + ALWAYS_AVAILABLE),
same parsing of contract_type / multiplier_range. This just re-shapes
the report around one specific question and adds two things
symbol_audit.py's table doesn't show directly:

  - a "Rise/Fall?" column, cross-referenced against config.RISE_FALL_SYMBOLS
  - a final summary that splits every RISE_FALL_SYMBOLS entry into three
    buckets: confirmed Multiplier-eligible (with range), confirmed NOT
    eligible, and not-yet-audited/errored (still genuinely unknown)

Read-only — same guarantee as symbol_audit.py: contracts_for() per
symbol is a single read-only API request, no buys/subscriptions/orders.

USAGE
-----
Run locally with the same environment variables the bot uses in
production (DERIV_API_TOKEN, DERIV_APP_ID, and optionally DERIV_WS_URL):

    DERIV_API_TOKEN=xxx DERIV_APP_ID=xxxxx python3 multiplier_audit.py

Or as a Render one-off job in the same service/environment as the bot,
exactly like symbol_audit.py — it exits on its own once the report
prints, it does not loop or stay resident.
"""

import asyncio
import logging
import sys

import config
from deriv_client import DerivClient
from symbol_audit import (
    _audit_symbol_list,
    _summarize,
    AUTH_TIMEOUT_SECS,
    REQUEST_DELAY_SECS,
)

logging.basicConfig(
    level=logging.WARNING,  # keep connection chatter out of the way of the report
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("multiplier_audit")


def _has_multiplier(result: dict) -> bool:
    return "MULTUP" in result["contract_types"] or "MULTDOWN" in result["contract_types"]


def _fmt_range(result: dict) -> str:
    if not result["multiplier_range"]:
        return "-"
    lo, hi = result["multiplier_range"]
    return f"x{lo:g}-x{hi:g}"


def _fmt_types(result: dict, max_len: int = 46) -> str:
    types_str = ", ".join(result["contract_types"]) or "(none returned)"
    if len(types_str) > max_len:
        types_str = types_str[: max_len - 1] + "…"
    return types_str


async def _gather_results() -> list:
    """
    Same connect -> authorize -> loop-over-symbols shape as
    symbol_audit.py's run_audit(), just returning the results list
    instead of printing a table directly, since this script needs to
    reshape the data two different ways (full table + Rise/Fall summary).
    """
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
        return []

    symbol_list = _audit_symbol_list()
    print(
        f"Authorized ✓ — auditing {len(symbol_list)} symbols for Multiplier "
        f"support against {config.DERIV_WS_URL}\n",
        file=sys.stderr,
    )

    results = []
    for symbol in symbol_list:
        try:
            available = await client.contracts_for(symbol)
            results.append(_summarize(symbol, available))
        except Exception as exc:
            logger.warning(f"AUDIT SKIP: {symbol} | error={exc}")
            results.append({
                "symbol": symbol,
                "contract_types": [],
                "multiplier_range": None,
                "barrier_notes": [],
                "error": str(exc),
            })
        await asyncio.sleep(REQUEST_DELAY_SECS)

    connect_task.cancel()
    try:
        await connect_task
    except (asyncio.CancelledError, Exception):
        pass

    return results


def _print_full_table(results: list, rise_fall: list, already_multiplier: set):
    by_symbol = {r["symbol"]: r for r in results}
    # Rise/Fall symbols first (in config's own order), then everything else
    # that got audited but isn't currently on Rise/Fall.
    ordered = [s for s in rise_fall if s in by_symbol]
    ordered += [r["symbol"] for r in results if r["symbol"] not in rise_fall]

    col_sym = max(len("Symbol"), max((len(s) for s in ordered), default=0))
    header = (
        f"{'Symbol':<{col_sym}}  {'Rise/Fall?':<10}  {'Multiplier?':<11}  "
        f"{'Range':<12}  {'Already in':<12}  Contract types"
    )
    print(header)
    print("-" * len(header))

    for symbol in ordered:
        r = by_symbol[symbol]
        in_rf = "yes" if symbol in rise_fall else "no"
        in_mult = "yes" if symbol in already_multiplier else "no"

        if r["error"]:
            print(
                f"{symbol:<{col_sym}}  {in_rf:<10}  {'ERROR':<11}  "
                f"{'-':<12}  {in_mult:<12}  {r['error']}"
            )
            continue

        has_mult = "yes" if _has_multiplier(r) else "no"
        print(
            f"{symbol:<{col_sym}}  {in_rf:<10}  {has_mult:<11}  "
            f"{_fmt_range(r):<12}  {in_mult:<12}  {_fmt_types(r)}"
        )


def _print_rise_fall_summary(results: list, rise_fall: list, already_multiplier: set):
    by_symbol = {r["symbol"]: r for r in results}

    print("\n" + "=" * 70)
    print("RISE/FALL SYMBOLS — MULTIPLIER ELIGIBILITY")
    print("=" * 70)

    eligible, not_eligible, not_audited = [], [], []
    for symbol in rise_fall:
        r = by_symbol.get(symbol)
        if r is None:
            # Not in symbols.SYNTHETIC / ALWAYS_AVAILABLE at all — the
            # audit never queried it, same situation DSHIFT10/20/30 were
            # in before. Distinct from a confirmed "no".
            not_audited.append(f"{symbol} (not in audited symbol list)")
        elif r["error"]:
            not_audited.append(f"{symbol} (error: {r['error']})")
        elif _has_multiplier(r):
            eligible.append((symbol, _fmt_range(r)))
        else:
            not_eligible.append(symbol)

    print(f"\n✅ Support Multipliers ({len(eligible)}):")
    if eligible:
        for symbol, rng in eligible:
            flag = " [already in MULTIPLIER_SYMBOLS]" if symbol in already_multiplier else " [not yet added]"
            print(f"   {symbol:<10} {rng:<14}{flag}")
    else:
        print("   (none)")

    print(f"\n❌ Confirmed NO Multiplier support ({len(not_eligible)}):")
    if not_eligible:
        for symbol in not_eligible:
            print(f"   {symbol}")
    else:
        print("   (none)")

    print(f"\n❓ Still unknown — not audited or errored ({len(not_audited)}):")
    if not_audited:
        for entry in not_audited:
            print(f"   {entry}")
    else:
        print("   (none)")

    print()


async def run_audit():
    results = await _gather_results()
    if not results:
        return 1

    rise_fall = list(getattr(config, "RISE_FALL_SYMBOLS", []))
    already_multiplier = set(getattr(config, "MULTIPLIER_SYMBOLS", []))

    print()
    _print_full_table(results, rise_fall, already_multiplier)
    _print_rise_fall_summary(results, rise_fall, already_multiplier)

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(run_audit())
    sys.exit(exit_code)
