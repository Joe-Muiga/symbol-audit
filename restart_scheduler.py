"""
restart_scheduler.py – Graceful auto-redeploy on Render.

v1 → v2 changes (BUG 4):

  NEW — trigger_redeploy():
    A standalone function called by bot_engine when _cycle_count reaches
    config.REDEPLOY_EVERY_N_CYCLES.  POSTs to RENDER_DEPLOY_HOOK_URL from
    the environment.  Wrapped in try/except — logs success or failure, never
    raises, never crashes the caller.

  UNCHANGED — _scheduler_loop() / start_restart_scheduler():
    Time-based random-interval redeploy (50–90 min) preserved as-is.
    The two mechanisms are independent: cycle-count redeploy (BUG 4) is
    orchestrated from bot_engine; the scheduler provides a time-based fallback.

Cycle (repeats forever):
  1. Sleep a random interval between MIN_INTERVAL and MAX_INTERVAL.
  2. Signal the bot to pause new trade entries (sets redeploy_pending = True).
  3. Wait up to DRAIN_TIMEOUT seconds for all active trades to close.
  4. POST to RENDER_DEPLOY_HOOK_URL → Render builds a fresh instance.
  5. The new instance starts with redeploy_pending = False (clean slate).

bot_engine.py integration required (two calls):
  • Before opening any new trade:
        from keep_alive import is_redeploy_pending
        if is_redeploy_pending():
            return  # skip – redeploy is imminent

  • Whenever your open-position count changes (open OR close a trade):
        from keep_alive import set_active_trades
        set_active_trades(len(self.open_positions))  # or equivalent
"""

import logging
import os
import random
import threading
import time

import requests
import config
from keep_alive import set_redeploy_pending, get_active_trades

logger = logging.getLogger(__name__)

# ── Tuneable constants ────────────────────────────────────────────────────────
MIN_INTERVAL  = 50 * 60   # 50 min in seconds
MAX_INTERVAL  = 90 * 60   # 90 min in seconds
DRAIN_TIMEOUT = 10 * 60   # wait at most 10 min for open trades to close
DRAIN_POLL    = 5         # poll active-trade count every 5 seconds
# ─────────────────────────────────────────────────────────────────────────────


# ─── BUG 4: on-demand redeploy called by bot_engine ──────────────────────────

def trigger_redeploy() -> None:
    """
    POST to the Render deploy hook URL to trigger an immediate redeploy.

    Called by bot_engine._main_loop() when _cycle_count reaches
    config.REDEPLOY_EVERY_N_CYCLES.  This function:
      • Reads RENDER_DEPLOY_HOOK_URL from the environment (primary) or
        config (fallback).
      • POSTs with a 15-second timeout.
      • Logs success or failure.
      • NEVER raises — all exceptions are caught so bot_engine is never
        crashed by a network hiccup or missing env var.
    """
    try:
        url = (os.environ.get("RENDER_DEPLOY_HOOK_URL", "")
               or getattr(config, "RENDER_DEPLOY_HOOK_URL", ""))

        if not url:
            logger.warning(
                "trigger_redeploy: RENDER_DEPLOY_HOOK_URL is not set — "
                "skipping redeploy.  Set the env var in Render → Environment.")
            return

        logger.info("trigger_redeploy: POSTing to Render deploy hook …")
        r = requests.post(url, timeout=15)

        if r.ok:
            logger.info(
                f"trigger_redeploy: deploy triggered ✓ (HTTP {r.status_code}). "
                "Render is building a fresh instance.")
        else:
            logger.error(
                f"trigger_redeploy: deploy hook returned "
                f"HTTP {r.status_code}: {r.text[:200]}")

    except Exception as exc:
        logger.error(
            f"trigger_redeploy: POST failed — {exc}. "
            "Bot will continue running; redeploy was not triggered.")


# ─── Time-based scheduler loop ────────────────────────────────────────────────

def _scheduler_loop() -> None:
    # BUG 3 FIX: wrap config attribute access in try/except so a missing or
    # mis-typed config value never crashes the background scheduler thread.
    try:
        url = config.RENDER_DEPLOY_HOOK_URL
    except AttributeError:
        logger.warning(
            "restart_scheduler: config.RENDER_DEPLOY_HOOK_URL is not defined – "
            "auto-redeploy scheduler is disabled. "
            "Add RENDER_DEPLOY_HOOK_URL to config.py or as a Render env var."
        )
        return

    if not url:
        logger.warning(
            "RENDER_DEPLOY_HOOK_URL is not set – "
            "auto-redeploy scheduler is disabled. "
            "Add the env var in Render → Environment to enable it."
        )
        return

    logger.info(
        f"Restart scheduler started "
        f"(random interval {MIN_INTERVAL // 60}–{MAX_INTERVAL // 60} min, "
        f"drain timeout {DRAIN_TIMEOUT // 60} min)."
    )

    while True:
        # ── 1. Random sleep ──────────────────────────────────────────────────
        interval = random.randint(MIN_INTERVAL, MAX_INTERVAL)
        logger.info(
            f"Restart scheduler: next redeploy in "
            f"{interval // 60}m {interval % 60}s."
        )
        time.sleep(interval)

        # ── 2. Pause new trade entries ────────────────────────────────────────
        logger.info(
            "Restart scheduler: signalling bot to pause new trade entries …"
        )
        set_redeploy_pending(True)

        # ── 3. Drain open trades ──────────────────────────────────────────────
        deadline = time.time() + DRAIN_TIMEOUT
        while time.time() < deadline:
            active = get_active_trades()
            if active == 0:
                logger.info(
                    "Restart scheduler: all trades closed – proceeding to deploy."
                )
                break
            logger.info(
                f"Restart scheduler: {active} open trade(s) remaining, "
                f"waiting …"
            )
            time.sleep(DRAIN_POLL)
        else:
            logger.warning(
                f"Restart scheduler: drain timeout – "
                f"{get_active_trades()} trade(s) still open. Deploying anyway."
            )

        # ── 4. Trigger Render redeploy ────────────────────────────────────────
        logger.info("Restart scheduler: POSTing to Render deploy hook …")
        try:
            # Re-read URL defensively in case config was patched at runtime
            try:
                url = config.RENDER_DEPLOY_HOOK_URL
            except AttributeError:
                logger.error(
                    "Restart scheduler: config.RENDER_DEPLOY_HOOK_URL disappeared – "
                    "clearing pause flag and skipping this redeploy cycle."
                )
                set_redeploy_pending(False)
                continue

            if not url:
                logger.warning(
                    "Restart scheduler: RENDER_DEPLOY_HOOK_URL is empty – "
                    "skipping redeploy, clearing pause flag."
                )
                set_redeploy_pending(False)
                continue

            r = requests.post(url, timeout=15)
            if r.ok:
                logger.info(
                    f"Restart scheduler: deploy triggered ✓ (HTTP {r.status_code}). "
                    "Render is building a fresh instance."
                )
                # Leave redeploy_pending = True – the dying process won't open
                # any more trades and the new process starts clean (False).
            else:
                logger.error(
                    f"Restart scheduler: deploy hook returned "
                    f"HTTP {r.status_code}: {r.text[:200]} – "
                    "clearing pause flag; will retry next cycle."
                )
                set_redeploy_pending(False)
        except requests.RequestException as exc:
            logger.error(
                f"Restart scheduler: POST failed – {exc}. "
                "Clearing pause flag; will retry next cycle."
            )
            set_redeploy_pending(False)


def start_restart_scheduler() -> None:
    """Spawn the scheduler as a background daemon thread."""
    t = threading.Thread(
        target=_scheduler_loop,
        name="restart-scheduler",
        daemon=True,
    )
    t.start()
    logger.info("Restart scheduler thread started ✓")
