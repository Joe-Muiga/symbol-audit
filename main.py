"""
main.py – Entry point for the SIFM Deriv Trading Bot on Render.

Architecture:
  • Thread 1 (main) : Flask web server for Render's health checks
  • Thread 2        : asyncio event loop running the bot engine
  • Thread 3        : self-ping keep-alive (pings /health every 40 s)
  • Thread 4        : restart scheduler (triggers a fresh deploy every 15 min)

Environment variables required:
  DERIV_API_TOKEN        – your Deriv API token (trade + read scope)
  DERIV_APP_ID           – your Deriv app ID  (default 1089 = demo)
  RENDER_EXTERNAL_URL    – set automatically by Render  (e.g. https://yourapp.onrender.com)
  PORT                   – set automatically by Render   (default 8080)
  RENDER_DEPLOY_HOOK_URL – deploy hook URL from Render dashboard (optional; enables auto-redeploy)
"""

import asyncio
import logging
import threading
import sys
import config

# ─── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level   = getattr(logging, config.LOG_LEVEL, logging.INFO),
    format  = "%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    stream  = sys.stdout,
)
logger = logging.getLogger("main")

# ─── Sanity check ─────────────────────────────────────────────────────────────
if not config.DERIV_API_TOKEN:
    logger.critical(
        "DERIV_API_TOKEN is not set!\n"
        "Add it as an environment variable in your Render service settings.\n"
        "Get your token at: https://app.deriv.com/account/api-token"
    )
    # Don't exit – let the web server start so Render won't kill the service.

# ─── Bot thread ───────────────────────────────────────────────────────────────
def _run_bot():
    from bot_engine import BotEngine
    bot = BotEngine()
    try:
        asyncio.run(bot.run())
    except Exception as exc:
        logger.error(f"Bot crashed: {exc}", exc_info=True)

# ─── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from keep_alive import app, start_keep_alive
    from restart_scheduler import start_restart_scheduler

    logger.info("Starting SIFM Deriv Trading Bot …")
    logger.info(f"  Deriv App ID : {config.DERIV_APP_ID}")
    logger.info(f"  API token    : {'SET ✓' if config.DERIV_API_TOKEN else 'MISSING ✗'}")
    logger.info(f"  Port         : {config.PORT}")
    logger.info(f"  Self-URL     : {config.SELF_URL}")

    # 1. Start keep-alive pinger
    start_keep_alive()

    # 2. Start auto-redeploy scheduler (no-op if RENDER_DEPLOY_HOOK_URL is unset)
    start_restart_scheduler()

    # 3. Start bot in background thread
    bot_thread = threading.Thread(target=_run_bot, name="bot-engine", daemon=True)
    bot_thread.start()
    logger.info("Bot engine thread started ✓")

    # 4. Flask in main thread (Render requires a bound web server)
    logger.info(f"Starting Flask on 0.0.0.0:{config.PORT}")
    app.run(host="0.0.0.0", port=config.PORT, use_reloader=False, threaded=True)
