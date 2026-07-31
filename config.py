import os

# ── GENERAL ───────────────────────────────────────────────────
LOG_LEVEL = "INFO"
DEBUG     = False
VERSION   = "1.1.0"

# ── DERIV API ─────────────────────────────────────────────────
DERIV_API_TOKEN = os.environ.get("DERIV_API_TOKEN", "")
DERIV_APP_ID    = os.environ.get("DERIV_APP_ID", "1089")

# ── SERVER ────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", 10000))
SELF_URL = os.environ.get("SELF_URL", os.environ.get("RENDER_EXTERNAL_URL", ""))
KEEP_ALIVE_INTERVAL = 600  # seconds between self-ping requests

# ── ALL DERIV SYNTHETIC INDICES ──────────────────────────────

# Standard Volatility (2s tick)
VOLATILITY_STANDARD = [
    "R_10","R_25","R_50","R_75","R_100",
]

# 1-Second Volatility (faster tick)
VOLATILITY_1S = [
    "1HZ10V","1HZ25V","1HZ50V",
    "1HZ75V","1HZ100V","1HZ150V",
    "1HZ200V","1HZ250V",
]

# Boom & Crash
# NOTE: Removed from trading — Boom/Crash symbols do NOT support
# CALL/PUT Rise/Fall contracts via the API (OfferingsValidationError).
# They require different contract types (e.g. multipliers) not used here.
BOOM_CRASH = []

# Step Index
STEP = ["stpRNG"]

# Jump Indices
# NOTE: Removed from trading — Jump indices do NOT support
# CALL/PUT Rise/Fall contracts via the API (OfferingsValidationError).
JUMP = []

# Range Break
# REMOVED FROM ACTIVE TRADING (this pass) — confirmed MT5-only / not reachable
# via this bot's contract API. Kept here as a disabled reference list only;
# it is intentionally NOT folded into RISE_FALL_SYMBOLS, RANGE_BREAK_SYMBOLS,
# ALL_SYMBOLS, or PRIORITY_SYMBOLS below. Do not re-add without re-verifying
# against a fresh contracts_for audit.
RANGE_BREAK = ["RDBULL", "RDBEAR"]          # disabled — MT5-only, not in trade queue
RANGE_BREAK_ENABLED = False

# Drift Switch
DRIFT = ["DSHIFT10","DSHIFT20","DSHIFT30"]

# ── BEAR/BULL MARKET SYMBOLS — PENDING AUDIT CONFIRMATION ────
# NOT ADDED YET. Your message referenced pasting symbol_audit.py output but
# the actual output/findings were never included (placeholder text was left
# in the prompt). Per your own instruction #2, these are only to be added if
# the audit confirms they are currently active on your account.
#
# Once you have real output, fill this in and route it into RISE_FALL_SYMBOLS
# / MEAN_REVERSION_SYMBOLS (or a new strategy list) as appropriate:
#
# BEAR_BULL_SYMBOLS = ["<symbol_from_audit>", ...]
# BEAR_BULL_TREND_SHIFT_MINS = 20   # configurable: 10 / 20 / 30
BEAR_BULL_SYMBOLS = []              # left empty — unconfirmed
BEAR_BULL_TREND_SHIFT_MINS = 20     # default if/when enabled (10, 20, or 30)

# Only these symbols support CALL/PUT Rise/Fall via Deriv API
# (RDBULL/RDBEAR removed — see RANGE_BREAK note above)
# NOTE: 1HZ150V / 1HZ200V / 1HZ250V added — same Rise/Fall-compatible
# 1-second volatility family as 1HZ10V-1HZ100V above, and both already
# had MULTIPLIER_MAP and STOP_LOSS_MAP entries defined further down in
# this file with nothing routing symbols to them. Confirm with a fresh
# contracts_for / symbol_audit.py pass before relying on this in
# production — treat as "should work by the same pattern as its
# siblings", not yet empirically re-verified against your account.
RISE_FALL_SYMBOLS = [
    "R_10","R_25","R_50","R_75","R_100",
    "1HZ10V","1HZ25V","1HZ50V","1HZ75V","1HZ100V",
    "1HZ150V","1HZ200V","1HZ250V",
    "stpRNG",
]

# Digit (Match/Differ/Over/Under/Even/Odd) contracts are not used by this bot —
# only CALL/PUT Rise/Fall is traded — so this stays empty. signal_engine.py
# checks `if symbol in config.DIGIT_SYMBOLS` to route digit-specific logic;
# an empty list means that branch is always skipped, as intended.
DIGIT_SYMBOLS = []

# ── STRATEGY ROUTING (signal_engine.py) ──────────────────────
# Every traded symbol is routed to exactly one strategy evaluator.
MEAN_REVERSION_SYMBOLS = VOLATILITY_STANDARD + VOLATILITY_1S  # all 10 vol indices
RANGE_BREAK_SYMBOLS    = []                                      # disabled — see RANGE_BREAK note
BOOM_CRASH_SYMBOLS     = BOOM_CRASH                              # empty — not traded
STEP_SYMBOLS           = STEP                                    # stpRNG

# All symbols combined — Rise/Fall compatible only
ALL_SYMBOLS       = RISE_FALL_SYMBOLS
ALL_TRADE_SYMBOLS = RISE_FALL_SYMBOLS
VOLATILITY_SYMBOLS = RISE_FALL_SYMBOLS  # alias for compatibility with bot_engine.py

# ── MULTIPLIER SETTINGS ──────────────────────────────────────
# Higher volatility = higher multiplier potential
MULTIPLIER_MAP = {
    # Low volatility — moderate multiplier
    "R_10":    100, "1HZ10V":  100,
    "R_25":    200, "1HZ25V":  200,
    # Medium volatility
    "R_50":    300, "1HZ50V":  300,
    "R_75":    500, "1HZ75V":  500,
    # High volatility — maximum multiplier
    "R_100":   500, "1HZ100V": 500,
    "1HZ150V": 500, "1HZ200V": 500,
    "1HZ250V": 500,
    # Boom/Crash — moderate (spike risk)
    "BOOM150": 100, "BOOM300": 100,
    "BOOM500": 100, "BOOM1000":100,
    "CRASH150":100, "CRASH300":100,
    "CRASH500":100, "CRASH1000":100,
    # Others
    "stpRNG":  200,
    "JD10":    100, "JD25":    200,
    "JD50":    300, "JD75":    400,
    "JD100":   500,
    # RDBULL/RDBEAR kept for reference only — not traded (RANGE_BREAK_ENABLED = False)
    "RDBULL":  200, "RDBEAR":  200,
    "DSHIFT10":200, "DSHIFT20":200,
    "DSHIFT30":200,
}
DEFAULT_MULTIPLIER = 100

# ── STOP LOSS AND TAKE PROFIT (% of stake) ──────────────────
# Stop loss as % of stake — caps maximum loss per trade
STOP_LOSS_MAP = {
    # Low vol — tighter SL
    "R_10":    30.0,  "1HZ10V":  30.0,
    "R_25":    40.0,  "1HZ25V":  40.0,
    # Medium vol
    "R_50":    50.0,  "1HZ50V":  50.0,
    "R_75":    60.0,  "1HZ75V":  60.0,
    # High vol — wider SL to avoid noise
    "R_100":   70.0,  "1HZ100V": 70.0,
    "1HZ150V": 75.0,  "1HZ200V": 80.0,
    "1HZ250V": 80.0,
    # Boom/Crash — wide SL due to spikes
    "BOOM150": 50.0,  "BOOM300": 50.0,
    "BOOM500": 50.0,  "BOOM1000":50.0,
    "CRASH150":50.0,  "CRASH300":50.0,
    "CRASH500":50.0,  "CRASH1000":50.0,
}
DEFAULT_STOP_LOSS_PCT = 50.0

# Take profit = 2x stop loss (2:1 RR minimum)
TAKE_PROFIT_RATIO = 2.0

# ── STAKE SETTINGS ───────────────────────────────────────────
BASE_STAKE_PCT       = 0.005   # 0.5% of current balance per trade — this
                                # IS the compounding: stake grows/shrinks
                                # automatically as balance grows/shrinks.
MIN_STAKE            = 0.35    # safety floor — never stake less than this
MAX_STAKE            = 1000.0  # safety backstop only, not the everyday
                                # driver — was previously == MIN_STAKE,
                                # which silently capped every trade at
                                # $0.35 regardless of balance or the 0.5%
                                # calculation above. Adjust if you want a
                                # tighter per-trade ceiling.
DAILY_LOSS_LIMIT_PCT = 0.15   # NOTE: value is 15%, comment below said 20% — see flags in reply
DAILY_LOSS_PAUSE_MINS = 30

# ── AGGRESSIVE COMPOUNDING ───────────────────────────────────
# Disabled per user request — stake no longer scales up on win streaks.
# Multipliers all set to 1.0 so PLS tier lookups (wherever risk_manager.py
# applies them) are a no-op; stake stays flat regardless of streak length.
PLS_WIN_THRESHOLDS  = [3,   5,   8,   12,  15  ]
PLS_WIN_MULTIPLIERS = [1.0, 1.0, 1.0, 1.0, 1.0]
PLS_WIN_EXTRA_SLOTS = [0,   0,   0,   0,   0   ]

# ── CONCURRENT TRADES ────────────────────────────────────────
MAX_CONCURRENT_TRADES = 30

# ── TIMEFRAMES ───────────────────────────────────────────────
HTF_GRANULARITY   = 3600   # 1H
MTF_GRANULARITY   = 300    # 5M
LTF_GRANULARITY   = 60     # 1M
HTF_BARS          = 100
MTF_BARS          = 50
LTF_BARS          = 30

# ── SIGNAL SETTINGS ──────────────────────────────────────────
MIN_SIGNAL_SCORE       = 0.68
MIN_STRATEGY_AGREEMENT = 4

# SMC parameters
OB_LOOKBACK            = 50
FVG_MIN_ATR            = 0.5
SWEEP_LOOKBACK         = 20
SWING_LOOKBACK         = 5
FIB_LEVELS             = [0.382, 0.5, 0.618, 0.786]
FIB_TOLERANCE          = 0.1
EMA_FAST              = 8
EMA_SLOW              = 21
EMA_TREND             = 50
RSI_PERIOD            = 14
RSI_OVERBOUGHT        = 70
RSI_OVERSOLD          = 30
MOMENTUM_LOOKBACK     = 10
ATR_PERIOD            = 14
BREAKOUT_ATR_MULT     = 1.5

# ── CONTRACT SETTINGS ────────────────────────────────────────
# Multiplier contracts — keep short to avoid funding fees
MAX_TRADE_OPEN_MINS   = 30   # force close at 30 min
CHECK_TRADE_MINS      = 20   # check at 20 min
CONTRACT_CHECK_SECS   = 1200
CONTRACT_TIMEOUT_SECS = 1800

# ── SYMBOL SUSPENSION (minutes) ──────────────────────────────
SYMBOL_WIN_SUSPEND_MINS   = 20
SYMBOL_LOSS_SUSPEND_MINS  = 55
SYMBOL_MIN_GAP_MINS       = 1
SYMBOL_SESSION_BAN_LOSSES = 4

# ── RAW TICK BUFFER (feeds tick-based evaluators via evaluate(ticks=...)) ──
TICK_BUFFER_MAXLEN = 200

# ── DEGRADED-SYMBOL TICK-SUBSCRIPTION RETRY ──────────────────
TICK_RESUBSCRIBE_RETRY_SECS = 30

# ── BUY-FAILURE CIRCUIT BREAKER ──────────────────────────────
BUY_FAILURE_CIRCUIT_BREAKER_THRESHOLD    = 5
BUY_FAILURE_CIRCUIT_BREAKER_SUSPEND_MINS = 15

# ── SCANNING ────────────────────────────────────────────────
SCAN_CYCLE_SLEEP       = 1
INIT_BATCH_SIZE        = 8
INIT_BATCH_DELAY       = 0.3
PRIORITY_SYMBOLS = [
    "R_75","R_100","1HZ75V","1HZ100V",
    "1HZ250V","1HZ150V","R_50","R_25",
]

# ── RATE LIMITING ────────────────────────────────────────────
BUY_REQUEST_DELAY_SECS = 3.0
MAX_BUY_PER_SECOND     = 3

# ── RENDER ──────────────────────────────────────────────────
RENDER_DEPLOY_HOOK_URL = os.environ.get(
    "RENDER_DEPLOY_HOOK_URL","")
# REDEPLOY_EVERY_N_CYCLES previously = 8. With SETTLE_WAIT_SECS defaulting to
# 15s (bot_engine._settle_loop), that was an 8*15=120s cycle-based redeploy —
# fighting restart_scheduler.py's timer and redeploying roughly every 2
# minutes instead of every 2 hours. Set high enough that it never fires on
# its own; restart_scheduler.py's fixed 2-hour timer is now the only
# redeploy trigger. Lower this back down only if you deliberately want a
# SECOND, settle-count-based redeploy path in addition to the timer.
REDEPLOY_EVERY_N_CYCLES = 999999
SETTLE_WAIT_SECS = 15

# ── ALIASES (required by bot_engine.py / risk_manager.py) ────
RISK_PER_TRADE_PCT = BASE_STAKE_PCT          # alias
MAX_CONCURRENT     = 20               # alias — NOTE: mismatched with MAX_CONCURRENT_TRADES=30, see flags
DAILY_LOSS_LIMIT   = DAILY_LOSS_LIMIT_PCT    # alias

# ── ADDITIONAL SIGNAL/RISK SETTINGS ──────────────────────────
MIN_MODULES_FOR_SIGNAL     = 3
MIN_INDICATOR_VOTES        = 3
OB_EXPIRY_BARS             = 100
NEWS_BLOCK_MINUTES         = 60
FOREX_LTF_GRANULARITY      = 900
OTHER_LTF_GRANULARITY      = 60
MIN_SIGNAL_PROBABILITY     = 1.8
MIN_STRENGTH_REPEAT_SYMBOL = 3

# ── DERIV WEBSOCKET ───────────────────────────────────────────
DERIV_WS_URL : str = os.environ.get(
    "DERIV_WS_URL",
    f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"
)

# ── SIGNAL GENERATION GATES (additional) ─────────────────────
MIN_SCORE                  = 2.0
MIN_CONFLUENCE             = 2
MIN_MODULE_STRENGTH        = 2
MIN_MODULE_STRENGTH_NORMAL = 2
MIN_CONFIDENCE_NORMAL      = 5
MIN_CONFIDENCE_FOR_PARTIAL = 5

# ── SESSION TIMING — disabled for 24/7 synthetics ────────────
DEAD_ZONE_START_UTC  = 0
DEAD_ZONE_END_UTC    = 5
BOOM500_PRIME_START  = 7
BOOM500_PRIME_END    = 12
TRADE_DURATION = 14
TRADE_DURATION_UNIT = "m"

# ══════════════════════════════════════════════════════════════
# NEW STRATEGY CONFIG (added in this pass — none of these are
# wired into ALL_SYMBOLS / signal_engine.py routing yet; they are
# config surfaces for strategies you're building incrementally)
# ══════════════════════════════════════════════════════════════

# ── DIGIT STRATEGY (Over/Under) ───────────────────────────────
# When True, a Digit Over/Under signal must be confirmed by BOTH an
# indicator-based read AND a statistical digit-frequency read before
# it fires. DIGIT_SYMBOLS is still empty above, so this is inert
# until you populate that list.
DIGIT_HYBRID_MODE = False

# ── ACCUMULATOR SETTINGS ──────────────────────────────────────
ACCU_GROWTH_RATE_MIN = 1.0   # percent, per-tick growth rate floor
ACCU_GROWTH_RATE_MAX = 5.0   # percent, per-tick growth rate ceiling
# Fraction of a symbol's historical average in-range tick survival at
# which to take profit early instead of holding to knockout.
# e.g. 0.7 = exit once you've captured 70% of the typical survival length.
ACCU_EXIT_FRACTION = 0.7

# ── STRATEGY PERFORMANCE MONITORING ───────────────────────────
# Once a (strategy, symbol) pair has 100+ logged trades, flag it as
# underperforming if its win rate falls below this floor.
STRATEGY_WIN_RATE_FLOOR = 0.55
STRATEGY_WIN_RATE_MIN_TRADES = 100

# ── META-LABELING (future ML filter) ──────────────────────────
META_LABEL_MIN_TRADES      = 200   # trades required before the filter is trusted
META_LABEL_RETRAIN_EVERY_N = 100   # retrain cadence, in newly logged trades

# ── POSITION SIZING (Kelly) ───────────────────────────────────
# Conservative multiplier applied to full Kelly-optimal sizing.
# 0.25 = quarter-Kelly.
KELLY_FRACTION_MULTIPLIER = 0.25

# ── ENSEMBLE MODE ──────────────────────────────────────────────
# When True, requires 2+ independent strategies to agree within the
# agreement window before a signal fires.
ENSEMBLE_MODE = False
ENSEMBLE_AGREEMENT_WINDOW_SECS = 60
ENSEMBLE_MIN_STRATEGIES_AGREEING = 2

# ── SESSION / DAY-OF-WEEK SCORE WEIGHTING ─────────────────────
# Multiplier applied to signal score based on symbol category, UTC
# hour, and day of week. Range is 0.8-1.2 by convention (0.8 = dampen,
# 1.2 = boost, 1.0 = neutral/no adjustment).
#
# `days` uses Python's datetime.weekday() convention:
#   Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6
# `days: None` means "applies every day". `hours_utc` is an inclusive
# (start, end) 24h UTC range; `hours_utc: None` means "applies all hours".
#
# NOTE: Boom/Crash and Jump symbols are currently NOT traded by this bot
# (BOOM_CRASH = [] and JUMP = [] above, since they don't support Rise/Fall
# via this API). These entries are config-only placeholders for when/if
# you add a Multipliers-contract strategy for them — they have no effect
# until something actually reads this table for those categories.
SESSION_DOW_WEIGHT_TABLE = {
    "BOOM600_CRASH900": {
        "hours_utc": (14, 20),   # boosted 14:00-20:00 UTC
        "days": None,             # every day
        "multiplier": 1.2,
    },
    "BOOM300N_CRASH300N": {
        "hours_utc": None,
        "days": [6],               # Sunday
        "multiplier": 1.15,
    },
    "JUMP50": {
        "hours_utc": (11, 13),   # around 12:00 UTC
        "days": [5],               # Saturday
        "multiplier": 1.15,
    },
}
SESSION_DOW_WEIGHT_DEFAULT = 1.0  # applied when no table entry matches
