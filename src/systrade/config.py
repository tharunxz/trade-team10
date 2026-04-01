"""
Central strategy configuration.

To swap strategies, change STRATEGY_NAME and STRATEGY_PARAMS below.
Both live trading and backtesting reference this module.

Available strategies: alpha_vwap, quant_vwap, regime, vwap

Environment variables (loaded from .env if present):
    ALPACA_API_KEY     — Alpaca API key
    ALPACA_API_SECRET  — Alpaca API secret
    ALPACA_PAPER       — "True"/"False", defaults to True
    DATABASE_URL       — PostgreSQL URL (optional)
"""

import os

from dotenv import load_dotenv

from systrade.strategies.registry import create_strategy
from systrade.strategy import Strategy

# Load .env once at import time.  Existing environment variables take
# precedence (load_dotenv does not overwrite already-set variables).
load_dotenv()

# ── Active Strategy ──────────────────────────────────────────

STRATEGY_NAME = "vsb"

# Trading universe — used for backtesting and as fallback when scanner is off.
# Liquid leveraged ETFs verified with 7-9 IEX bars per 10 min:
#   TQQQ (3x Nasdaq), SOXL (3x Semis), TNA (3x Russell),
#   SQQQ (3x inv Nasdaq), UDOW (3x Dow)
TRADING_SYMBOLS: tuple[str, ...] = ("TQQQ", "SOXL", "TNA", "SQQQ", "UDOW")

# Full screening universe for live scanning.
# All subscribed at startup (single batched API call); scanner scores and
# selects the top K each hour.
SCAN_UNIVERSE: tuple[str, ...] = (
    # Tier 1: 3x leveraged broad-market ETFs
    "TQQQ", "SQQQ", "SOXL", "SOXS", "TNA", "TZA",
    "UDOW", "SDOW", "UPRO", "SPXU", "FAS", "FAZ",
    "LABU", "LABD", "ERX",
    # Tier 2: 2x leveraged & sector ETFs
    "QLD", "QID", "SSO", "SDS", "TECL", "FNGU",
    "JNUG", "JDST", "NUGT", "DUST",
    # Tier 3: high-volume tech / mega-cap
    "NVDA", "TSLA", "AMD", "AAPL", "AMZN",
    "META", "GOOGL", "MSFT", "NFLX", "COIN",
    "PLTR", "SOFI", "MARA", "RIOT", "MU",
    # Tier 4: high-beta / momentum names
    "GME", "AMC", "RIVN", "LCID", "NIO",
    "SNAP", "ROKU", "SQ", "HOOD", "RBLX",
    "DKNG", "ARKK",
    # Tier 5: volatility products
    "UVXY", "SVXY", "VXX", "VIXY",
)

# Broad-market leveraged ETFs that are shortable on Alpaca paper/live.
# Leveraged single-stock ETFs cannot be shorted; only broad-market ones can.
SHORTABLE_SYMBOLS: frozenset[str] = frozenset({
    "TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU",
    "TNA", "TZA", "FAS", "FAZ", "ERX", "NRGU", "LABU", "LABD",
    "UDOW", "SDOW", "QLD", "QID", "SSO", "SDS", "TECL",
    "FNGU", "JNUG", "JDST", "NUGT", "DUST",
    "UVXY", "SVXY", "VXX", "VIXY", "ARKK",
})

SCANNER_PARAMS: dict = {
    "active_count": 8,
    "scan_interval_bars": 60,
    "min_rvol_for_scan": 1.0,
    "weight_rvol": 0.40,
    "weight_gap": 0.25,
    "weight_atr_pct": 0.20,
    "weight_range": 0.15,
}

STRATEGY_PARAMS: dict = {
    "symbols": TRADING_SYMBOLS,
    "shortable_symbols": SHORTABLE_SYMBOLS,
    "max_active_symbols": 3,
    "min_gap_pct": 0.15,
    "twap_tranches": 3,
    "twap_spacing": 2,
    "twap_offset_bps": 1.0,
    "entry_z": 2.0,                # lowered from 3.0 — z=3.0 too extreme, 94% hit hard stop
    "fft_entry_z": 1.5,            # lowered from 2.0 — proportional with entry_z
    "exit_z": 0.0,                 # lowered from 1.0 — trail activates at VWAP, not before
    "position_frac": 0.07,          # reduced from 15% — secondary to VSB, limits drag
    "max_positions": 2,
    "min_bars": 20,
    "regime_confidence": 0.60,
    "cooldown_bars": 45,           # lowered from 180 — 45 min vs 3 hr, allows re-entry
    "trailing_stop_pct": 0.025,    # widened from 1.5% to 2.5% — 1.5% triggers on noise
    "stop_z": 4.0,                 # hard z-stop unchanged
    "max_loss_pct": 0.05,          # widened from 3% to 5% — 3x ETFs need room to revert
}

# Context-specific overrides (merged on top of STRATEGY_PARAMS)
LIVE_OVERRIDES: dict = {
    "leverage": 1.0,  # Alpaca's 4x buying power already applied
}

BACKTEST_OVERRIDES: dict = {
    "leverage": 2.0,
}

VSB_STRATEGY_NAME = "vsb"

VSB_PARAMS: dict = {
    "symbols": TRADING_SYMBOLS,
    "scan_universe": SCAN_UNIVERSE,
    "shortable_symbols": SHORTABLE_SYMBOLS,
    "rvol_threshold": 2.0,
    "atr_period": 14,
    "atr_stop_mult": 1.5,
    "atr_trail_mult": 2.0,
    "atr_profit_trigger": 1.0,
    "position_frac": 0.15,         # primary strategy — gets more capital allocation
    "max_positions": 2,
    **SCANNER_PARAMS,
}

STARTING_CASH = 1_000_000


# ── Environment helpers ──────────────────────────────────────

def get_alpaca_credentials() -> tuple[str, str, bool]:
    """Return (api_key, secret_key, paper_trading) from environment.

    Raises ValueError if either key is missing.
    """
    api_key = os.environ.get("ALPACA_API_KEY")
    secret_key = os.environ.get("ALPACA_API_SECRET")
    paper_trading = os.environ.get("ALPACA_PAPER", "True").lower() == "true"

    if not api_key or not secret_key:
        raise ValueError(
            "ALPACA_API_KEY and ALPACA_API_SECRET must be set in the environment or .env file."
        )
    return api_key, secret_key, paper_trading


# ── Factory ──────────────────────────────────────────────────

def make_live_strategy(**overrides) -> Strategy:
    """Create the alpha_vwap strategy configured for live trading."""
    return create_strategy(
        STRATEGY_NAME, **{**STRATEGY_PARAMS, **LIVE_OVERRIDES, **overrides},
    )


def make_live_vsb(**overrides) -> Strategy:
    """Create the VSB strategy configured for live trading."""
    return create_strategy(
        VSB_STRATEGY_NAME, **{**VSB_PARAMS, **LIVE_OVERRIDES, **overrides},
    )


def make_live_strategies(**overrides) -> list[Strategy]:
    """Create all live strategies to run simultaneously.

    Currently VSB only.  alpha_vwap shelved — negative alpha in both bull
    and bear backtests (Q2 2025, YTD 2026).  HMM regime detection is 10%
    accurate on TRENDING, making MR entries unreliable on leveraged ETFs.
    """
    return [make_live_vsb(**overrides)]


def make_backtest_strategy(**overrides) -> Strategy:
    """Create the alpha_vwap strategy configured for backtesting."""
    return create_strategy(
        STRATEGY_NAME, **{**STRATEGY_PARAMS, **BACKTEST_OVERRIDES, **overrides},
    )


def make_backtest_vsb(**overrides) -> Strategy:
    """Create the VSB strategy configured for backtesting.

    Strips scan_universe so backtests use only the symbols present in
    historical data (FileFeed validates against _available_symbols).
    """
    bt_params = {k: v for k, v in VSB_PARAMS.items() if k != "scan_universe"}
    return create_strategy(
        VSB_STRATEGY_NAME, **{**bt_params, **BACKTEST_OVERRIDES, **overrides},
    )
