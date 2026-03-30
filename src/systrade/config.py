"""
Central strategy configuration.

To swap strategies, change STRATEGY_NAME and STRATEGY_PARAMS below.
Both live trading and backtesting reference this module.

Available strategies: alpha_vwap, quant_vwap, regime, vwap
"""

from systrade.strategies.registry import create_strategy
from systrade.strategy import Strategy

# ── Active Strategy ──────────────────────────────────────────

STRATEGY_NAME = "alpha_vwap"

STRATEGY_PARAMS: dict = {
    # Leveraged ETFs: NVDL(2x), GGLL(2x), NRGU(3x), AAPU(2x), TQQQ(3x)
    "symbols": ("NVDL", "GGLL", "NRGU", "AAPU", "TQQQ"),
    "max_active_symbols": 2,
    "min_gap_pct": 0.15,
    "twap_tranches": 3,
    "twap_spacing": 2,
    "twap_offset_bps": 1.0,
    "entry_z": 3.0,
    "fft_entry_z": 2.0,
    "exit_z": 1.0,
    "position_frac": 0.15,         # down from 0.50 — discipline for leveraged products
    "max_positions": 2,
    "min_bars": 20,
    "regime_confidence": 0.60,
    "cooldown_bars": 180,
    "trailing_stop_pct": 0.015,    # wider than 0.4% — leveraged ETFs need room
    "stop_z": 4.0,                 # tighter than 5.0 — cut losers faster
    "max_loss_pct": 0.03,          # hard 3% max loss per trade
}

# Context-specific overrides (merged on top of STRATEGY_PARAMS)
LIVE_OVERRIDES: dict = {
    "leverage": 1.0,  # Alpaca's 4x buying power already applied
}

BACKTEST_OVERRIDES: dict = {
    "leverage": 2.0,
}

STARTING_CASH = 1_000_000


# ── Factory ──────────────────────────────────────────────────

def make_live_strategy(**overrides) -> Strategy:
    """Create the active strategy configured for live trading."""
    return create_strategy(
        STRATEGY_NAME, **{**STRATEGY_PARAMS, **LIVE_OVERRIDES, **overrides},
    )


def make_backtest_strategy(**overrides) -> Strategy:
    """Create the active strategy configured for backtesting."""
    return create_strategy(
        STRATEGY_NAME, **{**STRATEGY_PARAMS, **BACKTEST_OVERRIDES, **overrides},
    )
