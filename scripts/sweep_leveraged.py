#!/usr/bin/env python3
"""
Parameter sweep for leveraged ETF stop/sizing parameters.

Tests 108 combinations of (max_loss_pct, trailing_stop_pct, position_frac, entry_z)
on historical 1-min data.  All combos use exit_z=0.0 and cooldown_bars=45 as baseline.

Usage:
    set -a && source .env && set +a
    python scripts/sweep_leveraged.py
"""

from __future__ import annotations

import itertools
import logging
import math
import sys
import time
from typing import NamedTuple
from unittest.mock import patch

import pandas as pd

# Silence noisy loggers before importing strategy modules
logging.getLogger("systrade").setLevel(logging.ERROR)
logging.getLogger("hmmlearn").setLevel(logging.CRITICAL)

from systrade.broker import BacktestBroker
from systrade.config import (
    BACKTEST_OVERRIDES,
    STARTING_CASH,
    STRATEGY_NAME,
    STRATEGY_PARAMS,
)
from systrade.engine import Engine
from systrade.feed import HistoricalFeed
from systrade.history import FileHistoryProvider
from systrade.portfolio import Portfolio
from systrade.strategies.registry import create_strategy
import systrade.db as _db
from systrade.strategies.alpha_vwap import AlphaVWAPStrategy

# ── Data path ───────────────────────────────────────────────────────

DATA_PATH = "data/history_1min.csv"

# ── Fixed baseline overrides (new tuned values) ────────────────────

BASELINE_OVERRIDES: dict = {
    "exit_z": 0.0,
    "cooldown_bars": 45,
}

# ── Sweep grid ──────────────────────────────────────────────────────

MAX_LOSS_PCTS = [0.03, 0.05, 0.07, 0.10]
TRAILING_STOP_PCTS = [0.015, 0.025, 0.04]
POSITION_FRACS = [0.08, 0.15, 0.25]
ENTRY_ZS = [2.0, 2.5, 3.0]

TOTAL_COMBOS = (
    len(MAX_LOSS_PCTS)
    * len(TRAILING_STOP_PCTS)
    * len(POSITION_FRACS)
    * len(ENTRY_ZS)
)


class SweepResult(NamedTuple):
    """Immutable result for one parameter combination."""
    max_loss_pct: float
    trailing_stop_pct: float
    position_frac: float
    entry_z: float
    total_return_pct: float
    max_drawdown_pct: float
    sharpe: float
    trades: int


# ── Disable expensive per-bar I/O for sweep speed ──────────────────

def _noop_save_checkpoint(self: AlphaVWAPStrategy) -> None:
    """No-op replacement — skip file/DB checkpoint writes during sweep."""


def _noop_load_checkpoint(self: AlphaVWAPStrategy) -> bool:
    """No-op replacement — no checkpoint to load in sweep mode."""
    return False


# Patch once at module level so every combo benefits
_original_save = AlphaVWAPStrategy._save_checkpoint
_original_load = AlphaVWAPStrategy._load_checkpoint
AlphaVWAPStrategy._save_checkpoint = _noop_save_checkpoint  # type: ignore[assignment]
AlphaVWAPStrategy._load_checkpoint = _noop_load_checkpoint  # type: ignore[assignment]

# Also neuter DB bar saves (they fail without a running DB anyway)
_db.save_bars = lambda bars: False  # type: ignore[assignment]
_db.save_checkpoint = lambda *a, **kw: False  # type: ignore[assignment]
_db.load_checkpoint = lambda *a, **kw: None  # type: ignore[assignment]


# ── Single backtest run ─────────────────────────────────────────────

def _run_single(
    max_loss_pct: float,
    trailing_stop_pct: float,
    position_frac: float,
    entry_z: float,
) -> SweepResult:
    """Run one backtest with the given parameter overrides and return metrics."""
    params = {
        **STRATEGY_PARAMS,
        **BACKTEST_OVERRIDES,
        **BASELINE_OVERRIDES,
        "max_loss_pct": max_loss_pct,
        "trailing_stop_pct": trailing_stop_pct,
        "position_frac": position_frac,
        "entry_z": entry_z,
    }

    provider = FileHistoryProvider(path=DATA_PATH)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()
    strategy = create_strategy(STRATEGY_NAME, **params)
    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(
        feed=feed,
        broker=broker,
        strategy=strategy,
        cash=STARTING_CASH,
        portfolio=portfolio,
    )
    engine.run()

    activity = portfolio.activity()
    df = activity.df()

    if df.empty:
        return SweepResult(
            max_loss_pct=max_loss_pct,
            trailing_stop_pct=trailing_stop_pct,
            position_frac=position_frac,
            entry_z=entry_z,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            sharpe=0.0,
            trades=0,
        )

    total_return = activity.total_return()
    equity = activity.equity_curve()
    running_max = equity.cummax()
    drawdown = (equity - running_max) / running_max
    max_dd = drawdown.min()

    returns = equity.pct_change().dropna()
    sharpe = 0.0
    if len(returns) > 1 and returns.std() > 0:
        # Annualize: 390 bars/day * 252 days/year
        sharpe = (returns.mean() / returns.std()) * math.sqrt(390 * 252)

    trades = len(strategy._trading_records)

    return SweepResult(
        max_loss_pct=max_loss_pct,
        trailing_stop_pct=trailing_stop_pct,
        position_frac=position_frac,
        entry_z=entry_z,
        total_return_pct=round(total_return * 100, 3),
        max_drawdown_pct=round(max_dd * 100, 3),
        sharpe=round(sharpe, 3),
        trades=trades,
    )


# ── Sweep orchestrator ──────────────────────────────────────────────

def run_sweep() -> pd.DataFrame:
    """Run the full 108-combo parameter sweep and return sorted results."""
    combos = list(itertools.product(
        MAX_LOSS_PCTS,
        TRAILING_STOP_PCTS,
        POSITION_FRACS,
        ENTRY_ZS,
    ))

    assert len(combos) == TOTAL_COMBOS, (
        f"Expected {TOTAL_COMBOS} combos, got {len(combos)}"
    )

    print(f"=== LEVERAGED ETF PARAMETER SWEEP ({TOTAL_COMBOS} combos) ===")
    print(f"Strategy: {STRATEGY_NAME}  |  Cash: ${STARTING_CASH:,.0f}")
    print(f"Baseline: exit_z={BASELINE_OVERRIDES['exit_z']}, "
          f"cooldown_bars={BASELINE_OVERRIDES['cooldown_bars']}")
    print(f"Sweep: max_loss={MAX_LOSS_PCTS}, trail={TRAILING_STOP_PCTS}, "
          f"pos_frac={POSITION_FRACS}, entry_z={ENTRY_ZS}")
    print("-" * 90)

    results: list[SweepResult] = []
    t0 = time.monotonic()

    for i, (ml, ts, pf, ez) in enumerate(combos, 1):
        label = (
            f"[{i:3d}/{TOTAL_COMBOS}] "
            f"max_loss={ml:.0%} trail={ts:.1%} pos={pf:.0%} entry_z={ez}"
        )
        print(f"  {label} ... ", end="", flush=True)

        result = _run_single(ml, ts, pf, ez)
        results.append(result)

        print(
            f"ret={result.total_return_pct:+7.2f}%  "
            f"dd={result.max_drawdown_pct:+7.2f}%  "
            f"sharpe={result.sharpe:+6.2f}  "
            f"trades={result.trades}"
        )

    elapsed = time.monotonic() - t0
    print(f"\nCompleted {TOTAL_COMBOS} backtests in {elapsed:.1f}s "
          f"({elapsed / TOTAL_COMBOS:.2f}s/combo)")

    df = pd.DataFrame(results)
    df = df.sort_values("sharpe", ascending=False).reset_index(drop=True)
    return df


# ── Main ────────────────────────────────────────────────────────────

def main() -> None:
    df = run_sweep()

    print("\n" + "=" * 90)
    print("TOP 20 BY SHARPE RATIO")
    print("=" * 90)
    pd.set_option("display.max_columns", 20)
    pd.set_option("display.width", 120)
    pd.set_option("display.float_format", lambda x: f"{x:+.3f}" if abs(x) < 1000 else f"{x:+.0f}")
    print(df.head(20).to_string(index=False))

    print("\n" + "=" * 90)
    print("BOTTOM 5 (WORST SHARPE)")
    print("=" * 90)
    print(df.tail(5).to_string(index=False))

    # Current params baseline comparison
    current = df[
        (df["max_loss_pct"] == 0.03)
        & (df["trailing_stop_pct"] == 0.015)
        & (df["position_frac"] == 0.15)
        & (df["entry_z"] == 3.0)
    ]
    if not current.empty:
        row = current.iloc[0]
        rank = df.index[
            (df["max_loss_pct"] == 0.03)
            & (df["trailing_stop_pct"] == 0.015)
            & (df["position_frac"] == 0.15)
            & (df["entry_z"] == 3.0)
        ][0] + 1
        print(f"\n--- CURRENT PARAMS (rank {rank}/{TOTAL_COMBOS}) ---")
        print(f"  return={row.total_return_pct:+.3f}%  "
              f"dd={row.max_drawdown_pct:+.3f}%  "
              f"sharpe={row.sharpe:+.3f}  "
              f"trades={row.trades}")


if __name__ == "__main__":
    main()
