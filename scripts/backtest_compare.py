#!/usr/bin/env python3
"""
Compare old vs new strategy parameters on historical data.

Usage:
    python scripts/backtest_compare.py --data data/history_1min.csv
"""

import argparse
import math
from pathlib import Path

import pandas as pd

from systrade.broker import BacktestBroker
from systrade.config import STARTING_CASH, STRATEGY_PARAMS, BACKTEST_OVERRIDES
from systrade.engine import Engine
from systrade.feed import HistoricalFeed
from systrade.history import FileHistoryProvider
from systrade.portfolio import Portfolio
from systrade.strategies.registry import create_strategy


# ── Parameter sets to compare ────────────────────────────────────

OLD_PARAMS = {
    "exit_z": 1.0,
    "cooldown_bars": 180,
}

NEW_PARAMS = {
    "exit_z": 0.0,
    "cooldown_bars": 45,
}


def run_one(data_path: str, label: str, overrides: dict) -> dict:
    """Run a single backtest with merged params."""
    merged = {**STRATEGY_PARAMS, **BACKTEST_OVERRIDES, **overrides}
    provider = FileHistoryProvider(path=data_path)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()
    strategy = create_strategy("alpha_vwap", **merged)
    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(
        feed=feed, broker=broker, strategy=strategy,
        cash=STARTING_CASH, portfolio=portfolio,
    )
    engine.run()

    activity = portfolio.activity()
    df = activity.df()

    if df.empty:
        return {
            "variant": label, "total_return_pct": 0.0, "max_drawdown_pct": 0.0,
            "sharpe": 0.0, "trades": 0, "wins": 0, "win_rate_pct": 0.0,
            "avg_return_per_trade_pct": 0.0, "final_equity": STARTING_CASH,
        }

    total_return = activity.total_return()
    equity = activity.equity_curve()
    running_max = equity.cummax()
    drawdown = (equity - running_max) / running_max
    max_dd = drawdown.min()

    returns = equity.pct_change().dropna()
    sharpe = 0.0
    if len(returns) > 1 and returns.std() > 0:
        sharpe = (returns.mean() / returns.std()) * math.sqrt(390 * 252)

    records = strategy._trading_records
    trades = len(records)
    wins = sum(1 for r in records if r.get("pnl", 0) > 0)
    win_rate = (wins / trades * 100) if trades > 0 else 0.0
    avg_ret = (total_return / trades * 100) if trades > 0 else 0.0
    final_equity = equity.iloc[-1] if len(equity) > 0 else STARTING_CASH

    return {
        "variant": label,
        "total_return_pct": round(total_return * 100, 3),
        "max_drawdown_pct": round(max_dd * 100, 3),
        "sharpe": round(sharpe, 3),
        "trades": trades,
        "wins": wins,
        "win_rate_pct": round(win_rate, 1),
        "avg_return_per_trade_pct": round(avg_ret, 3),
        "final_equity": round(final_equity, 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="A/B backtest comparison")
    parser.add_argument("--data", required=True, help="Path to 1-min CSV")
    args = parser.parse_args()

    if not Path(args.data).exists():
        print(f"Error: {args.data} not found")
        return

    variants = [
        ("OLD (exit_z=1.0, cooldown=180)", OLD_PARAMS),
        ("NEW (exit_z=0.0, cooldown=45)", NEW_PARAMS),
    ]

    results = []
    for label, overrides in variants:
        print(f"Running: {label} ...", flush=True)
        result = run_one(args.data, label, overrides)
        results.append(result)

    # Per-symbol breakdown
    symbols = list(STRATEGY_PARAMS.get("symbols", ()))
    symbol_results = []
    for sym in symbols:
        for label, overrides in variants:
            sym_overrides = {**overrides, "symbols": (sym,), "max_active_symbols": 1}
            print(f"  Running {sym} {label.split('(')[0].strip()} ...", flush=True)
            result = run_one(args.data, f"{sym} {label}", sym_overrides)
            symbol_results.append(result)

    print("\n" + "=" * 72)
    print("OVERALL COMPARISON (all symbols)")
    print("=" * 72)
    df = pd.DataFrame(results)
    print(df.to_string(index=False))

    print("\n" + "=" * 72)
    print("PER-SYMBOL BREAKDOWN")
    print("=" * 72)
    df_sym = pd.DataFrame(symbol_results)
    print(df_sym.to_string(index=False))


if __name__ == "__main__":
    main()
