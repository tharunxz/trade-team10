#!/usr/bin/env python3
"""
Backtest strategies on historical 1-min data.

Usage:
    python scripts/backtest.py --data data/history_1min.csv
    python scripts/backtest.py --data data/history_1min.csv --strategy regime
    python scripts/backtest.py --data data/history_1min.csv --strategy alpha_vwap --params '{"leverage": 1.0}'
    python scripts/backtest.py --data data/history_1min.csv --sweep
"""

import argparse
import itertools
import json
import math
from pathlib import Path

import pandas as pd

from systrade.broker import BacktestBroker
from systrade.config import make_backtest_strategy, STARTING_CASH, STRATEGY_NAME
from systrade.engine import Engine
from systrade.feed import HistoricalFeed
from systrade.history import FileHistoryProvider
from systrade.portfolio import Portfolio
from systrade.strategies.registry import create_strategy, list_strategies


def run_backtest(data_path: str, strategy_name: str | None = None, **strategy_params) -> dict:
    """Run a backtest. Defaults to the active strategy from config."""
    provider = FileHistoryProvider(path=data_path)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()

    if strategy_name is None:
        strategy = make_backtest_strategy(**strategy_params)
        strategy_name = STRATEGY_NAME
    else:
        strategy = create_strategy(strategy_name, **strategy_params)

    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(
        feed=feed, broker=broker, strategy=strategy,
        cash=STARTING_CASH, portfolio=portfolio,
    )
    engine.run()
    return _compute_metrics(portfolio, strategy, strategy_name, strategy_params)


def _compute_metrics(
    portfolio: Portfolio, strategy, strategy_name: str, params: dict,
) -> dict:
    """Extract performance metrics from a completed backtest."""
    activity = portfolio.activity()
    df = activity.df()

    label = {"strategy": strategy_name, **params}

    if df.empty:
        return {**label, "total_return": 0.0, "max_drawdown": 0.0, "sharpe": 0.0, "trades": 0}

    total_return = activity.total_return()
    equity = activity.equity_curve()
    running_max = equity.cummax()
    drawdown = (equity - running_max) / running_max
    max_dd = drawdown.min()

    returns = equity.pct_change().dropna()
    sharpe = 0.0
    if len(returns) > 1 and returns.std() > 0:
        sharpe = (returns.mean() / returns.std()) * math.sqrt(390 * 252)

    trades = len(strategy._trading_records)

    return {
        **label,
        "total_return": round(total_return * 100, 3),
        "max_drawdown": round(max_dd * 100, 3),
        "sharpe": round(sharpe, 3),
        "trades": trades,
    }


# ── Parameter sweeps ─────────────────────────────────────────────────

SWEEP_GRIDS: dict[str, list[dict]] = {
    "vwap": [
        {"entry_z": ez, "exit_z": xz, "rolling_window": w}
        for ez, xz, w in itertools.product(
            [1.5, 2.0, 2.5], [0.3, 0.5, 0.7], [15, 20, 30],
        )
    ],
    "regime": [
        {"orb_bars": ob, "entry_z": ez, "breakout_z": bz,
         "trailing_stop_pct": ts, "position_frac": pf}
        for ob, ez, bz, ts, pf in itertools.product(
            [3, 5, 10], [1.5, 2.0, 2.5], [3.0, 3.5, 4.0],
            [0.003, 0.005, 0.01], [0.50, 0.80],
        )
    ],
}


def sweep(data_path: str, strategy_name: str) -> list[dict]:
    """Run parameter sweep for a strategy (if a grid is defined)."""
    grid = SWEEP_GRIDS.get(strategy_name)
    if grid is None:
        print(f"No sweep grid defined for '{strategy_name}'. Running with defaults.")
        result = run_backtest(data_path, strategy_name)
        _print_result(result)
        return [result]

    print(f"=== {strategy_name.upper()} PARAMETER SWEEP ({len(grid)} combos) ===")
    results = []
    for params in grid:
        params_str = " ".join(f"{k}={v}" for k, v in params.items())
        print(f"  {strategy_name} {params_str} ... ", end="", flush=True)
        m = run_backtest(data_path, strategy_name, **params)
        results.append(m)
        print(f"return={m['total_return']:.2f}% dd={m['max_drawdown']:.2f}% sharpe={m['sharpe']:.2f}")
    return results


def _print_result(m: dict) -> None:
    name = m["strategy"].upper()
    print(
        f"  {name}: return={m['total_return']:.3f}% "
        f"dd={m['max_drawdown']:.3f}% "
        f"sharpe={m['sharpe']:.2f} "
        f"trades={m['trades']}"
    )


# ── CLI ──────────────────────────────────────────────────────────────

def main() -> None:
    available = list_strategies()
    parser = argparse.ArgumentParser(description="Backtest strategies")
    parser.add_argument("--data", required=True, help="Path to 1-min CSV")
    parser.add_argument(
        "--strategy", default=None,
        help=f"Strategy name (default: {STRATEGY_NAME} from config). "
             f"Use 'all' to run every registered strategy. Available: {', '.join(available)}",
    )
    parser.add_argument(
        "--params", default="{}",
        help="JSON string of strategy params, e.g. '{\"leverage\": 1.0}'",
    )
    parser.add_argument("--sweep", action="store_true", help="Run parameter sweep")
    args = parser.parse_args()

    if not Path(args.data).exists():
        print(f"Error: data file not found: {args.data}")
        return

    strategy_params = json.loads(args.params)

    if args.strategy is None:
        # Use active strategy from config
        if args.sweep:
            all_results = sweep(args.data, STRATEGY_NAME)
            df = pd.DataFrame(all_results).sort_values("total_return", ascending=False)
            print("\n=== ALL RESULTS (sorted by return) ===")
            print(df.head(20).to_string(index=False))
        else:
            print(f"Running {STRATEGY_NAME} backtest (from config) ...")
            m = run_backtest(args.data, **strategy_params)
            _print_result(m)
    else:
        targets = available if args.strategy == "all" else [args.strategy]

        if args.sweep:
            all_results = []
            for name in targets:
                all_results.extend(sweep(args.data, name))
            df = pd.DataFrame(all_results).sort_values("total_return", ascending=False)
            print("\n=== ALL RESULTS (sorted by return) ===")
            print(df.head(20).to_string(index=False))
        else:
            for name in targets:
                print(f"Running {name} backtest ...")
                m = run_backtest(args.data, name, **strategy_params)
                _print_result(m)


if __name__ == "__main__":
    main()
