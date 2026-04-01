#!/usr/bin/env python3
"""
Compare alpha_vwap vs VSB strategies and measure daily return correlation.
If uncorrelated, run them together and compare vs each alone.
"""

import math

import numpy as np
import pandas as pd

from systrade.broker import BacktestBroker
from systrade.config import STARTING_CASH, STRATEGY_PARAMS, BACKTEST_OVERRIDES
from systrade.engine import Engine
from systrade.feed import HistoricalFeed
from systrade.history import FileHistoryProvider
from systrade.portfolio import Portfolio
from systrade.strategies.registry import create_strategy

DATA_PATH = "data/history_ytd.csv"


def run_single(strategy_name: str, params: dict) -> tuple[dict, pd.Series]:
    """Run a single strategy. Returns (metrics_dict, equity_series)."""
    provider = FileHistoryProvider(path=DATA_PATH)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()
    strategy = create_strategy(strategy_name, **params)
    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(feed=feed, broker=broker, strategy=strategy,
                    cash=STARTING_CASH, portfolio=portfolio)
    engine.run()

    activity = portfolio.activity()
    equity = activity.equity_curve()
    trades = len(strategy._trading_records)

    if equity.empty:
        return {"trades": 0, "return": 0, "sharpe": 0, "max_dd": 0}, pd.Series(dtype=float)

    total_ret = activity.total_return()
    running_max = equity.cummax()
    dd = ((equity - running_max) / running_max).min()
    rets = equity.pct_change().dropna()
    sharpe = (rets.mean() / rets.std()) * math.sqrt(390 * 252) if len(rets) > 1 and rets.std() > 0 else 0

    return {
        "trades": trades,
        "return_pct": round(total_ret * 100, 3),
        "sharpe": round(sharpe, 3),
        "max_dd_pct": round(dd * 100, 3),
        "final_equity": round(equity.iloc[-1], 2),
    }, equity


def run_combined(alpha_params: dict, vsb_params: dict) -> tuple[dict, pd.Series]:
    """Run both strategies on the same engine with shared portfolio."""
    provider = FileHistoryProvider(path=DATA_PATH)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()
    alpha = create_strategy("alpha_vwap", **alpha_params)
    vsb = create_strategy("vsb", **vsb_params)
    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(feed=feed, broker=broker, strategies=[alpha, vsb],
                    cash=STARTING_CASH, portfolio=portfolio)
    engine.run()

    activity = portfolio.activity()
    equity = activity.equity_curve()
    trades = len(alpha._trading_records) + len(vsb._trading_records)

    if equity.empty:
        return {"trades": 0, "return": 0, "sharpe": 0, "max_dd": 0}, pd.Series(dtype=float)

    total_ret = activity.total_return()
    running_max = equity.cummax()
    dd = ((equity - running_max) / running_max).min()
    rets = equity.pct_change().dropna()
    sharpe = (rets.mean() / rets.std()) * math.sqrt(390 * 252) if len(rets) > 1 and rets.std() > 0 else 0

    return {
        "alpha_trades": len(alpha._trading_records),
        "vsb_trades": len(vsb._trading_records),
        "trades": trades,
        "return_pct": round(total_ret * 100, 3),
        "sharpe": round(sharpe, 3),
        "max_dd_pct": round(dd * 100, 3),
        "final_equity": round(equity.iloc[-1], 2),
    }, equity


def daily_returns(equity: pd.Series) -> pd.Series:
    """Resample 1-min equity to daily returns."""
    if equity.empty:
        return pd.Series(dtype=float)
    # Equity curve may have RangeIndex or DatetimeIndex
    if not isinstance(equity.index, pd.DatetimeIndex):
        # Use the activity df to get per-tick equity, group by day
        # Fall back to splitting into ~390-bar chunks (1 trading day)
        bars_per_day = 390
        n = len(equity)
        day_ends = list(range(bars_per_day - 1, n, bars_per_day))
        if day_ends[-1] != n - 1:
            day_ends.append(n - 1)
        daily_equity = equity.iloc[day_ends]
        return daily_equity.pct_change().dropna()
    daily = equity.resample("D").last().dropna()
    return daily.pct_change().dropna()


def main() -> None:
    alpha_params = {**STRATEGY_PARAMS, **BACKTEST_OVERRIDES}
    vsb_params = {
        "symbols": STRATEGY_PARAMS["symbols"],
        "rvol_threshold": 2.0,
        "position_frac": 0.10,
        "leverage": 2.0,
        "max_positions": 2,
    }

    print("=" * 60)
    print("STEP 1: Run each strategy independently")
    print("=" * 60)

    print("\nRunning alpha_vwap ...", flush=True)
    alpha_metrics, alpha_eq = run_single("alpha_vwap", alpha_params)
    print(f"  {alpha_metrics}")

    print("Running VSB ...", flush=True)
    vsb_metrics, vsb_eq = run_single("vsb", vsb_params)
    print(f"  {vsb_metrics}")

    print("\n" + "=" * 60)
    print("STEP 2: Correlation analysis")
    print("=" * 60)

    alpha_daily = daily_returns(alpha_eq)
    vsb_daily = daily_returns(vsb_eq)

    # Align on common dates
    common = alpha_daily.index.intersection(vsb_daily.index)
    if len(common) >= 3:
        a = alpha_daily.loc[common]
        v = vsb_daily.loc[common]
        corr = np.corrcoef(a.values, v.values)[0, 1]
        print(f"\n  Daily return correlation: {corr:.4f}")
        print(f"  Days compared: {len(common)}")
        print(f"  Interpretation: ", end="")
        if abs(corr) < 0.3:
            print("LOW correlation — strategies are independent. Good to combine.")
        elif abs(corr) < 0.6:
            print("MODERATE correlation — some diversification benefit.")
        else:
            print("HIGH correlation — limited diversification benefit.")
    else:
        corr = float("nan")
        print("\n  Not enough common trading days for correlation.")

    print("\n" + "=" * 60)
    print("STEP 3: Combined backtest (both strategies, shared portfolio)")
    print("=" * 60)

    print("\nRunning combined ...", flush=True)
    combined_metrics, combined_eq = run_combined(alpha_params, vsb_params)
    print(f"  {combined_metrics}")

    print("\n" + "=" * 60)
    print("COMPARISON TABLE")
    print("=" * 60)
    print(f"\n{'Metric':<20} {'alpha_vwap':>15} {'VSB':>15} {'COMBINED':>15}")
    print("-" * 65)
    for key in ["return_pct", "max_dd_pct", "sharpe", "trades", "final_equity"]:
        av = alpha_metrics.get(key, "—")
        vv = vsb_metrics.get(key, "—")
        cv = combined_metrics.get(key, "—")
        if isinstance(av, float):
            print(f"{key:<20} {av:>15.3f} {vv:>15.3f} {cv:>15.3f}")
        else:
            print(f"{key:<20} {av:>15} {vv:>15} {cv:>15}")

    print(f"\n  Correlation: {corr:.4f}")
    if abs(corr) < 0.3:
        print("  VERDICT: Strategies are uncorrelated — combined approach is viable.")
    elif abs(corr) < 0.6:
        print("  VERDICT: Moderate correlation — some benefit from combining.")
    else:
        print("  VERDICT: High correlation — combining adds limited value.")


if __name__ == "__main__":
    main()
