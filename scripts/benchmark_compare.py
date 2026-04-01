#!/usr/bin/env python3
"""
Compare strategy returns against passive benchmarks (SPY buy-and-hold, TQQQ buy-and-hold).
"""

import math
import os

import numpy as np
import pandas as pd
from alpaca.data import StockHistoricalDataClient, StockBarsRequest, TimeFrame, TimeFrameUnit
from datetime import datetime
from zoneinfo import ZoneInfo

from systrade.broker import BacktestBroker
from systrade.config import (
    STARTING_CASH, STRATEGY_PARAMS, BACKTEST_OVERRIDES,
    VSB_PARAMS, get_alpaca_credentials,
)
from systrade.engine import Engine
from systrade.feed import HistoricalFeed
from systrade.history import FileHistoryProvider
from systrade.portfolio import Portfolio
from systrade.strategies.registry import create_strategy

ET = ZoneInfo("America/New_York")
DATA_PATH = "data/history_q2_2025.csv"


def fetch_benchmark(symbol: str, start: str, end: str) -> dict:
    """Fetch daily bars and compute buy-and-hold metrics."""
    api_key, secret_key, _ = get_alpaca_credentials()
    client = StockHistoricalDataClient(api_key, secret_key)
    req = StockBarsRequest(
        symbol_or_symbols=[symbol],
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Day),
        start=datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=ET),
        end=datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, tzinfo=ET),
    )
    bars = client.get_stock_bars(req).df.reset_index().sort_values("timestamp")

    closes = bars["close"].values
    first_open = bars.iloc[0]["open"]
    total_ret = (closes[-1] - first_open) / first_open

    # Equity curve as if we bought $1M worth on day 1
    shares = STARTING_CASH / first_open
    equity = pd.Series(closes * shares)
    running_max = equity.cummax()
    max_dd = ((equity - running_max) / running_max).min()

    daily_rets = pd.Series(closes).pct_change().dropna()
    sharpe = 0.0
    if len(daily_rets) > 1 and daily_rets.std() > 0:
        sharpe = (daily_rets.mean() / daily_rets.std()) * math.sqrt(252)

    return {
        "variant": f"{symbol} Buy & Hold",
        "return_pct": round(total_ret * 100, 3),
        "max_dd_pct": round(max_dd * 100, 3),
        "sharpe": round(sharpe, 3),
        "trades": 1,
        "final_equity": round(equity.iloc[-1], 2),
    }


def run_strategy(name: str, params: dict, label: str) -> dict:
    """Run a single strategy backtest."""
    provider = FileHistoryProvider(path=DATA_PATH)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()
    strategy = create_strategy(name, **params)
    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(feed=feed, broker=broker, strategy=strategy,
                    cash=STARTING_CASH, portfolio=portfolio)
    engine.run()

    activity = portfolio.activity()
    equity = activity.equity_curve()

    if equity.empty:
        return {"variant": label, "return_pct": 0, "max_dd_pct": 0,
                "sharpe": 0, "trades": 0, "final_equity": STARTING_CASH}

    total_ret = activity.total_return()
    running_max = equity.cummax()
    max_dd = ((equity - running_max) / running_max).min()
    rets = equity.pct_change().dropna()
    sharpe = 0.0
    if len(rets) > 1 and rets.std() > 0:
        sharpe = (rets.mean() / rets.std()) * math.sqrt(390 * 252)

    return {
        "variant": label,
        "return_pct": round(total_ret * 100, 3),
        "max_dd_pct": round(max_dd * 100, 3),
        "sharpe": round(sharpe, 3),
        "trades": len(strategy._trading_records),
        "final_equity": round(equity.iloc[-1], 2),
    }


def run_combined(label: str) -> dict:
    """Run both strategies together."""
    alpha_params = {**STRATEGY_PARAMS, **BACKTEST_OVERRIDES}
    vsb_params = {**VSB_PARAMS, **BACKTEST_OVERRIDES}

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

    if equity.empty:
        return {"variant": label, "return_pct": 0, "max_dd_pct": 0,
                "sharpe": 0, "trades": 0, "final_equity": STARTING_CASH}

    total_ret = activity.total_return()
    running_max = equity.cummax()
    max_dd = ((equity - running_max) / running_max).min()
    rets = equity.pct_change().dropna()
    sharpe = 0.0
    if len(rets) > 1 and rets.std() > 0:
        sharpe = (rets.mean() / rets.std()) * math.sqrt(390 * 252)

    trades = len(alpha._trading_records) + len(vsb._trading_records)
    return {
        "variant": label,
        "return_pct": round(total_ret * 100, 3),
        "max_dd_pct": round(max_dd * 100, 3),
        "sharpe": round(sharpe, 3),
        "trades": trades,
        "final_equity": round(equity.iloc[-1], 2),
    }


def main() -> None:
    results = []

    # Benchmarks
    print("Fetching benchmarks ...", flush=True)
    results.append(fetch_benchmark("SPY", "2025-04-01", "2025-06-30"))
    results.append(fetch_benchmark("TQQQ", "2025-04-01", "2025-06-30"))

    # Strategies
    alpha_params = {**STRATEGY_PARAMS, **BACKTEST_OVERRIDES}
    vsb_params = {**VSB_PARAMS, **BACKTEST_OVERRIDES}

    print("Running alpha_vwap ...", flush=True)
    results.append(run_strategy("alpha_vwap", alpha_params, "alpha_vwap"))

    print("Running VSB ...", flush=True)
    results.append(run_strategy("vsb", vsb_params, "VSB"))

    print("Running combined ...", flush=True)
    results.append(run_combined("alpha_vwap + VSB"))

    # Print comparison
    print("\n" + "=" * 80)
    print(f"  YTD BENCHMARK COMPARISON (Jan 2 - Mar 28, 2026) | Starting: ${STARTING_CASH:,.0f}")
    print("=" * 80)

    df = pd.DataFrame(results)
    df = df.rename(columns={
        "variant": "Strategy",
        "return_pct": "Return %",
        "max_dd_pct": "Max DD %",
        "sharpe": "Sharpe",
        "trades": "Trades",
        "final_equity": "Final Equity",
    })
    print(df.to_string(index=False))

    # Alpha vs benchmarks
    print("\n" + "-" * 80)
    spy_ret = results[0]["return_pct"]
    for r in results[2:]:
        alpha = r["return_pct"] - spy_ret
        print(f"  {r['variant']:20s} vs SPY: {alpha:+.2f}% alpha")


if __name__ == "__main__":
    main()
