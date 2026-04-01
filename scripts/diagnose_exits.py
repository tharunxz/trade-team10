#!/usr/bin/env python3
"""
Diagnose alpha_vwap exit reasons.

Monkey-patches _close_position and _flatten_all to capture exit reason,
entry price, exit price, entry side, and bar count for every closed trade.
Then reports a breakdown by exit category.

Usage:
    set -a && source .env && set +a
    python scripts/diagnose_exits.py 2>&1 | grep -v "Model is not converging"
"""

from __future__ import annotations

import logging
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

# Silence noisy loggers before any other imports trigger them
logging.basicConfig(level=logging.WARNING)
for noisy in (
    "systrade", "systrade.strategies.alpha_vwap",
    "systrade.strategies.signal_processing", "hmmlearn",
):
    logging.getLogger(noisy).setLevel(logging.WARNING)

from systrade.broker import BacktestBroker
from systrade.config import (
    BACKTEST_OVERRIDES,
    STARTING_CASH,
    STRATEGY_PARAMS,
    make_backtest_strategy,
)
from systrade.engine import Engine
from systrade.feed import HistoricalFeed
from systrade.history import FileHistoryProvider
from systrade.portfolio import Portfolio

# ── Trade record storage ─────────────────────────────────────────────

EXIT_LOG: list[dict] = []


def _categorize_reason(raw: str) -> str:
    """Map the raw reason string to a bucket."""
    r = raw.upper()
    if "MAX LOSS" in r:
        return "MAX_LOSS"
    if "HARD STOP" in r:
        return "HARD_STOP"
    if "MR TRAIL EXIT" in r:
        return "MR_TRAIL_EXIT"
    if "TRAIL STOP" in r:
        return "TRAIL_STOP"
    if "EOD" in r:
        return "EOD_FLATTEN"
    return "OTHER"


# ── Monkey-patch factory ─────────────────────────────────────────────

def _patch_strategy(strategy) -> None:
    """Instrument the strategy to capture exit metadata."""
    original_close = strategy._close_position.__func__
    original_flatten = strategy._flatten_all.__func__

    def patched_close(self, sym: str, reason: str) -> None:
        if not self.portfolio.is_invested_in(sym):
            return

        state = self._states.get(sym)
        pos = self.portfolio.position(sym)
        exit_price = state.prices[-1] if (state and state.prices) else 0.0
        entry_price = state.entry_price if (state and state.entry_price) else 0.0
        entry_side = state.entry_side if state else ""
        bar_count = state.bar_count if state else 0

        # Compute P&L percentage
        pnl_pct = 0.0
        if entry_price > 0 and exit_price > 0:
            if entry_side == "long":
                pnl_pct = (exit_price - entry_price) / entry_price
            elif entry_side == "short":
                pnl_pct = (entry_price - exit_price) / entry_price

        EXIT_LOG.append({
            "symbol": sym,
            "reason_raw": reason,
            "category": _categorize_reason(reason),
            "entry_side": entry_side,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl_pct": pnl_pct,
            "qty": pos.qty,
            "bar_count": bar_count,
            "timestamp": str(self.current_time) if hasattr(self, "_current_time") else "",
        })

        # Call original
        original_close(self, sym, reason)

    def patched_flatten(self, reason: str) -> None:
        # _flatten_all calls _close_position per symbol, which is already patched
        original_flatten(self, reason)

    import types
    strategy._close_position = types.MethodType(patched_close, strategy)
    strategy._flatten_all = types.MethodType(patched_flatten, strategy)


# ── Run backtest ─────────────────────────────────────────────────────

def run_diagnosed_backtest(data_path: str) -> tuple[list[dict], dict]:
    """Run the backtest with patched strategy. Returns (exit_log, metrics)."""
    provider = FileHistoryProvider(path=data_path)
    feed = HistoricalFeed(provider=provider)
    broker = BacktestBroker()
    strategy = make_backtest_strategy()

    # Patch BEFORE engine.run() but AFTER strategy is constructed
    _patch_strategy(strategy)

    portfolio = Portfolio(cash=STARTING_CASH, broker=broker)
    engine = Engine(
        feed=feed, broker=broker, strategy=strategy,
        cash=STARTING_CASH, portfolio=portfolio,
    )
    engine.run()

    # Compute high-level metrics
    activity = portfolio.activity()
    df = activity.df()
    total_return = activity.total_return() if not df.empty else 0.0
    equity = activity.equity_curve() if not df.empty else None
    max_dd = 0.0
    if equity is not None and len(equity) > 1:
        running_max = equity.cummax()
        drawdown = (equity - running_max) / running_max
        max_dd = drawdown.min()
    trades = len(strategy._trading_records)

    metrics = {
        "total_return_pct": round(total_return * 100, 3),
        "max_drawdown_pct": round(max_dd * 100, 3),
        "total_fills": trades,
    }
    return EXIT_LOG, metrics


# ── Report ───────────────────────────────────────────────────────────

def print_report(exits: list[dict], metrics: dict) -> None:
    """Print the diagnostic breakdown."""
    sep = "=" * 72

    print(f"\n{sep}")
    print("  ALPHA_VWAP EXIT DIAGNOSTICS")
    print(sep)
    print(f"  Total return:   {metrics['total_return_pct']:+.3f}%")
    print(f"  Max drawdown:   {metrics['max_drawdown_pct']:.3f}%")
    print(f"  Total fills:    {metrics['total_fills']}")
    print(f"  Total exits:    {len(exits)}")
    print(sep)

    if not exits:
        print("  No exits recorded.")
        return

    # Group by category
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for e in exits:
        by_cat[e["category"]].append(e)

    # Sort categories by count descending
    sorted_cats = sorted(by_cat.items(), key=lambda kv: -len(kv[1]))

    # Table header
    header = (
        f"  {'Category':<16} {'Count':>6} {'Avg PnL%':>10} {'Med PnL%':>10} "
        f"{'Win Rate':>10} {'Avg Bars':>10} {'Tot$ PnL':>12}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    total_pnl_all = 0.0

    for cat, trades in sorted_cats:
        count = len(trades)
        pnls = [t["pnl_pct"] for t in trades]
        bars = [t["bar_count"] for t in trades]
        wins = sum(1 for p in pnls if p > 0)
        avg_pnl = sum(pnls) / count
        sorted_pnls = sorted(pnls)
        med_pnl = sorted_pnls[count // 2] if count % 2 == 1 else (
            (sorted_pnls[count // 2 - 1] + sorted_pnls[count // 2]) / 2
        )
        avg_bars = sum(bars) / count
        win_rate = wins / count * 100

        # Approximate dollar P&L: sum(pnl_pct * entry_price * abs(qty))
        dollar_pnl = sum(
            t["pnl_pct"] * t["entry_price"] * abs(t["qty"]) for t in trades
        )
        total_pnl_all += dollar_pnl

        print(
            f"  {cat:<16} {count:>6} {avg_pnl * 100:>+10.3f}% {med_pnl * 100:>+10.3f}% "
            f"{win_rate:>9.1f}% {avg_bars:>10.0f} {dollar_pnl:>+12,.0f}"
        )

    print("  " + "-" * (len(header) - 2))
    total_exits = len(exits)
    all_pnls = [t["pnl_pct"] for t in exits]
    all_bars = [t["bar_count"] for t in exits]
    all_wins = sum(1 for p in all_pnls if p > 0)
    avg_all = sum(all_pnls) / total_exits
    sorted_all = sorted(all_pnls)
    med_all = sorted_all[total_exits // 2] if total_exits % 2 == 1 else (
        (sorted_all[total_exits // 2 - 1] + sorted_all[total_exits // 2]) / 2
    )
    print(
        f"  {'TOTAL':<16} {total_exits:>6} {avg_all * 100:>+10.3f}% {med_all * 100:>+10.3f}% "
        f"{all_wins / total_exits * 100:>9.1f}% {sum(all_bars) / total_exits:>10.0f} "
        f"{total_pnl_all:>+12,.0f}"
    )

    # ── Per-side breakdown ───────────────────────────────────────────
    print(f"\n{sep}")
    print("  BY ENTRY SIDE")
    print(sep)
    for side in ("long", "short"):
        side_trades = [t for t in exits if t["entry_side"] == side]
        if not side_trades:
            continue
        sc = len(side_trades)
        sp = [t["pnl_pct"] for t in side_trades]
        sw = sum(1 for p in sp if p > 0)
        sd = sum(t["pnl_pct"] * t["entry_price"] * abs(t["qty"]) for t in side_trades)
        print(
            f"  {side.upper():<8}: {sc:>4} exits, "
            f"avg={sum(sp) / sc * 100:+.3f}%, "
            f"win={sw / sc * 100:.1f}%, "
            f"${sd:+,.0f}"
        )

    # ── Worst individual trades ──────────────────────────────────────
    print(f"\n{sep}")
    print("  WORST 10 TRADES (by PnL %)")
    print(sep)
    worst = sorted(exits, key=lambda t: t["pnl_pct"])[:10]
    for t in worst:
        print(
            f"  {t['symbol']:<6} {t['entry_side']:<6} "
            f"entry=${t['entry_price']:.2f} exit=${t['exit_price']:.2f} "
            f"pnl={t['pnl_pct'] * 100:+.3f}% bars={t['bar_count']} "
            f"reason={t['reason_raw']}"
        )

    # ── Best individual trades ───────────────────────────────────────
    print(f"\n{sep}")
    print("  BEST 10 TRADES (by PnL %)")
    print(sep)
    best = sorted(exits, key=lambda t: t["pnl_pct"], reverse=True)[:10]
    for t in best:
        print(
            f"  {t['symbol']:<6} {t['entry_side']:<6} "
            f"entry=${t['entry_price']:.2f} exit=${t['exit_price']:.2f} "
            f"pnl={t['pnl_pct'] * 100:+.3f}% bars={t['bar_count']} "
            f"reason={t['reason_raw']}"
        )

    # ── Config params for reference ──────────────────────────────────
    merged = {**STRATEGY_PARAMS, **BACKTEST_OVERRIDES}
    print(f"\n{sep}")
    print("  CONFIG PARAMS")
    print(sep)
    for k in sorted(merged):
        v = merged[k]
        if not isinstance(v, (tuple, list, set, frozenset)):
            print(f"  {k:<25} = {v}")
    print(sep)


# ── Main ─────────────────────────────────────────────────────────────

def main() -> None:
    data_path = "data/history_1min.csv"
    if not Path(data_path).exists():
        print(f"Error: data file not found: {data_path}")
        sys.exit(1)

    print("Running alpha_vwap backtest with exit diagnostics ...")
    print(f"  Data:  {data_path}")
    print(f"  Cash:  ${STARTING_CASH:,.0f}")
    print(f"  Config params + backtest overrides applied.")

    exits, metrics = run_diagnosed_backtest(data_path)
    print_report(exits, metrics)


if __name__ == "__main__":
    main()
