#!/usr/bin/env python3
"""
Validate HMM regime detection accuracy.

For each day and symbol, compute:
1. What regime the HMM detected at each bar
2. What actually happened (realized vol, directional move)
3. Whether the regime label was correct

A good regime detector should:
- Label low-vol oscillating periods as MEAN_REVERTING
- Label high-vol directional moves as TRENDING
- Label high-vol non-directional chaos as VOLATILE
- Have labels that are PREDICTIVE (next N bars behave like the label), not just descriptive
"""

import math
from collections import defaultdict

import numpy as np
import pandas as pd

from systrade.strategies.signal_processing import HMMRegimeDetector, MarketRegime

DATA_PATH = "data/history_ytd.csv"
SYMBOLS = ["TQQQ", "SOXL", "TNA", "SQQQ", "UDOW"]


def load_bars(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["Date"] = pd.to_datetime(df["Date"], utc=True)
    return df


def realized_regime(returns: list[float]) -> str:
    """Classify what ACTUALLY happened in a window of returns."""
    if len(returns) < 5:
        return "UNKNOWN"
    vol = np.std(returns)
    drift = abs(np.mean(returns))
    # Directional ratio: |mean| / std — high means trending
    ratio = drift / vol if vol > 1e-9 else 0

    # Thresholds calibrated for 1-min leveraged ETF returns
    if vol < 0.001 and ratio < 0.3:
        return "MEAN_REVERTING"
    elif ratio > 0.3:
        return "TRENDING"
    elif vol > 0.002:
        return "VOLATILE"
    else:
        return "MEAN_REVERTING"


def main() -> None:
    df = load_bars(DATA_PATH)

    # Track regime detection vs reality
    predictions = []  # (hmm_regime, actual_regime, confidence, forward_return)

    for sym in SYMBOLS:
        sym_bars = df[df["Symbol"] == sym].sort_values("Date").reset_index(drop=True)
        if sym_bars.empty:
            continue

        # Group by date
        sym_bars["day"] = sym_bars["Date"].dt.date
        days = sym_bars.groupby("day")

        for day, day_bars in days:
            day_bars = day_bars.reset_index(drop=True)
            if len(day_bars) < 150:  # need enough bars for HMM warmup
                continue

            hmm = HMMRegimeDetector(lookback=120, refit_interval=30)
            prices = day_bars["Close"].values
            volumes = day_bars["Volume"].values

            for i in range(len(prices)):
                estimate = hmm.update(float(prices[i]), float(volumes[i]))

                # Need forward-looking window to check prediction accuracy
                if i < 120 or i + 30 >= len(prices):
                    continue

                # What the HMM says NOW
                hmm_regime = estimate.regime.name
                confidence = estimate.confidence

                # What ACTUALLY happens in the next 30 bars
                forward_prices = prices[i:i + 30]
                forward_rets = [
                    (forward_prices[j + 1] - forward_prices[j]) / forward_prices[j]
                    for j in range(len(forward_prices) - 1)
                ]
                actual = realized_regime(forward_rets)

                # Forward return (for measuring P&L impact of regime call)
                fwd_ret = (prices[i + 30] - prices[i]) / prices[i]

                predictions.append({
                    "symbol": sym,
                    "day": str(day),
                    "bar": i,
                    "hmm_regime": hmm_regime,
                    "actual_regime": actual,
                    "confidence": confidence,
                    "correct": hmm_regime == actual,
                    "fwd_return_30bar": fwd_ret,
                })

    pdf = pd.DataFrame(predictions)

    # Overall accuracy
    total = len(pdf)
    correct = pdf["correct"].sum()
    print("=" * 70)
    print(f"REGIME DETECTION VALIDATION ({total:,} predictions)")
    print("=" * 70)
    print(f"\nOverall accuracy: {correct}/{total} = {correct/total*100:.1f}%")

    # Confusion matrix
    print("\n--- Confusion Matrix (HMM predicted vs Actual) ---")
    regimes = ["MEAN_REVERTING", "TRENDING", "VOLATILE", "UNKNOWN"]
    ct = pd.crosstab(pdf["hmm_regime"], pdf["actual_regime"], margins=True)
    print(ct.to_string())

    # Per-regime accuracy
    print("\n--- Per-Regime Accuracy ---")
    for regime in regimes:
        subset = pdf[pdf["hmm_regime"] == regime]
        if subset.empty:
            continue
        acc = subset["correct"].mean() * 100
        avg_conf = subset["confidence"].mean()
        avg_fwd = subset["fwd_return_30bar"].mean() * 100
        n = len(subset)
        print(f"  {regime:18s}: accuracy={acc:5.1f}%  avg_conf={avg_conf:.2f}  "
              f"avg_fwd_ret={avg_fwd:+.3f}%  n={n}")

    # High-confidence predictions only
    print("\n--- High Confidence (>0.7) ---")
    hi = pdf[pdf["confidence"] > 0.7]
    if not hi.empty:
        for regime in regimes:
            subset = hi[hi["hmm_regime"] == regime]
            if subset.empty:
                continue
            acc = subset["correct"].mean() * 100
            n = len(subset)
            avg_fwd = subset["fwd_return_30bar"].mean() * 100
            print(f"  {regime:18s}: accuracy={acc:5.1f}%  "
                  f"avg_fwd_ret={avg_fwd:+.3f}%  n={n}")
    else:
        print("  No high-confidence predictions.")

    # The critical question: when HMM says TRENDING, is it right?
    print("\n--- TRENDING Detection (the key question) ---")
    trending_calls = pdf[pdf["hmm_regime"] == "TRENDING"]
    if not trending_calls.empty:
        actually_trending = trending_calls[trending_calls["actual_regime"] == "TRENDING"]
        actually_mr = trending_calls[trending_calls["actual_regime"] == "MEAN_REVERTING"]
        print(f"  HMM called TRENDING {len(trending_calls)} times")
        print(f"    Actually trending: {len(actually_trending)} ({len(actually_trending)/len(trending_calls)*100:.1f}%)")
        print(f"    Actually MR:       {len(actually_mr)} ({len(actually_mr)/len(trending_calls)*100:.1f}%)")
        print(f"    Avg fwd return when HMM says TRENDING: {trending_calls['fwd_return_30bar'].mean()*100:+.4f}%")
        print(f"    Avg |fwd return| (directional strength): {trending_calls['fwd_return_30bar'].abs().mean()*100:.4f}%")

    # Regime stability: how often does it flip?
    print("\n--- Regime Stability ---")
    for sym in SYMBOLS:
        sym_preds = pdf[pdf["symbol"] == sym].sort_values(["day", "bar"])
        if sym_preds.empty:
            continue
        flips = (sym_preds["hmm_regime"] != sym_preds["hmm_regime"].shift()).sum()
        total_bars = len(sym_preds)
        flip_rate = flips / total_bars * 100
        print(f"  {sym}: {flips} regime flips / {total_bars} bars = {flip_rate:.1f}% flip rate")

    # Regime distribution
    print("\n--- Regime Distribution ---")
    dist = pdf["hmm_regime"].value_counts(normalize=True) * 100
    for regime, pct in dist.items():
        print(f"  {regime:18s}: {pct:5.1f}%")


if __name__ == "__main__":
    main()
