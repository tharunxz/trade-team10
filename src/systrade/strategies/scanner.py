"""
Dynamic stock scanner for VSB strategy.

Scores universe symbols by volume, gap, volatility, and range.
Called synchronously from on_data() -- no threads or async.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from systrade.strategies.volume_surge_breakout import VSBSymbolState

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScannerWeights:
    rvol: float = 0.40
    gap: float = 0.25
    atr_pct: float = 0.20
    range: float = 0.15


@dataclass(frozen=True)
class SymbolScore:
    symbol: str
    rvol: float
    gap_pct: float
    atr_pct: float
    intraday_range: float
    composite: float


def _normalize(values: list[float]) -> list[float]:
    """Min-max normalize a list of values to [0, 1]."""
    lo, hi = min(values), max(values)
    span = hi - lo
    if span < 1e-12:
        return [0.0] * len(values)
    return [(v - lo) / span for v in values]


def score_universe(
    states: dict[str, VSBSymbolState],
    universe: tuple[str, ...],
    weights: ScannerWeights,
    min_rvol: float = 1.0,
) -> list[SymbolScore]:
    """Score all universe symbols, return sorted by composite descending.

    Symbols with fewer than 5 bars or RVOL below min_rvol are excluded.
    """
    raw: list[tuple[str, float, float, float, float]] = []

    for sym in universe:
        state = states.get(sym)
        if state is None or state.bar_count < 5:
            continue

        # RVOL: current volume vs rolling average
        if len(state.volume_history) >= 5:
            avg_vol = sum(state.volume_history) / len(state.volume_history)
            rvol = (state.volume_history[-1] / avg_vol) if avg_vol > 0 else 0.0
        else:
            continue

        if rvol < min_rvol:
            continue

        # ATR as % of price
        atr_pct = (
            (state.atr / state.prev_close * 100)
            if state.prev_close > 0 and state.atr > 0
            else 0.0
        )

        # Gap % (set during daily reset)
        gap_pct = abs(state.gap_pct)

        # Intraday range from opening range
        if state.or_complete and state.or_high > 0 and state.or_low < float("inf"):
            mid = (state.or_high + state.or_low) / 2
            range_pct = (state.or_high - state.or_low) / mid * 100 if mid > 0 else 0.0
        else:
            range_pct = 0.0

        raw.append((sym, rvol, gap_pct, atr_pct, range_pct))

    if not raw:
        return []

    # Min-max normalize each dimension
    n_rvol = _normalize([r[1] for r in raw])
    n_gap = _normalize([r[2] for r in raw])
    n_atr = _normalize([r[3] for r in raw])
    n_range = _normalize([r[4] for r in raw])

    scores: list[SymbolScore] = []
    for i, (sym, rvol, gap, atr, rng) in enumerate(raw):
        composite = (
            weights.rvol * n_rvol[i]
            + weights.gap * n_gap[i]
            + weights.atr_pct * n_atr[i]
            + weights.range * n_range[i]
        )
        scores.append(SymbolScore(sym, rvol, gap, atr, rng, composite))

    scores.sort(key=lambda s: s.composite, reverse=True)
    return scores


def select_active(
    scores: list[SymbolScore],
    count: int,
    protected: set[str],
) -> list[str]:
    """Pick top ``count`` symbols, always including ``protected`` symbols.

    Protected symbols (those with open positions) are never demoted,
    even if their score is below the cutoff.
    """
    selected: set[str] = set(protected)
    for s in scores:
        if len(selected) >= count:
            break
        selected.add(s.symbol)
    return sorted(selected)
