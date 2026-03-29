"""
Strategy registry — maps string names to strategy classes with default configs.

Usage:
    from systrade.strategies.registry import create_strategy, list_strategies

    strategy = create_strategy("alpha_vwap")
    strategy = create_strategy("alpha_vwap", leverage=1.0, entry_z=2.5)
"""

from __future__ import annotations

from typing import Any

from systrade.strategy import Strategy
from systrade.strategies.alpha_vwap import AlphaVWAPStrategy
from systrade.strategies.quant_vwap import QuantVWAPStrategy
from systrade.strategies.regime_adaptive import RegimeAdaptiveStrategy
from systrade.strategies.vwap_mean_reversion import VWAPMeanReversionStrategy


_REGISTRY: dict[str, type[Strategy]] = {
    "alpha_vwap": AlphaVWAPStrategy,
    "quant_vwap": QuantVWAPStrategy,
    "regime": RegimeAdaptiveStrategy,
    "vwap": VWAPMeanReversionStrategy,
}


def register(name: str, cls: type[Strategy]) -> None:
    """Register a new strategy class under the given name."""
    _REGISTRY[name] = cls


def create_strategy(name: str, **overrides: Any) -> Strategy:
    """Instantiate a strategy by name, passing any kwarg overrides to its constructor."""
    cls = _REGISTRY.get(name)
    if cls is None:
        available = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"Unknown strategy '{name}'. Available: {available}")
    return cls(**overrides)


def list_strategies() -> list[str]:
    """Return sorted list of registered strategy names."""
    return sorted(_REGISTRY)
