from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, override

import pandas as pd

from systrade.data import BarData
from systrade.position import Position


class PortfolioActivity:
    """History of portfolio activity along with metrics"""

    def __init__(self, records: list[dict]) -> None:
        self._records = records
        self._df = pd.DataFrame.from_records(records)

    def total_return(self) -> float:
        """Total return on portfolio"""
        return self._df["value"].iloc[-1] / self._df["value"].iloc[0] - 1

    def equity_curve(self) -> pd.Series:
        """The total value of the portfolio at each point in time"""
        return self._df["value"]

    def df(self, condensed=True) -> pd.DataFrame:
        """Return all portfolio activity. If condensed, will keep individual
        position information packed in lists"""
        if condensed:
            result = self._df.copy()
        else:
            result = self._df.explode(
                ["symbols", "quantities", "prices", "asset_values"]
            )
        return result


class PortfolioView(ABC):
    """A read-only portfolio view"""

    @abstractmethod
    def cash(self) -> float:
        """Current cash balance"""

    @abstractmethod
    def asset_value(self) -> float:
        """Current market value of all assets"""

    @abstractmethod
    def asset_value_of(self, symbol: str) -> float:
        """Current market value of symbol"""

    @abstractmethod
    def value(self) -> float:
        """Current market value of assets + cash"""

    @abstractmethod
    def as_of(self) -> datetime:
        """As of timestamp"""

    @abstractmethod
    def is_invested(self) -> bool:
        """Whether portfolio currently has asset exposure"""

    @abstractmethod
    def is_invested_in(self, symbol: str) -> bool:
        """Whether portfolio is currently invested in symbol"""

    @abstractmethod
    def position(self, symbol) -> Position:
        """Return position in symbol"""

    @abstractmethod
    def activity(self) -> PortfolioActivity:
        """Return portfolio activity"""


class Portfolio(PortfolioView):
    _zero_tolerance = 1e-8

    def __init__(
        self,
        cash: float,
        current_positions: Optional[dict[str, Position]] = None,
        current_prices: Optional[BarData] = None,
    ) -> None:
        self._cash = cash
        self._current_positions = current_positions or {}
        self._current_prices = (
            current_prices if current_prices is not None else BarData()
        )
        self._portfolio_activity = list[dict]()

    @override
    def cash(self) -> float:
        return self._cash

    @override
    def asset_value(self) -> float:
        total = 0
        if len(self._current_positions) > 0:
            for sym, pos in self._current_positions.items():
                bar = self._current_prices.get(sym)
                if bar is None:
                    raise RuntimeError(f"No last price data for symbol sym={sym}")
                total += pos.value(bar.close)
        return total

    @override
    def asset_value_of(self, symbol: str) -> float:
        position = self._current_positions.get(symbol)
        if position is None:
            raise ValueError(f"Not invested in {symbol}")
        bar = self._current_prices.get(symbol)
        if bar is None:
            raise RuntimeError(f"No last price data for symbol sym={symbol}")
        return position.value(bar.close)

    @override
    def value(self) -> float:
        return self._cash + self.asset_value()

    @override
    def as_of(self) -> datetime:
        return self._current_prices.as_of

    @override
    def is_invested(self) -> bool:
        return bool(self._current_positions.keys())

    @override
    def is_invested_in(self, symbol: str) -> bool:
        return symbol in self._current_positions

    @override
    def position(self, symbol) -> Position:
        if not self.is_invested_in(symbol):
            raise ValueError(f"Not invested in {symbol}")
        return self._current_positions[symbol]

    @override
    def activity(self) -> PortfolioActivity:
        return PortfolioActivity(self._portfolio_activity)

    def on_data(self, data: BarData) -> None:
        """Cache latest data to use in calculating latest values"""
        self._current_prices = data
        symbols = list(self._current_positions.keys())
        positions = list(self._current_positions.values())
        record = {}
        record["timestamp"] = self.as_of()
        record["cash"] = self.cash()
        record["symbols"] = symbols
        record["quantities"] = [p.qty for p in positions]
        record["prices"] = [data[sym].close for sym in symbols]
        record["asset_values"] = [self.asset_value_of(sym) for sym in symbols]
        record["asset_value"] = self.asset_value()
        record["value"] = self.value()
        self._portfolio_activity.append(record)

    def on_fill(self, symbol: str, price: float, qty: float) -> None:
        """Update portfolio with a fill information (negative qty indicates
        sell). If a fill takes the quantity down to 0 (within tolerance) it
        should be removed from tracking"""
        pos = self._current_positions.get(symbol)
        if pos is None:
            self._current_positions[symbol] = Position(symbol, qty)
        else:
            pos.qty += qty
            if abs(pos.qty) <= self._zero_tolerance:
                del self._current_positions[symbol]

        self._cash -= qty * price
