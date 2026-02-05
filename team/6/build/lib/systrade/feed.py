from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, override
from zoneinfo import ZoneInfo

import pandas as pd

from systrade.data import Bar, BarData


class Feed(ABC):
    @abstractmethod
    def start(self) -> None:
        """Start streaming"""

    @abstractmethod
    def stop(self) -> None:
        """Stop streaming"""

    @abstractmethod
    def is_running(self) -> bool:
        """Whether feed is currently running"""

    @abstractmethod
    def subscribe(self, symbol: str) -> None:
        """Subscribe to a symbol"""

    @abstractmethod
    def next_data(self) -> BarData:
        """Block until returning the next available data for subscribed
        symbols"""


class FileFeed(Feed):
    def __init__(
        self, path: str | Path, start: Optional[str] = None, end: Optional[str] = None
    ) -> None:
        """File feed initializer

        Parameters
        ----------
        path
            Full path to data file
        start, optional
            When to start the replay, in YYYY-MM-DD format
        end, optional
            When to end the replay, in YYYY-MM-DD format
        """
        self._path = path
        self._data = pd.DataFrame()
        self._available_symbols = set()
        self._subscribed_symbols = BarData()
        self._timestamp_iter = None
        self._current_ts = pd.Timestamp(year=1970, month=1, day=1)
        self._is_running = False
        self._start = start
        self._end = end

    @property
    def df(self) -> pd.DataFrame:
        return self._data

    @override
    def start(self) -> None:
        df = pd.read_csv(self._path)
        df["Date"] = df["Date"].astype(pd.DatetimeTZDtype(tz="America/New_York"))
        df["Symbol"] = df["Symbol"].astype(pd.StringDtype(storage="python"))
        if self._start is not None:
            df = df.loc[df["Date"] >= self._start]
        if self._end is not None:
            df = df.loc[df["Date"] <= self._end]

        self._data = df.set_index(["Symbol", "Date"]).sort_index()
        self._available_symbols = set(self._data.index.get_level_values(0).unique())
        self._timestamp_iter = iter(self._data.index.get_level_values(1).unique())
        self._is_running = True
        try:
            self._current_ts = next(self._timestamp_iter)
            self._is_running = True
        except StopIteration:
            self._is_running = False

    @override
    def stop(self) -> None:
        self._is_running = False

    @override
    def is_running(self) -> bool:
        return self._is_running

    @override
    def subscribe(self, symbol: str) -> None:
        if symbol not in self._available_symbols:
            raise ValueError(f"Symbol {symbol} not available!")
        self._subscribed_symbols[symbol] = Bar()

    @override
    def next_data(self) -> BarData:
        ts = self._current_ts
        result = BarData(ts.to_pydatetime())
        for symbol in self._subscribed_symbols.symbols():
            data: pd.Series = self._data.loc[(symbol, ts)]  # type: ignore
            if not isinstance(data, pd.Series):
                raise ValueError(f"Non unique ({symbol}, {ts})")
            bar = Bar(
                open=data["Open"],
                high=data["High"],
                low=data["Low"],
                close=data["Close"],
                volume=data["Volume"],
            )
            result[symbol] = bar
        try:
            self._current_ts = next(self._timestamp_iter)  # type: ignore
        except StopIteration:
            self._is_running = False

        return result
