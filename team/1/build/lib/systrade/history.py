from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, override

import pandas as pd

from systrade.data import BarData
from systrade.position import Position

# add Path so HistoryProvider.load
#+can accept a path as an argument
#+in order to read a file to load
#+the data
from pathlib import Path

# Refernce portfolio.py
class HistoryProvider(ABC):
    """Historical data loader"""

    @abstractmethod
    def load(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        symbols: Optional[list[str]] = None,
        columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Load historical data from storage, returning a DataFrame with schema
        corresponding to systrade.data.Bar.
        """

# TO DO
class FileHistoryProvider(HistoryProvider):
    def __init__(
            self, 
            path: str | Path, 
            start: Optional[datetime] = None, 
            end: Optional[datetime] = None,
            ) -> None:
        """File History Provider Initializer 

        Parameters
        ----------

        path
            Full path to data file
        start, optional
            Beginning of desired data timeframe, in YYYY-MM-DD format
        end, optional
            End of desired data timeframe
            By default, will pull most recent, in YYYY-MM-DD format
        """
        self._path = path # tells you where the file is stored
        self._data = pd.DataFrame()
        self._start = start
        self._end = end

    @property
    def df(self) -> pd.DataFrame:
        df = pd.read_csv(self._path)
        
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
        self._columns = df["Symbols"].unique()
        self._is_running = True
        try:
            self._current_ts = next(self._timestamp_iter)
            self._is_running = True
        except StopIteration:
            self._is_running = False


# TO DO
class QuestDBHistoryProvider(HistoryProvider):
    def __init__(
            self, 
            path: str | Path, 
            start: Optional[datetime] = None, 
            end: Optional[datetime] = None,
            ) -> None:
        """File History Provider Initializer 

        Parameters
        ----------

        path
            Full path to data file
        start, optional
            Beginning of desired data timeframe, in YYYY-MM-DD format
        end, optional
            End of desired data timeframe
            By default, will pull most recent, in YYYY-MM-DD format
        """
        self._path = path # tells you where the file is stored
        self._data = pd.DataFrame()
        self._start = start
        self._end = end

    @property
    def df(self) -> pd.DataFrame:
        df = pd.read_csv(self._path)
        
        return self._data