from abc import ABC, abstractmethod
from collections import defaultdict
from typing import override

from systrade.data import BarData, ExecutionReport, Order


class Broker(ABC):
    @abstractmethod
    def on_data(self, data: BarData) -> None:
        """Handle data updates"""

    @abstractmethod
    def post_order(self, order: Order) -> None:
        """Post an order to the broker"""

    @abstractmethod
    def pop_latest(self) -> list[ExecutionReport]:
        """Pop latest execution reports, will return an empty list if none"""


class BacktestBroker(Broker):
    """A test broker to simulate order communication"""

    def __init__(self) -> None:
        self._orders = defaultdict[str, list[Order]](lambda: [])
        self._exec_reports = list[ExecutionReport]()
        self._last_data = BarData()

    @override
    def on_data(self, data: BarData) -> None:
        # We assume we trade at the close but won't be able to get filled until
        # the open. Since we currently only support market orders, any orders
        # posted will get filled right away.
        self._last_data = data
        for symbol, bar in data.bars():
            open_orders = self._orders.get(symbol)
            if open_orders:
                for order in open_orders:
                    fill = ExecutionReport(
                        order=order,
                        last_price=bar.open,
                        last_quantity=order.quantity,
                        cum_quantity=order.quantity,
                        rem_quantity=0.0,
                        fill_timestamp=data.as_of,
                    )
                    self._exec_reports.append(fill)
                # All filled
                open_orders.clear()

    @override
    def post_order(self, order: Order) -> None:
        # Save order to get filled on the next bar
        self._orders[order.symbol].append(order)

    @override
    def pop_latest(self) -> list[ExecutionReport]:
        reports = self._exec_reports.copy()
        self._exec_reports.clear()
        return reports

## TO DO: make it work
class AlpacaBroker(Broker):
    """A broker to communicate with Alpaca API"""

    def __init__(self) -> None:
        self._orders = defaultdict[str, list[Order]](lambda: [])
        self._exec_reports = list[ExecutionReport]()
        self._last_data = BarData()

    @override
    def on_data(self, data: BarData) -> None:
        # We assume we trade at the close but won't be able to get filled until
        # the open. Since we currently only support market orders, any orders
        # posted will get filled right away.
        self._last_data = data
        for symbol, bar in data.bars():
            open_orders = self._orders.get(symbol)
            if open_orders:
                for order in open_orders:
                    fill = ExecutionReport(
                        order=order,
                        last_price=bar.open,
                        last_quantity=order.quantity,
                        cum_quantity=order.quantity,
                        rem_quantity=0.0,
                        fill_timestamp=data.as_of,
                    )
                    self._exec_reports.append(fill)
                # All filled
                open_orders.clear()

    @override
    def post_order(self, order: Order) -> None:
        # Save order to get filled on the next bar
        self._orders[order.symbol].append(order)

    @override
    def pop_latest(self) -> list[ExecutionReport]:
        reports = self._exec_reports.copy()
        self._exec_reports.clear()
        return reports