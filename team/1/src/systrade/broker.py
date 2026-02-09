from abc import ABC, abstractmethod
from collections import defaultdict
import os
from typing import override, Optional, List

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide as AlpacaOrderSideEnum, TimeInForce, QueryOrderStatus
from alpaca.trading.models import Order as AlpacaOrderModel

from systrade.data import BarData, ExecutionReport, Order
# ---------------------------
# --------- LOGGING ---------
# ----- logging imports -----
import logging.config
import logging.handlers
import json
import pathlib
# instantiate logger
logger = logging.getLogger(__name__)

# --- LOGGER CONFIG ---
# Verbose dictionary-type config
#+for custom logger.
# Config file found in:
# /config/logger/config.json
# (source: youtube.com/mCoding)
def setup_logging():
    config_file = pathlib.Path("config/logger/config.json")
    with open(config_file) as f_in:
        config = json.load(f_in)
    logging.config.dictConfig(config)
# initialize escape codes for
#+color-coding logs
red = "\033[31m"
green = "\033[32m"
yellow = "\033[33m"
blue = "\033[34m"
hl_red = "\033[41m"
hl_green = "\033[42m"
hl_yellow = "\033[43m"
hl_blue = "\033[44m"
reset = "\033[0m"
# ---------------------


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
                open_orders.clear()

    @override
    def post_order(self, order: Order) -> None:
        self._orders[order.symbol].append(order)

    @override
    def pop_latest(self) -> list[ExecutionReport]:
        reports = self._exec_reports.copy()
        self._exec_reports.clear()
        return reports


class AlpacaBroker(Broker):
    """A broker to communicate with Alpaca API for live/paper trading."""

    def __init__(self) -> None:
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET")
        paper_trading = os.getenv("ALPACA_PAPER", "True").lower() == "true" 

        if not api_key or not secret_key:
            raise ValueError("ALPACA_API_KEY and ALPACA_API_SECRET environment variables must be set.")

        self.trading_client = TradingClient(api_key, secret_key, paper=paper_trading)
        
        self._pending_orders: dict[str, Order] = {}
        self._exec_reports: list[ExecutionReport] = []

    def get_account_details(self):
        """Helper method for LivePortfolioView to fetch account details."""
        return self.trading_client.get_account()

    @override
    def on_data(self, data: BarData) -> None:
        """Poll Alpaca for order updates when new data arrives."""
        if not self._pending_orders:
            return

        request_params = GetOrdersRequest(
            status=QueryOrderStatus.CLOSED,
            limit=100,
            nested=True
        )
        closed_orders: List[AlpacaOrderModel] = self.trading_client.get_orders(request_params)

        for alpaca_order in closed_orders:
            client_order_id = alpaca_order.client_order_id
            if client_order_id in self._pending_orders:
                original_systrade_order = self._pending_orders.pop(client_order_id)
                
                if alpaca_order.filled_avg_price is not None and alpaca_order.filled_qty is not None:
                    fill = ExecutionReport(
                        order=original_systrade_order,
                        last_price=float(alpaca_order.filled_avg_price),
                        last_quantity=float(alpaca_order.filled_qty),
                        cum_quantity=float(alpaca_order.filled_qty),
                        rem_quantity=0.0,
                        fill_timestamp=alpaca_order.updated_at or alpaca_order.created_at,
                    )
                    self._exec_reports.append(fill)

    @override
    def post_order(self, order: Order) -> None:
        """
        Post an order to Alpaca, handling long and short positions safely
        with margin/buying power checks.
        Positive quantity -> BUY (long or cover short)
        Negative quantity -> SELL (short or close long)
        """
        account = self.trading_client.get_account()
        cash = float(account.cash)
        buying_power = float(account.buying_power)  # includes margin
        current_price = order.price if order.price else 0.0  # fallback

        # Determine side and quantity
        if order.quantity == 0:
            logger.info(f"Skipping zero-quantity order: {order}")
            return

        if order.quantity > 0:
            alpaca_side = AlpacaOrderSideEnum.BUY
            qty_to_place = order.quantity
            max_affordable = math.floor(buying_power / current_price * 0.95) if current_price > 0 else order.quantity
            if qty_to_place > max_affordable:
                logger.warning(f"Adjusting BUY order {order.symbol} from {qty_to_place} to {max_affordable} shares due to buying power")
                qty_to_place = max_affordable
        else:
            alpaca_side = AlpacaOrderSideEnum.SELL
            qty_to_place = abs(order.quantity)
            # For shorting, max shortable depends on margin
            max_shortable = math.floor(buying_power / current_price * 0.95) if current_price > 0 else qty_to_place
            if qty_to_place > max_shortable:
                logger.warning(f"Adjusting SHORT order {order.symbol} from {qty_to_place} to {max_shortable} shares due to margin")
                qty_to_place = max_shortable

        if qty_to_place <= 0:
            logger.error(f"Order quantity for {order.symbol} reduced to zero, skipping order")
            return

        market_order_request = MarketOrderRequest(
            symbol=order.symbol,
            qty=qty_to_place,
            side=alpaca_side,
            time_in_force=TimeInForce.GTC,
            client_order_id=order.id
        )

        try:
            self.trading_client.submit_order(market_order_request)
            self._pending_orders[order.id] = order
            logger.info(f"Submitted {alpaca_side.value} order for {qty_to_place} shares of {order.symbol}")
        except Exception as e:
            logger.error(f"{red}Error submitting order for {order.symbol}: {e}{reset}")

    @override
    def pop_latest(self) -> list[ExecutionReport]:
        reports = self._exec_reports.copy()
        self._exec_reports.clear()
        return reports
