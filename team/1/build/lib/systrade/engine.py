from systrade.broker import Broker
from systrade.feed import Feed
from systrade.portfolio import Portfolio, PortfolioView
from systrade.strategy import Strategy


class Engine:
    """Orchestrator for the different components"""

    def __init__(
        self, feed: Feed, broker: Broker, strategy: Strategy, cash: float
    ) -> None:
        self._feed = feed
        self._broker = broker
        self._strategy = strategy
        self._stop_flag = False
        self._portfolio = Portfolio(cash)

    def run(self) -> None:
        """Run the strategy"""
        self._strategy.setup_context(
            self._feed.subscribe, self._broker.post_order, self._portfolio
        )

        self._feed.start()
        self._strategy.on_start()
        while self._feed.is_running():
            data = self._feed.next_data()
            self._strategy.current_time = data.as_of
            # Broker should get the data first in case an outstanding order has
            # a fill. That way the strategy will have the most recent view of
            # the portfolio.
            self._broker.on_data(data)
            exec_reports = self._broker.pop_latest()
            # Update portfolio with fill information and notify strategy that
            # fills have taken place.
            for report in exec_reports:
                self._portfolio.on_fill(
                    report.order.symbol,
                    price=report.last_price,
                    qty=report.last_quantity,
                )
                self._strategy.on_execution(report)

            self._portfolio.on_data(data)
            self._strategy.on_data(data)

    @property
    def portfolio(self) -> PortfolioView:
        return self._portfolio
