import logging
import time as _time

from systrade.broker import Broker, AlpacaBroker
from systrade.feed import Feed
from systrade.portfolio import Portfolio, PortfolioView, LivePortfolioView
from systrade.strategy import Strategy

logger = logging.getLogger(__name__)

class Engine:
    """Orchestrator for the different components"""

    def __init__(
        self,
        feed: Feed,
        broker: Broker,
        strategy: Strategy,
        cash: float,
        portfolio: PortfolioView | None = None,
    ) -> None:
        self._feed = feed
        self._broker = broker
        self._strategy = strategy
        self._stop_flag = False
        if portfolio is not None:
            self._portfolio = portfolio
        elif isinstance(broker, AlpacaBroker):
            self._portfolio = LivePortfolioView(broker=broker)
        else:
            self._portfolio = Portfolio(cash=cash, broker=broker)

    def run(self) -> None:
        """Run the strategy"""
        self._strategy.setup_context(
            self._feed.subscribe, self._broker.post_order, self._portfolio
        )

        self._feed.start()
        self._strategy.on_start()
        _consecutive_errors = 0
        while self._feed.is_running():
            try:
                data = self._feed.next_data()
                self._strategy.current_time = data.as_of
                self._broker.on_data(data)
                exec_reports = self._broker.pop_latest()
                for report in exec_reports:
                    self._portfolio.on_fill(
                        report.order.symbol,
                        price=report.last_price,
                        qty=report.last_quantity,
                    )
                    self._strategy.on_execution(report)

                self._portfolio.on_data(data)
                self._strategy.on_data(data)
                _consecutive_errors = 0
            except KeyboardInterrupt:
                raise
            except Exception as e:
                _consecutive_errors += 1
                logger.error("Engine tick error (#%d): %s", _consecutive_errors, e, exc_info=True)
                if _consecutive_errors >= 20:
                    logger.critical("Too many consecutive errors, stopping engine")
                    raise
                _time.sleep(5)

    @property
    def portfolio(self) -> LivePortfolioView:
        return self._portfolio
