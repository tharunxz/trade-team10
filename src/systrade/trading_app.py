"""Systematic Trading Application"""

import os
import json
import logging
from datetime import datetime
from typing import override
from zoneinfo import ZoneInfo

from systrade.feed import AlpacaLiveStockFeed
from systrade.broker import AlpacaBroker
from systrade.strategy import Strategy
from systrade.data import BarData, ExecutionReport
from systrade.engine import Engine
from systrade.config import make_live_strategy, STARTING_CASH, STRATEGY_NAME

import math
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

# ===============================================
#        ---- Buy and Hold Strategy ----
# ===============================================
# It's in the name. _It will not sell_.
# Could also be called the diamond hands strategy
class LongStrategy(Strategy):
    """
    Buy and hold. "Go long" strategy
    """
    def __init__(self, symbol: str) -> None:
        super().__init__()
        self.symbol = symbol
        self.history: list[float] = []
        self.trading_records: list[dict] = []
        logger.info(f"Long Strategy initialized for {self.symbol}")

    @override
    def on_start(self) -> None:
        """Subscribe to the symbol on strategy start"""
        self.subscribe(self.symbol)

    # this will just buy when it gets its first price
    @override
    def on_data(self, data: BarData) -> None:
        """Processes incoming 1-minute bars live."""
        self.current_time = data.as_of

        if self.symbol in data.symbols():
            bar = data[self.symbol]
            price = bar.close

            logger.info(f"Processing bar for {self.symbol} at {data.as_of}: Close={price}")

            # 30% buffer for daytrading
            #-------------------------
            # If you are marked by alpaca as a pattern daytrader,
            #+they will nerf your buying power so this is added
            #+to skirt that.
            qty = math.floor((self.portfolio.buying_power() * 0.70) / price)
            if qty > 0:
                self.post_market_order(self.symbol, quantity=qty)
                logger.info(f"{hl_green}Buy signal! Posting market order for {qty} shares of {self.symbol}{reset}")
                self.order_pending = True
                self._record_trade("BUY", qty, price)
            else:
                logger.warning(f"{yellow}Quantity calculated as 0. Buying Power: {self.portfolio.buying_power()}{reset}")

            
            # add price to tracking log
            self.history.append(price)

    @override
    def on_execution(self, report: ExecutionReport) -> None:
        """Called on an order update"""
        log_report = report.__dict__.copy()
        log_report['fill_timestamp_iso'] = report.fill_timestamp.isoformat()
        logger.info(f"Notified of execution: {log_report}")
        self.trading_records.append(log_report)

    # this function records trades into a json file
    # the trades can be extracted out of a dockerized
    #+instance of this trading app with the following
    #+command: $ docker cp <container_name_or_id>:trading_results.json trading_results.json
    def _record_trade(self, side, qty, price):
        """Helper to save a simple record locally."""
        record = {
            'timestamp': datetime.now().isoformat(),
            'symbol': self.symbol,
            'side': side,
            'quantity': qty,
            'price': price
        }
        with open("trading_results.json", "a") as f:
            f.write(json.dumps(record) + "\n")
    
# =============================================
#    -------  Momentum strategy -----------
# =============================================
# This is the strategy that's most developed in 
#+the repo. You can edit this one for ease, or 
#+anything else to your liking. Just make sure
#+it runs.
class MomentumStrategy(Strategy):
    """
    Momentum strategy with long/short support.
    """
    def __init__(self, symbol: str) -> None:
        super().__init__()
        self.symbol = symbol
        self.history: list[float] = []
        self.trading_records: list[dict] = []
        logger.info(f"Momentum Strategy initialized for {self.symbol}")

    @override
    def on_start(self) -> None:
        """Subscribe to the symbol on strategy start"""
        self.subscribe(self.symbol)

    # main logic that handles buying and selling for this
    #+moment strategy. there's almost surely a better way to
    #+both code and format this. LOLL
    # i am partial to thinking the bad logic offers nice
    #+logging though....
    @override
    def on_data(self, data: BarData) -> None:
        """Processes incoming 1-minute bars live."""
        self.current_time = data.as_of

        if self.symbol in data.symbols():
            bar = data[self.symbol]
            price = bar.close

            logger.info(f"Processing bar for {self.symbol} at {data.as_of}: Close={price}")

            if len(self.history) >= 2:
                buy_signal = price > self.history[-1] > self.history[-2]
                sell_signal = price < self.history[-1] < self.history[-2]

                holding = self.portfolio.is_invested_in(self.symbol)

                # this block will open a long position
                if buy_signal and not holding:
                    logger.debug(f"{blue}Buying Power={self.portfolio.buying_power()}, Invested={self.portfolio.is_invested_in(self.symbol)}{reset}")
                    # add 5% buying power buffer
                    #add temp 30% buffer for daytrading or something
                    qty = math.floor((self.portfolio.buying_power() * 0.70) / price)
                    if qty > 0:
                        self.post_market_order(self.symbol, quantity=qty)
                        logger.info(f"{hl_green}Buy signal! Posting market order for {qty} shares of {self.symbol}{reset}")
                        self.order_pending = True
                        self._record_trade("BUY", qty, price)
                    else:
                        logger.warning(f"{yellow}Quantity calculated as 0. Buying Power: {self.portfolio.buying_power()}{reset}")

                # this block will open a short position
                elif sell_signal and not holding:
                    logger.debug(f"{blue}Buying Power={self.portfolio.buying_power()}, Invested={self.portfolio.is_invested_in(self.symbol)}{reset}")
                    # add 5% buying power buffer
                    #add temp 30% buffer for daytrading or something
                    qty = math.floor((self.portfolio.buying_power() * 0.70) / price)
                    if qty > 0:
                        self.post_market_order(self.symbol, quantity=-qty)
                        logger.info(f"{hl_red}Sell signal! Posting market order for {qty} shares of {self.symbol}{reset}")
                        self.order_pending = True
                        self._record_trade("SELL", qty, price)
                    else:
                        logger.warning(f"{yellow}Quantity calculated as 0. Buying Power: {self.portfolio.buying_power()}{reset}")

                # this block will close a short position
                elif buy_signal and holding:
                    logger.debug(f"{blue}Buying Power={self.portfolio.buying_power()}, Invested={self.portfolio.is_invested_in(self.symbol)}{reset}")
                    pos = self.portfolio.position(self.symbol)
                    logger.info(f"{hl_yellow}Buy signal! Closing short position of {pos.qty} shares of {self.symbol}{reset}")
                    self.post_market_order(self.symbol, quantity=pos.qty)
                    self.order_pending = True
                    self._record_trade("BUY", pos.qty, price)

                # this block will close a long position
                elif sell_signal and holding:
                    logger.debug(f"{blue}Buying Power={self.portfolio.buying_power()}, Invested={self.portfolio.is_invested_in(self.symbol)}{reset}")
                    pos = self.portfolio.position(self.symbol)
                    logger.info(f"{hl_blue}Sell signal! Closing long position of {pos.qty} shares of {self.symbol}{reset}")
                    self.post_market_order(self.symbol, quantity=-pos.qty)
                    self.order_pending = True
                    self._record_trade("SELL", pos.qty, price)
            
            # add price to tracking log
            self.history.append(price)

    @override
    def on_execution(self, report: ExecutionReport) -> None:
        """Called on an order update"""
        log_report = report.__dict__.copy()
        log_report['fill_timestamp_iso'] = report.fill_timestamp.isoformat()
        logger.info(f"Notified of execution: {log_report}")
        self.trading_records.append(log_report)

    # this function records trades into a json file
    # the trades can be extracted out of a dockerized
    #+instance of this trading app with the following
    #+command: $ docker cp <container_name_or_id>:trading_results.json trading_results.json
    def _record_trade(self, side, qty, price):
        """Helper to save a simple record locally."""
        record = {
            'timestamp': datetime.now().isoformat(),
            'symbol': self.symbol,
            'side': side,
            'quantity': qty,
            'price': price
        }
        with open("trading_results.json", "a") as f:
            f.write(json.dumps(record) + "\n")

def main():
    setup_logging()
    logger.info("Starting Systrade Live Trading Application...")
    if not os.getenv("ALPACA_API_KEY"):
        logger.error("API keys not set. Exiting.")
        return

    import time
    max_restarts = 50
    fast_crashes = 0

    while fast_crashes < max_restarts:
        run_start = time.time()
        try:
            feed = AlpacaLiveStockFeed()
            broker = AlpacaBroker()
            strategy = make_live_strategy()

            logger.info(f"Using strategy: {STRATEGY_NAME} (restarts={fast_crashes})")
            engine = Engine(feed=feed, broker=broker, strategy=strategy, cash=STARTING_CASH)
            logger.info("Engine initialized. Starting run...")

            engine.run()
            logger.info("Engine run completed successfully.")
            break  # clean exit

        except KeyboardInterrupt:
            logger.info("Trading interrupted by user.")
            break
        except Exception as e:
            run_secs = time.time() - run_start
            if run_secs > 300:
                fast_crashes = 0  # ran 5+ min — not a fast crash, reset counter
            else:
                fast_crashes += 1

            delay = min(10 * (2 ** fast_crashes), 300)  # 20s → 40s → … → 5min cap
            logger.error(
                "%sEngine crashed after %.0fs (fast_crash #%d/%d): %s%s",
                hl_red, run_secs, fast_crashes, max_restarts, e, reset,
                exc_info=True,
            )
            logger.info("Auto-restarting in %ds...", delay)
            time.sleep(delay)
    else:
        logger.critical(
            "%s%d consecutive fast crashes — giving up. Manual intervention needed.%s",
            hl_red, max_restarts, reset,
        )

    logger.info("Application stopped.")


if __name__ == "__main__":
    main()

