"""
Volume Surge Breakout (VSB) Strategy — Momentum complement to VWAP MR.

Enters when price breaks out of the opening range on high relative volume.
This is structurally uncorrelated with alpha_vwap: VSB profits on trend days
while alpha_vwap profits on mean-reverting days.

Signals
-------
1. Opening range (first 15 bars, 9:30–9:44 ET) defines the high/low channel.
2. Breakout: 1-min close breaches the OR high/low with RVOL >= 2.0.
3. VWAP filter: long only above VWAP, short only below VWAP.
4. One signal per side per day to avoid chasing.

Exits
-----
- ATR-based trailing stop (activates after 1x ATR profit, trails at 2x ATR).
- Initial stop: 1.5x ATR from entry.
- EOD flatten at 15:45 ET.
"""

from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import time
from typing import override
from zoneinfo import ZoneInfo

from systrade.data import Bar, BarData, ExecutionReport
from systrade.strategies.scanner import ScannerWeights, score_universe, select_active
from systrade.strategy import Strategy

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

MARKET_OPEN = time(9, 30)
OR_END = time(9, 45)          # 15-minute opening range
ENTRY_OPEN = time(10, 0)      # don't enter during first 30 min noise
ENTRY_CLOSE = time(14, 30)    # stop entering after 2:30 PM
FLATTEN_TIME = time(15, 45)


@dataclass
class VSBSymbolState:
    """Per-symbol intraday state."""
    # Opening range
    or_high: float = 0.0
    or_low: float = float("inf")
    or_bars: int = 0
    or_complete: bool = False

    # VWAP
    cumulative_volume: float = 0.0
    cumulative_pv: float = 0.0
    vwap: float = 0.0

    # RVOL — rolling 20-bar volume average
    volume_history: deque = field(default_factory=lambda: deque(maxlen=20))
    bar_count: int = 0

    # ATR (14-bar)
    true_ranges: deque = field(default_factory=lambda: deque(maxlen=14))
    prev_close: float = 0.0
    atr: float = 0.0

    # Overnight gap for scanner scoring
    gap_pct: float = 0.0

    # Position tracking
    entry_price: float | None = None
    entry_side: str = ""          # "long" or "short"
    highest_since_entry: float = 0.0
    lowest_since_entry: float = float("inf")
    trail_active: bool = False

    # One signal per side per day
    fired_long: bool = False
    fired_short: bool = False


class VolumeSurgeBreakoutStrategy(Strategy):
    """
    Opening range breakout with relative volume confirmation.

    Parameters
    ----------
    symbols : tuple
        Symbols to trade.
    rvol_threshold : float
        Minimum relative volume to confirm breakout (default 2.0x).
    atr_stop_mult : float
        Initial stop distance in ATR multiples (default 1.5).
    atr_trail_mult : float
        Trailing stop distance in ATR multiples (default 2.0).
    atr_profit_trigger : float
        ATR profit needed before trailing stop activates (default 1.0).
    position_frac : float
        Fraction of buying power per trade.
    leverage : float
        Leverage multiplier.
    max_positions : int
        Maximum concurrent positions.
    """

    def __init__(
        self,
        symbols: tuple[str, ...] = ("TQQQ", "SOXL", "TNA", "SQQQ", "UDOW"),
        scan_universe: tuple[str, ...] | None = None,
        rvol_threshold: float = 2.0,
        atr_period: int = 14,
        atr_stop_mult: float = 1.5,
        atr_trail_mult: float = 2.0,
        atr_profit_trigger: float = 1.0,
        position_frac: float = 0.10,
        leverage: float = 2.0,
        max_positions: int = 2,
        shortable_symbols: frozenset[str] | None = None,
        active_count: int = 8,
        scan_interval_bars: int = 60,
        min_rvol_for_scan: float = 1.0,
        weight_rvol: float = 0.40,
        weight_gap: float = 0.25,
        weight_atr_pct: float = 0.20,
        weight_range: float = 0.15,
    ) -> None:
        super().__init__()
        self._symbols = symbols
        self._rvol_threshold = rvol_threshold
        self._atr_period = atr_period
        self._atr_stop_mult = atr_stop_mult
        self._atr_trail_mult = atr_trail_mult
        self._atr_profit_trigger = atr_profit_trigger
        self._position_frac = position_frac
        self._leverage = leverage
        self._max_positions = max_positions
        self._shortable: frozenset[str] = shortable_symbols or frozenset({
            "TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU",
            "TNA", "TZA", "FAS", "FAZ", "ERX", "NRGU", "LABU", "UDOW", "SDOW",
        })

        # Scanner: when scan_universe is set, subscribe all and dynamically
        # rotate the active trading set.  When None, fall back to symbols only.
        self._scan_universe: tuple[str, ...] = scan_universe or symbols
        self._scanning_enabled = scan_universe is not None
        self._active_symbols: list[str] = list(symbols)
        self._active_count = active_count
        self._scan_interval = scan_interval_bars
        self._bars_since_scan = 0
        self._min_rvol_for_scan = min_rvol_for_scan
        self._scanner_weights = ScannerWeights(
            rvol=weight_rvol, gap=weight_gap,
            atr_pct=weight_atr_pct, range=weight_range,
        )

        self._states: dict[str, VSBSymbolState] = {}
        self._open_position_count = 0
        self._last_reset_date = None
        self._trading_records: list[dict] = []

        logger.info(
            "VSB initialized | symbols=%s universe=%d scanning=%s "
            "active_count=%d rvol=%.1fx atr_stop=%.1f atr_trail=%.1f",
            symbols, len(self._scan_universe), self._scanning_enabled,
            active_count, rvol_threshold, atr_stop_mult, atr_trail_mult,
        )

    # ── Lifecycle ─────────────────────────────────────────────────

    @override
    def on_start(self) -> None:
        for sym in self._scan_universe:
            self.subscribe(sym)
            self._states[sym] = VSBSymbolState()
        if self._scanning_enabled:
            self._active_symbols = list(self._scan_universe[:self._active_count])
            logger.info("Scanner active: subscribed %d symbols, initial active=%s",
                        len(self._scan_universe), self._active_symbols)

    @override
    def on_data(self, data: BarData) -> None:
        self.current_time = data.as_of
        now_et = data.as_of.astimezone(ET) if data.as_of.tzinfo else data.as_of
        now_time = now_et.time()

        if self._should_reset(now_et):
            self._daily_reset(now_et, data)

        if now_time >= FLATTEN_TIME:
            self._flatten_all("EOD flatten")
            return

        # Update indicators for ALL universe symbols (keeps them warm)
        for sym in self._scan_universe:
            bar = data.get(sym)
            if bar is None:
                continue
            state = self._states.get(sym)
            if state is None:
                continue
            self._update_indicators(sym, bar, now_time, state)

        # Run scanner periodically
        if self._scanning_enabled:
            self._bars_since_scan += 1
            if self._bars_since_scan >= self._scan_interval:
                self._run_scanner()
                self._bars_since_scan = 0

        # Manage positions for any invested symbol (uses local state, no API calls)
        for sym in self._scan_universe:
            state = self._states.get(sym)
            if state is None or state.entry_price is None:
                continue
            bar = data.get(sym)
            if bar is not None:
                self._manage_position(sym, bar, state)

        # Check entries ONLY for active symbols
        if now_time >= ENTRY_OPEN and now_time < ENTRY_CLOSE:
            for sym in self._active_symbols:
                bar = data.get(sym)
                if bar is None:
                    continue
                state = self._states.get(sym)
                if state is None or state.entry_price is not None:
                    continue
                self._check_entry(sym, bar, state)

    @override
    def on_execution(self, report: ExecutionReport) -> None:
        sym = report.order.symbol
        logger.info("VSB FILL %s %+.0f @ %.2f", sym, report.last_quantity, report.last_price)
        self._trading_records.append({
            "timestamp": report.fill_timestamp.isoformat() if report.fill_timestamp else "",
            "symbol": sym,
            "side": "BUY" if report.order.quantity > 0 else "SELL",
            "quantity": abs(report.order.quantity),
            "price": report.last_price,
        })

    # ── Daily reset ───────────────────────────────────────────────

    def _should_reset(self, now_et) -> bool:
        if self._last_reset_date is None:
            return True
        return now_et.date() != self._last_reset_date.date()

    def _daily_reset(self, now_et, data: BarData | None = None) -> None:
        logger.info("=== VSB DAILY RESET %s ===", now_et.date())
        # Preserve prev_close for gap computation
        prev_closes = {
            sym: s.prev_close
            for sym, s in self._states.items()
            if s.prev_close > 0
        }
        for sym in self._scan_universe:
            self._states[sym] = VSBSymbolState()
        # Compute gap % from yesterday's close vs today's open
        if data is not None:
            for sym in self._scan_universe:
                bar = data.get(sym)
                pc = prev_closes.get(sym)
                if bar is not None and pc and pc > 0:
                    self._states[sym].gap_pct = (bar.open - pc) / pc * 100
        self._open_position_count = 0
        self._last_reset_date = now_et
        self._bars_since_scan = 0  # force scan on first data after reset

    # ── Indicator updates ─────────────────────────────────────────

    def _update_indicators(
        self, sym: str, bar: Bar, now_time: time, state: VSBSymbolState,
    ) -> None:
        price = bar.close
        volume = bar.volume

        # VWAP
        state.cumulative_pv += price * volume
        state.cumulative_volume += volume
        state.vwap = (
            state.cumulative_pv / state.cumulative_volume
            if state.cumulative_volume > 0 else price
        )

        # Opening range (first 15 bars)
        if not state.or_complete:
            state.or_high = max(state.or_high, bar.high)
            state.or_low = min(state.or_low, bar.low)
            state.or_bars += 1
            if now_time >= OR_END:
                state.or_complete = True
                logger.debug("VSB OR %s: high=%.2f low=%.2f", sym, state.or_high, state.or_low)

        # ATR
        if state.prev_close > 0:
            tr = max(
                bar.high - bar.low,
                abs(bar.high - state.prev_close),
                abs(bar.low - state.prev_close),
            )
            state.true_ranges.append(tr)
            if len(state.true_ranges) >= self._atr_period:
                state.atr = sum(state.true_ranges) / len(state.true_ranges)
        state.prev_close = price

        # Volume history for RVOL
        state.volume_history.append(volume)
        state.bar_count += 1

    # ── Entry logic ───────────────────────────────────────────────

    def _check_entry(self, sym: str, bar: Bar, state: VSBSymbolState) -> None:
        if not state.or_complete:
            return
        if state.atr < 1e-9:
            return
        if self._open_position_count >= self._max_positions:
            return

        # Compute RVOL
        if len(state.volume_history) < 5:
            return
        avg_vol = sum(state.volume_history) / len(state.volume_history)
        if avg_vol < 1e-9:
            return
        rvol = bar.volume / avg_vol

        price = bar.close

        # Long breakout
        if (
            not state.fired_long
            and price > state.or_high
            and rvol >= self._rvol_threshold
            and price > state.vwap
        ):
            qty = self._compute_size(price)
            if qty > 0:
                self.post_market_order(sym, qty)
                state.entry_price = price
                state.entry_side = "long"
                state.highest_since_entry = price
                state.fired_long = True
                self._open_position_count += 1
                logger.info(
                    "VSB ENTRY LONG %s qty=%d price=%.2f or_high=%.2f rvol=%.1f atr=%.3f",
                    sym, qty, price, state.or_high, rvol, state.atr,
                )

        # Short breakout
        elif (
            not state.fired_short
            and price < state.or_low
            and rvol >= self._rvol_threshold
            and price < state.vwap
            and sym in self._shortable
        ):
            qty = self._compute_size(price)
            if qty > 0:
                self.post_market_order(sym, -qty)
                state.entry_price = price
                state.entry_side = "short"
                state.lowest_since_entry = price
                state.fired_short = True
                self._open_position_count += 1
                logger.info(
                    "VSB ENTRY SHORT %s qty=%d price=%.2f or_low=%.2f rvol=%.1f atr=%.3f",
                    sym, qty, price, state.or_low, rvol, state.atr,
                )

    # ── Position management ───────────────────────────────────────

    def _manage_position(self, sym: str, bar: Bar, state: VSBSymbolState) -> None:
        if state.entry_price is None or state.atr < 1e-9:
            return

        price = bar.close

        if state.entry_side == "long":
            state.highest_since_entry = max(state.highest_since_entry, price)
            profit = price - state.entry_price

            # Initial stop: 1.5x ATR below entry
            initial_stop = state.entry_price - self._atr_stop_mult * state.atr
            if price <= initial_stop:
                self._close_position(sym, "VSB INIT STOP LONG")
                return

            # Activate trailing stop after 1x ATR profit
            if profit >= self._atr_profit_trigger * state.atr:
                state.trail_active = True

            if state.trail_active:
                trail_stop = state.highest_since_entry - self._atr_trail_mult * state.atr
                if price <= trail_stop:
                    self._close_position(sym, "VSB TRAIL STOP LONG")
                    return

        elif state.entry_side == "short":
            state.lowest_since_entry = min(state.lowest_since_entry, price)
            profit = state.entry_price - price

            # Initial stop: 1.5x ATR above entry
            initial_stop = state.entry_price + self._atr_stop_mult * state.atr
            if price >= initial_stop:
                self._close_position(sym, "VSB INIT STOP SHORT")
                return

            # Activate trailing stop after 1x ATR profit
            if profit >= self._atr_profit_trigger * state.atr:
                state.trail_active = True

            if state.trail_active:
                trail_stop = state.lowest_since_entry + self._atr_trail_mult * state.atr
                if price >= trail_stop:
                    self._close_position(sym, "VSB TRAIL STOP SHORT")
                    return

    # ── Helpers ───────────────────────────────────────────────────

    def _compute_size(self, price: float) -> int:
        try:
            capital = self.portfolio.buying_power()
        except (AttributeError, NotImplementedError):
            capital = self.portfolio.value() * self._leverage
        raw = (capital * self._position_frac) / price
        return max(int(math.floor(raw)), 0)

    def _close_position(self, sym: str, reason: str) -> None:
        if not self.portfolio.is_invested_in(sym):
            return
        pos = self.portfolio.position(sym)
        self.post_market_order(sym, quantity=-pos.qty)
        self._open_position_count = max(self._open_position_count - 1, 0)
        state = self._states[sym]
        state.entry_price = None
        state.entry_side = ""
        state.trail_active = False
        logger.info("VSB CLOSE %s qty=%+.0f reason=%s", sym, -pos.qty, reason)

    def _run_scanner(self) -> None:
        """Score the universe and update _active_symbols."""
        # Build protected set: symbols with open positions (local check, no API)
        protected = {
            sym for sym, state in self._states.items()
            if state.entry_price is not None
        }

        scores = score_universe(
            self._states, self._scan_universe,
            self._scanner_weights, self._min_rvol_for_scan,
        )
        new_active = select_active(scores, self._active_count, protected)

        promoted = set(new_active) - set(self._active_symbols)
        demoted = set(self._active_symbols) - set(new_active)
        if promoted or demoted:
            logger.info(
                "SCANNER: promoted=%s demoted=%s active=%s",
                sorted(promoted), sorted(demoted), new_active,
            )
        else:
            logger.debug("SCANNER: no changes, active=%s", new_active)

        # Log top 5 scores for observability
        for s in scores[:5]:
            logger.debug(
                "  %s: composite=%.3f rvol=%.1f gap=%.1f%% atr=%.2f%% range=%.2f%%",
                s.symbol, s.composite, s.rvol, s.gap_pct, s.atr_pct, s.intraday_range,
            )

        self._active_symbols = new_active

    def _flatten_all(self, reason: str) -> None:
        for sym in self._scan_universe:
            self._close_position(sym, reason)
        self._open_position_count = 0
