"""PostgreSQL persistence for strategy state, trades, and bar history.

Activated when DATABASE_URL is set in environment.
All operations are best-effort — the trading loop continues if DB is unavailable.
"""

import json
import logging
import os
from datetime import date, datetime

logger = logging.getLogger(__name__)

_conn = None


def _reset_connection() -> None:
    """Close and discard the current connection."""
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
    _conn = None


def _get_connection():
    """Return a lazy singleton connection, or None if DB is not configured."""
    global _conn
    if _conn is not None:
        try:
            if not _conn.closed:
                _conn.execute("SELECT 1")  # verify connection is alive
                return _conn
        except Exception:
            logger.info("DB connection stale — reconnecting")
            _reset_connection()

    url = os.getenv("DATABASE_URL")
    if not url:
        return None

    try:
        import psycopg

        _conn = psycopg.connect(url, autocommit=True)
        _ensure_schema(_conn)
        logger.info("PostgreSQL connected")
        return _conn
    except Exception as e:
        logger.warning("PostgreSQL connection failed: %s", e)
        _conn = None
        return None


def _ensure_schema(conn) -> None:
    """Create tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategy_checkpoints (
            strategy_name TEXT NOT NULL,
            checkpoint_date DATE NOT NULL,
            state JSONB NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (strategy_name, checkpoint_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            traded_at TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity NUMERIC NOT NULL,
            price NUMERIC NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bars (
            symbol TEXT NOT NULL,
            bar_time TIMESTAMPTZ NOT NULL,
            open NUMERIC NOT NULL,
            high NUMERIC NOT NULL,
            low NUMERIC NOT NULL,
            close NUMERIC NOT NULL,
            volume NUMERIC NOT NULL,
            PRIMARY KEY (symbol, bar_time)
        )
    """)


def is_available() -> bool:
    """Check if DB is configured and reachable."""
    return _get_connection() is not None


def save_checkpoint(
    strategy_name: str, checkpoint_date: date, state: dict,
) -> bool:
    """Upsert a strategy checkpoint for the given date."""
    conn = _get_connection()
    if conn is None:
        return False
    try:
        from psycopg.types.json import Jsonb

        conn.execute(
            """
            INSERT INTO strategy_checkpoints (strategy_name, checkpoint_date, state, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (strategy_name, checkpoint_date)
            DO UPDATE SET state = EXCLUDED.state, updated_at = NOW()
            """,
            (strategy_name, checkpoint_date, Jsonb(state)),
        )
        return True
    except Exception as e:
        logger.warning("Checkpoint save to DB failed: %s", e)
        _reset_connection()
        return False


def load_checkpoint(strategy_name: str, checkpoint_date: date) -> dict | None:
    """Load a checkpoint for the given strategy and date."""
    conn = _get_connection()
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT state FROM strategy_checkpoints "
            "WHERE strategy_name = %s AND checkpoint_date = %s",
            (strategy_name, checkpoint_date),
        ).fetchone()
        return row[0] if row else None
    except Exception as e:
        logger.warning("Checkpoint load from DB failed: %s", e)
        _reset_connection()
        return None


def record_trade(
    traded_at: datetime, symbol: str, side: str, quantity: float, price: float,
) -> bool:
    """Insert a trade record."""
    conn = _get_connection()
    if conn is None:
        return False
    try:
        conn.execute(
            "INSERT INTO trades (traded_at, symbol, side, quantity, price) "
            "VALUES (%s, %s, %s, %s, %s)",
            (traded_at, symbol, side, quantity, price),
        )
        return True
    except Exception as e:
        logger.warning("Trade record to DB failed: %s", e)
        _reset_connection()
        return False


def save_bars(bars: list[dict]) -> bool:
    """Batch upsert bars.

    Each dict: symbol, timestamp, open, high, low, close, volume.
    """
    conn = _get_connection()
    if conn is None or not bars:
        return False
    try:
        with conn.cursor() as cur:
            for bar in bars:
                cur.execute(
                    """
                    INSERT INTO bars (symbol, bar_time, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, bar_time) DO NOTHING
                    """,
                    (
                        bar["symbol"], bar["timestamp"],
                        bar["open"], bar["high"], bar["low"],
                        bar["close"], bar["volume"],
                    ),
                )
        return True
    except Exception as e:
        logger.warning("Bar save to DB failed: %s", e)
        _reset_connection()
        return False


def load_recent_bars(symbols: tuple[str, ...], since: datetime) -> list[dict]:
    """Load bars since a given timestamp for warm-up replay."""
    conn = _get_connection()
    if conn is None:
        return []
    try:
        rows = conn.execute(
            """
            SELECT symbol, bar_time, open, high, low, close, volume
            FROM bars
            WHERE symbol = ANY(%s) AND bar_time >= %s
            ORDER BY bar_time ASC
            """,
            (list(symbols), since),
        ).fetchall()
        return [
            {
                "symbol": r[0], "timestamp": r[1],
                "open": float(r[2]), "high": float(r[3]),
                "low": float(r[4]), "close": float(r[5]),
                "volume": float(r[6]),
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning("Bar load from DB failed: %s", e)
        _reset_connection()
        return []
