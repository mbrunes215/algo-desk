"""
backtest/schema.py

Creates and manages the backtesting tables in trading.db.

Tables added:
  bt_forecast_accuracy  — per-day, per-city: ECMWF forecast vs actual high
  bt_signal_replay      — per-day, per-contract: simulated trade P&L
  bt_calibration        — per-city, per-month: aggregated stats the live bot reads
  bt_runs               — one row per backtest run (audit trail)
"""

import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DDL = [
    # ── Forecast accuracy: did ECMWF nail the daily high? ─────────────────
    """
    CREATE TABLE IF NOT EXISTS bt_forecast_accuracy (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id           INTEGER NOT NULL,
        city             TEXT NOT NULL,           -- 'NYC', 'LAX', etc.
        forecast_date    TEXT NOT NULL,           -- YYYY-MM-DD
        ecmwf_mean_f     REAL,                    -- ECMWF ensemble mean (°F)
        ecmwf_std_f      REAL,                    -- ECMWF ensemble std (°F)
        ecmwf_members    INTEGER,                 -- number of ensemble members
        noaa_actual_f    REAL,                    -- NWS observed daily high (°F)
        error_f          REAL,                    -- ecmwf_mean_f - noaa_actual_f
        abs_error_f      REAL,                    -- |error_f|
        model_source     TEXT DEFAULT 'OPEN_METEO_ECMWF',
        created_at       TEXT DEFAULT (datetime('now')),
        UNIQUE(run_id, city, forecast_date)
    )
    """,

    # ── Signal replay: what would we have made on each past day? ──────────
    """
    CREATE TABLE IF NOT EXISTS bt_signal_replay (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id           INTEGER NOT NULL,
        city             TEXT NOT NULL,
        contract_date    TEXT NOT NULL,           -- YYYY-MM-DD
        ticker           TEXT NOT NULL,
        direction        TEXT NOT NULL,           -- 'BUY' | 'SELL'
        threshold_f      REAL,                    -- temperature threshold
        model_prob       REAL,                    -- ECMWF probability at signal time
        market_prob      REAL,                    -- Kalshi implied prob at signal time
        edge_bps         REAL,                    -- model_prob - market_prob in bps
        entry_price      REAL,                    -- price we'd have paid (dollars)
        settlement_price REAL,                    -- 0.0 or 1.0
        result           TEXT,                    -- 'yes' | 'no'
        pnl_dollars      REAL,                    -- per-contract P&L
        outcome          TEXT,                    -- 'WIN' | 'LOSS' | 'PUSH'
        created_at       TEXT DEFAULT (datetime('now')),
        UNIQUE(run_id, ticker)
    )
    """,

    # ── Calibration: aggregated stats the live bot queries ────────────────
    # The live strategy reads this to set per-city thresholds dynamically.
    """
    CREATE TABLE IF NOT EXISTS bt_calibration (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        city             TEXT NOT NULL,
        month            INTEGER NOT NULL,        -- 1-12 (seasonal calibration)
        n_forecasts      INTEGER,                 -- sample size
        rmse_f           REAL,                    -- forecast RMSE in °F
        mae_f            REAL,                    -- mean absolute error in °F
        bias_f           REAL,                    -- systematic bias (positive = model runs hot)
        n_signals        INTEGER,                 -- signals replayed
        win_rate         REAL,                    -- fraction of winning trades
        avg_edge_bps     REAL,                    -- average edge on winning signals
        recommended_min_edge_bps REAL,            -- suggested threshold for live trading
        confidence_floor REAL,                    -- minimum model confidence to trade
        updated_at       TEXT DEFAULT (datetime('now')),
        UNIQUE(city, month)
    )
    """,

    # ── Run log: audit trail for every backtest run ───────────────────────
    """
    CREATE TABLE IF NOT EXISTS bt_runs (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at       TEXT NOT NULL,
        finished_at      TEXT,
        lookback_days    INTEGER NOT NULL,
        cities           TEXT NOT NULL,           -- JSON list
        n_forecast_rows  INTEGER DEFAULT 0,
        n_signal_rows    INTEGER DEFAULT 0,
        n_calibration_rows INTEGER DEFAULT 0,
        status           TEXT DEFAULT 'running',  -- 'running' | 'complete' | 'failed'
        error_message    TEXT,
        created_at       TEXT DEFAULT (datetime('now'))
    )
    """,
]


def init_schema(db_path: str = "trading.db") -> None:
    """Create backtest tables if they don't exist."""
    with sqlite3.connect(db_path) as conn:
        for ddl in DDL:
            conn.execute(ddl)
        conn.commit()
    logger.info(f"Backtest schema ready in {db_path}")


def create_run(db_path: str, lookback_days: int, cities: list) -> int:
    """Insert a new bt_runs row and return its id."""
    import json
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO bt_runs (started_at, lookback_days, cities) VALUES (?, ?, ?)",
            (datetime.utcnow().isoformat(), lookback_days, json.dumps(cities)),
        )
        run_id = cur.lastrowid
        conn.commit()
    return run_id


def finish_run(db_path: str, run_id: int, counts: dict, status: str = "complete", error: str = None) -> None:
    """Mark a run as complete and record row counts."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE bt_runs
               SET finished_at=?, status=?, error_message=?,
                   n_forecast_rows=?, n_signal_rows=?, n_calibration_rows=?
               WHERE id=?""",
            (
                datetime.utcnow().isoformat(),
                status,
                error,
                counts.get("forecast", 0),
                counts.get("signal", 0),
                counts.get("calibration", 0),
                run_id,
            ),
        )
        conn.commit()
