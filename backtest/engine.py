"""
backtest/engine.py

The backtest engine. Takes historical data and computes:

1. Signal replay P&L — for each settled Kalshi contract in the lookback window,
   would our ECMWF-based signal have been profitable at the market price?

2. Calibration stats — per-city, per-month aggregates written to bt_calibration.
   The live strategy reads these to dynamically tune its edge thresholds.

Key insight: We reconstruct what our model probability WOULD HAVE BEEN on each
past date by computing P(daily_high > threshold) from the ECMWF hindcast for
that date. Then we compare to what the Kalshi market was pricing. If the edge
was above our threshold AND we were right, that's a validated signal.
"""

import logging
import math
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


def _ecdf_prob_above(ensemble_highs: List[float], threshold: float) -> float:
    """
    Compute P(daily_high > threshold) from ensemble member daily highs.
    Uses empirical CDF from the 50 members, plus a small normal kernel
    to smooth at the tails.
    """
    if not ensemble_highs:
        return 0.5

    n = len(ensemble_highs)
    # Empirical fraction exceeding threshold
    empirical = sum(1 for h in ensemble_highs if h > threshold) / n

    # Fit normal to ensemble for smoothed estimate
    mean_h = float(np.mean(ensemble_highs))
    std_h = float(np.std(ensemble_highs)) if n > 1 else 3.0
    # Add NWS model error in quadrature (2.5°F RMSE for day-1)
    total_std = math.sqrt(std_h ** 2 + 2.5 ** 2)
    z = (threshold - mean_h) / max(total_std, 0.5)
    smooth = 1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2)))

    # Blend: 60% empirical, 40% smooth (empirical is more honest for 50 members)
    prob = 0.6 * empirical + 0.4 * smooth
    return max(0.01, min(0.99, prob))


class BacktestEngine:
    """
    Replays historical signals and computes calibration statistics.
    """

    def __init__(self, db_path: str = "trading.db"):
        self.db_path = db_path

    # ── Insert collected data into DB ─────────────────────────────────────

    def insert_forecast_accuracy(self, run_id: int, rows: List[Dict]) -> int:
        """Insert bt_forecast_accuracy rows. Returns count inserted."""
        inserted = 0
        with sqlite3.connect(self.db_path) as conn:
            for row in rows:
                try:
                    conn.execute(
                        """INSERT OR REPLACE INTO bt_forecast_accuracy
                           (run_id, city, forecast_date, ecmwf_mean_f, ecmwf_std_f,
                            ecmwf_members, noaa_actual_f, error_f, abs_error_f)
                           VALUES (?,?,?,?,?,?,?,?,?)""",
                        (
                            run_id,
                            row["city"],
                            row["forecast_date"],
                            row.get("ecmwf_mean_f"),
                            row.get("ecmwf_std_f"),
                            row.get("ecmwf_members"),
                            row.get("noaa_actual_f"),
                            row.get("error_f"),
                            row.get("abs_error_f"),
                        ),
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Insert forecast accuracy failed: {e}")
            conn.commit()
        return inserted

    def insert_signal_replay(self, run_id: int, rows: List[Dict]) -> int:
        """Insert bt_signal_replay rows. Returns count inserted."""
        inserted = 0
        with sqlite3.connect(self.db_path) as conn:
            for row in rows:
                try:
                    conn.execute(
                        """INSERT OR REPLACE INTO bt_signal_replay
                           (run_id, city, contract_date, ticker, direction,
                            threshold_f, model_prob, market_prob, edge_bps,
                            entry_price, settlement_price, result, pnl_dollars, outcome)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            run_id,
                            row["city"],
                            row["contract_date"],
                            row["ticker"],
                            row["direction"],
                            row.get("threshold_f"),
                            row.get("model_prob"),
                            row.get("market_prob"),
                            row.get("edge_bps"),
                            row.get("entry_price"),
                            row.get("settlement_price"),
                            row.get("result"),
                            row.get("pnl_dollars"),
                            row.get("outcome"),
                        ),
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Insert signal replay failed: {e}")
            conn.commit()
        return inserted

    # ── Signal replay: reconstruct what our signals would have been ───────

    def replay_signals(
        self,
        kalshi_markets: List[Dict],
        forecast_rows: List[Dict],
        min_edge_bps: float = 150.0,
    ) -> List[Dict]:
        """
        For each settled Kalshi market, reconstruct our model's probability
        using the ECMWF hindcast for that city/date and compute hypothetical P&L.

        Args:
            kalshi_markets: From data_collector.fetch_kalshi_settled_markets()
            forecast_rows:  From data_collector.collect_forecast_accuracy()
            min_edge_bps:   Minimum edge to count as a signal

        Returns:
            List of replay result dicts ready for bt_signal_replay.
        """
        # Index forecast rows by (city, date)
        forecast_index: Dict[Tuple[str, str], Dict] = {}
        for row in forecast_rows:
            key = (row["city"], row["forecast_date"])
            forecast_index[key] = row

        # We need the actual ensemble highs, not just mean/std.
        # The forecast_rows only store mean/std (for DB storage).
        # For replay, we reconstruct a synthetic ensemble from mean±std.
        # This is an approximation — the full ensemble highs are only in memory
        # during data collection. To be more precise, run_backtest stores them
        # in a temp cache passed directly to replay_signals.
        # (See run_backtest.py which passes the raw ensemble_highs through.)

        results = []

        for market in kalshi_markets:
            city = market.get("city")
            contract_date = market.get("contract_date")
            threshold_f = market.get("threshold_f")
            direction = market.get("direction")  # 'above' or 'below'
            last_price = market.get("last_market_price")
            settlement_price = market.get("settlement_price")
            result_str = market.get("result")
            ticker = market.get("ticker")

            # Skip if missing key data
            if None in (city, contract_date, threshold_f, direction, last_price, settlement_price):
                continue

            # Look up forecast for this city/date
            forecast = forecast_index.get((city, contract_date))
            if forecast is None:
                logger.debug(f"No forecast data for {city} on {contract_date} — skipping {ticker}")
                continue

            ecmwf_mean = forecast.get("ecmwf_mean_f")
            ecmwf_std = forecast.get("ecmwf_std_f") or 3.0
            if ecmwf_mean is None:
                continue

            # Reconstruct ensemble from mean/std (50 synthetic members)
            rng = np.random.default_rng(seed=hash(ticker) % (2**31))
            synthetic_highs = list(rng.normal(ecmwf_mean, ecmwf_std, 50))

            # Compute model probability for the contract
            if direction == "above":
                # T contract: YES if daily high > threshold
                model_prob = _ecdf_prob_above(synthetic_highs, threshold_f)
            else:
                # B contract: YES if daily high < threshold
                model_prob = 1.0 - _ecdf_prob_above(synthetic_highs, threshold_f)

            market_prob = last_price  # already normalized to 0-1
            edge_bps = (model_prob - market_prob) * 10000

            # Determine signal direction
            if model_prob > market_prob:
                signal_dir = "BUY"   # we think YES is underpriced
            else:
                signal_dir = "SELL"  # we think YES is overpriced

            # Only count as a signal if edge clears threshold
            if abs(edge_bps) < min_edge_bps:
                continue

            # P&L calculation (per contract, $1 face value)
            entry_price = last_price
            pnl = 0.0
            outcome = "PUSH"

            if signal_dir == "BUY":
                pnl = settlement_price - entry_price
            else:  # SELL: we sold YES, profit if it settles 0
                pnl = entry_price - settlement_price

            if pnl > 0.005:
                outcome = "WIN"
            elif pnl < -0.005:
                outcome = "LOSS"

            results.append({
                "city": city,
                "contract_date": contract_date,
                "ticker": ticker,
                "direction": signal_dir,
                "threshold_f": threshold_f,
                "model_prob": round(model_prob, 4),
                "market_prob": round(market_prob, 4),
                "edge_bps": round(edge_bps, 1),
                "entry_price": round(entry_price, 4),
                "settlement_price": settlement_price,
                "result": result_str,
                "pnl_dollars": round(pnl, 4),
                "outcome": outcome,
            })

        logger.info(
            f"Signal replay: {len(results)} signals from {len(kalshi_markets)} markets "
            f"(min_edge={min_edge_bps}bps)"
        )
        return results

    # ── Calibration: compute per-city stats and write to DB ──────────────

    def compute_calibration(self, run_id: int) -> List[Dict]:
        """
        Aggregate bt_forecast_accuracy and bt_signal_replay into per-city,
        per-month calibration stats. Writes to bt_calibration.

        The live strategy reads bt_calibration to tune edge thresholds.
        """
        calibration_rows = []

        with sqlite3.connect(self.db_path) as conn:
            # Get distinct city × month combinations from forecast accuracy
            cur = conn.execute(
                """
                SELECT DISTINCT city,
                       CAST(strftime('%m', forecast_date) AS INTEGER) as month
                FROM bt_forecast_accuracy
                WHERE run_id = ? AND noaa_actual_f IS NOT NULL
                """,
                (run_id,),
            )
            combos = cur.fetchall()

            for city, month in combos:
                # ── Forecast accuracy stats ────────────────────────────
                cur = conn.execute(
                    """
                    SELECT
                        COUNT(*) as n,
                        AVG(abs_error_f) as mae,
                        SQRT(AVG(error_f * error_f)) as rmse,
                        AVG(error_f) as bias
                    FROM bt_forecast_accuracy
                    WHERE run_id=? AND city=?
                      AND CAST(strftime('%m', forecast_date) AS INTEGER)=?
                      AND noaa_actual_f IS NOT NULL
                    """,
                    (run_id, city, month),
                )
                acc = cur.fetchone()
                n_forecasts, mae, rmse, bias = acc if acc else (0, None, None, None)

                # ── Signal P&L stats ───────────────────────────────────
                cur = conn.execute(
                    """
                    SELECT
                        COUNT(*) as n_signals,
                        SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
                        AVG(edge_bps) as avg_edge,
                        AVG(CASE WHEN outcome='WIN' THEN edge_bps END) as win_edge
                    FROM bt_signal_replay
                    WHERE run_id=? AND city=?
                      AND CAST(strftime('%m', contract_date) AS INTEGER)=?
                    """,
                    (run_id, city, month),
                )
                sig = cur.fetchone()
                n_signals, wins, avg_edge, win_edge = sig if sig else (0, 0, None, None)

                win_rate = (wins / n_signals) if n_signals and n_signals > 0 else None

                # ── Recommended edge threshold ────────────────────────
                # Logic: if RMSE is high (model less reliable), require more edge.
                # Base threshold = 150bps. Add 20bps per °F of RMSE above 2.5.
                base_threshold = 150.0
                if rmse and rmse > 2.5:
                    recommended_min_edge = base_threshold + (rmse - 2.5) * 20
                else:
                    recommended_min_edge = base_threshold

                # Confidence floor: lower it if model has shown high accuracy
                if rmse and rmse < 2.0:
                    confidence_floor = 0.60
                elif rmse and rmse < 3.0:
                    confidence_floor = 0.65
                else:
                    confidence_floor = 0.70

                row = {
                    "city": city,
                    "month": month,
                    "n_forecasts": int(n_forecasts or 0),
                    "rmse_f": round(rmse, 3) if rmse else None,
                    "mae_f": round(mae, 3) if mae else None,
                    "bias_f": round(bias, 3) if bias else None,
                    "n_signals": int(n_signals or 0),
                    "win_rate": round(win_rate, 3) if win_rate is not None else None,
                    "avg_edge_bps": round(avg_edge, 1) if avg_edge else None,
                    "recommended_min_edge_bps": round(recommended_min_edge, 1),
                    "confidence_floor": confidence_floor,
                }
                calibration_rows.append(row)

                logger.info(
                    f"  Calibration {city} month={month:02d}: "
                    f"RMSE={rmse:.2f if rmse else 'N/A'}°F, "
                    f"win_rate={win_rate:.0%if win_rate else 'N/A'}, "
                    f"rec_edge={recommended_min_edge:.0f}bps"
                )

            # Write calibration rows
            for row in calibration_rows:
                conn.execute(
                    """INSERT OR REPLACE INTO bt_calibration
                       (city, month, n_forecasts, rmse_f, mae_f, bias_f,
                        n_signals, win_rate, avg_edge_bps,
                        recommended_min_edge_bps, confidence_floor, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                    (
                        row["city"], row["month"], row["n_forecasts"],
                        row["rmse_f"], row["mae_f"], row["bias_f"],
                        row["n_signals"], row["win_rate"], row["avg_edge_bps"],
                        row["recommended_min_edge_bps"], row["confidence_floor"],
                    ),
                )
            conn.commit()

        logger.info(f"Wrote {len(calibration_rows)} calibration rows to bt_calibration")
        return calibration_rows

    # ── Summary report ────────────────────────────────────────────────────

    def print_summary(self, run_id: int) -> None:
        """Print a readable P&L and calibration summary to stdout."""
        with sqlite3.connect(self.db_path) as conn:
            print("\n" + "="*70)
            print(f"BACKTEST SUMMARY  (run_id={run_id})")
            print("="*70)

            # Overall P&L
            cur = conn.execute(
                """SELECT COUNT(*), SUM(pnl_dollars),
                          SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END),
                          SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END)
                   FROM bt_signal_replay WHERE run_id=?""",
                (run_id,),
            )
            row = cur.fetchone()
            n, total_pnl, wins, losses = row
            win_rate = wins / n if n else 0
            print(f"\nSignal Replay:  {n} signals | Win rate: {win_rate:.1%} | "
                  f"Total P&L: ${total_pnl:+.2f} ({wins}W / {losses}L)")

            # Forecast accuracy by city
            print("\nForecast Accuracy (ECMWF vs NOAA Actual):")
            cur = conn.execute(
                """SELECT city,
                          COUNT(*) as n,
                          ROUND(AVG(abs_error_f),2) as mae,
                          ROUND(SQRT(AVG(error_f*error_f)),2) as rmse,
                          ROUND(AVG(error_f),2) as bias
                   FROM bt_forecast_accuracy
                   WHERE run_id=? AND noaa_actual_f IS NOT NULL
                   GROUP BY city ORDER BY rmse""",
                (run_id,),
            )
            for city, n, mae, rmse, bias in cur.fetchall():
                bias_str = f"{bias:+.2f}" if bias is not None else "N/A"
                print(f"  {city:<4}  n={n:>2}  MAE={mae:.2f}°F  RMSE={rmse:.2f}°F  Bias={bias_str}°F")

            # P&L by city
            print("\nP&L by City:")
            cur = conn.execute(
                """SELECT city,
                          COUNT(*) as n,
                          ROUND(SUM(pnl_dollars),4) as pnl,
                          ROUND(AVG(edge_bps),0) as avg_edge,
                          SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins
                   FROM bt_signal_replay WHERE run_id=?
                   GROUP BY city ORDER BY pnl DESC""",
                (run_id,),
            )
            for city, n, pnl, avg_edge, wins in cur.fetchall():
                wr = wins / n if n else 0
                print(f"  {city:<4}  n={n:>3}  P&L=${pnl:+.4f}  "
                      f"Win%={wr:.0%}  AvgEdge={avg_edge:.0f}bps")

            # Calibration recommendations
            print("\nLive Strategy Recommendations (from bt_calibration):")
            cur = conn.execute(
                """SELECT city, month, rmse_f, win_rate,
                          recommended_min_edge_bps, confidence_floor
                   FROM bt_calibration ORDER BY city, month""",
            )
            for city, month, rmse, wr, rec_edge, conf_floor in cur.fetchall():
                rmse_str = f"{rmse:.2f}" if rmse else "N/A"
                wr_str = f"{wr:.0%}" if wr is not None else "N/A"
                print(f"  {city:<4} M{month:02d}  RMSE={rmse_str}°F  "
                      f"WinRate={wr_str}  → min_edge={rec_edge:.0f}bps  "
                      f"conf_floor={conf_floor:.2f}")

            print("="*70 + "\n")
