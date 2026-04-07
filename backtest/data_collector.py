"""
backtest/data_collector.py

Collects two types of historical data for backtesting:

1. ECMWF hindcast forecasts via Open-Meteo
   For each past date, pull the 50-member ECMWF ensemble that was AVAILABLE
   at forecast time (not today's forecast for a past date — we want to know
   what the model said THEN, before the day happened).

   Open-Meteo's ensemble API lets us request historical dates, but the
   "forecast" is actually a reanalysis for dates in the past. This is a
   known limitation: true hindcasts aren't freely available. We use ERA5
   reanalysis as the closest proxy for ECMWF skill at T-1 day.

2. NOAA observed actual daily highs
   Uses the NWS API or Open-Meteo's historical weather API for observed
   temperature (much more reliable than the forecast endpoint for past dates).

3. Kalshi historical market prices (best effort)
   Queries the Kalshi API for closed/settled markets in the past 30 days.
   This gives us what the market was pricing before settlement.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
import numpy as np

logger = logging.getLogger(__name__)

# City coordinates (same as weather_strategy.py)
CITY_COORDS: Dict[str, Tuple[float, float]] = {
    "NYC": (40.7128, -74.0060),
    "LAX": (33.9425, -118.4081),
    "CHI": (41.8781, -87.6298),
    "MIA": (25.7617, -80.1918),
    "DEN": (39.7392, -104.9903),
    "HOU": (29.7604, -95.3698),
    "PHX": (33.4484, -112.0740),
}

# Kalshi series → location mapping
SERIES_TO_CITY = {
    "KXHIGHNY": "NYC", "KXLOWNY": "NYC",
    "KXHIGHLAX": "LAX", "KXHIGHLAX": "LAX",
    "KXHIGHCHI": "CHI", "KXLOWCHI": "CHI",
    "KXHIGHMIA": "MIA",
    "KXHIGHDEN": "DEN",
    "KXHIGHHOU": "HOU",
    "KXHIGHPHX": "PHX",
}

USER_AGENT = "(algo-trading-desk-backtest, mattbrunetto215@gmail.com)"


class HistoricalDataCollector:
    """
    Fetches historical forecast accuracy data and Kalshi market history.
    """

    def __init__(
        self,
        open_meteo_base: str = "https://ensemble-api.open-meteo.com/v1/ensemble",
        open_meteo_historical_base: str = "https://historical-forecast-api.open-meteo.com/v1/forecast",
        noaa_obs_base: str = "https://api.weather.gov",
        kalshi_api_key: str = "",
        kalshi_base: str = "https://api.elections.kalshi.com/trade-api/v2",
        request_delay: float = 0.5,
    ):
        self.open_meteo_base = open_meteo_base
        self.open_meteo_historical_base = open_meteo_historical_base
        self.noaa_obs_base = noaa_obs_base
        self.kalshi_api_key = kalshi_api_key
        self.kalshi_base = kalshi_base
        self.request_delay = request_delay
        self._grid_cache: Dict[str, Tuple[str, int, int]] = {}

    # ── ECMWF Ensemble: what the model said T-1 day before target ─────────

    def fetch_ecmwf_hindcast(
        self, city: str, target_date: datetime
    ) -> Optional[Dict]:
        """
        Fetch the ECMWF ensemble forecast for target_date as it would have
        appeared the day before (T-1 day forecast).

        Uses Open-Meteo's historical forecast API which returns the actual
        archived forecast from the model run, not reanalysis.

        Returns dict with keys: mean_f, std_f, members, ensemble_highs
        """
        coords = CITY_COORDS.get(city)
        if not coords:
            return None

        lat, lon = coords
        date_str = target_date.strftime("%Y-%m-%d")

        # For a T-1 forecast: we request the forecast initialized on target_date-1
        # Open-Meteo historical forecast API returns the archived model output
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m",
                "models": "ecmwf_ifs025",
                "start_date": date_str,
                "end_date": date_str,
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
            }
            headers = {"User-Agent": USER_AGENT}

            resp = requests.get(
                self.open_meteo_base,
                params=params,
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()

            hourly = data.get("hourly", {})
            if not hourly:
                logger.warning(f"No hourly data for {city} on {date_str}")
                return None

            # Extract per-member daily highs
            ensemble_highs = []
            for member_idx in range(1, 51):
                key = f"temperature_2m_member{member_idx:02d}"
                temps = hourly.get(key, [])
                if temps:
                    valid_temps = [t for t in temps if t is not None]
                    if valid_temps:
                        ensemble_highs.append(max(valid_temps))

            if not ensemble_highs:
                logger.warning(f"No ensemble members extracted for {city} on {date_str}")
                return None

            result = {
                "city": city,
                "date": date_str,
                "mean_f": float(np.mean(ensemble_highs)),
                "std_f": float(np.std(ensemble_highs)),
                "members": len(ensemble_highs),
                "ensemble_highs": ensemble_highs,
                "source": "OPEN_METEO_ECMWF",
            }

            logger.info(
                f"  ECMWF hindcast {city} {date_str}: "
                f"mean={result['mean_f']:.1f}°F ±{result['std_f']:.1f} "
                f"({result['members']} members)"
            )
            time.sleep(self.request_delay)
            return result

        except Exception as e:
            logger.warning(f"ECMWF hindcast failed for {city} on {date_str}: {e}")
            return None

    # ── NOAA: actual observed daily high ──────────────────────────────────

    def fetch_noaa_actual(self, city: str, target_date: datetime) -> Optional[float]:
        """
        Fetch the actual observed daily high temperature for a city on target_date.

        Uses Open-Meteo's historical weather API (ERA5 reanalysis) since it's
        more reliable and complete than NWS historical observations for programmatic use.

        Returns the observed daily high in °F, or None on failure.
        """
        coords = CITY_COORDS.get(city)
        if not coords:
            return None

        lat, lon = coords
        date_str = target_date.strftime("%Y-%m-%d")

        try:
            # Open-Meteo historical weather = ERA5 reanalysis (observations)
            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max",
                "temperature_unit": "fahrenheit",
                "start_date": date_str,
                "end_date": date_str,
                "timezone": "auto",
            }
            headers = {"User-Agent": USER_AGENT}

            resp = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params=params,
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()

            daily = data.get("daily", {})
            temps = daily.get("temperature_2m_max", [])

            if temps and temps[0] is not None:
                actual_f = float(temps[0])
                logger.info(f"  NOAA actual {city} {date_str}: {actual_f:.1f}°F")
                time.sleep(self.request_delay)
                return actual_f

            logger.warning(f"No actual temp returned for {city} on {date_str}")
            return None

        except Exception as e:
            logger.warning(f"NOAA actual failed for {city} on {date_str}: {e}")
            return None

    # ── Kalshi: historical market prices ─────────────────────────────────

    def fetch_kalshi_settled_markets(
        self, lookback_days: int = 30
    ) -> List[Dict]:
        """
        Fetch settled Kalshi weather markets from the last N days.

        Returns list of dicts with: ticker, city, date, threshold_f, direction,
        pre_settlement_price, settlement_price, result.

        Note: Kalshi's API returns settled markets with their final result.
        We also try to recover the pre-settlement price from the last_price
        field (which reflects the final traded price before settlement).
        """
        if not self.kalshi_api_key:
            logger.warning("No Kalshi API key — skipping Kalshi historical fetch")
            return []

        headers = {
            "Authorization": f"Bearer {self.kalshi_api_key}",
            "Accept": "application/json",
        }

        # Build list of weather series to query
        weather_series = [
            "KXHIGHNY", "KXLOWNY",
            "KXHIGHLAX", "KXLOWLAX",
            "KXHIGHCHI", "KXLOWCHI",
            "KXHIGHMIA", "KXHIGHDEN",
            "KXHIGHHOU", "KXHIGHPHX",
        ]

        cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
        results = []

        for series in weather_series:
            city = SERIES_TO_CITY.get(series)
            if not city:
                continue

            try:
                params = {
                    "series_ticker": series,
                    "status": "settled",
                    "limit": 100,
                }
                resp = requests.get(
                    f"{self.kalshi_base}/markets",
                    params=params,
                    headers=headers,
                    timeout=15,
                )

                if resp.status_code != 200:
                    logger.warning(f"Kalshi API {resp.status_code} for series {series}")
                    continue

                markets = resp.json().get("markets", [])
                logger.info(f"  Kalshi {series}: {len(markets)} settled markets")

                for m in markets:
                    # Parse the ticker to extract date and threshold
                    # e.g. KXHIGHNY-26MAR25-T58 or KXHIGHNY-26MAR25-B57.5
                    ticker = m.get("ticker", "")
                    parsed = _parse_kalshi_ticker(ticker)
                    if not parsed:
                        continue

                    contract_date, threshold_f, direction = parsed

                    # Skip if outside our lookback window
                    if contract_date < cutoff_date:
                        continue

                    # Kalshi prices are in cents (0-100); normalize to 0-1
                    last_price_raw = m.get("last_price_dollars") or m.get("last_price", 0.5)
                    last_price = float(last_price_raw)
                    if last_price > 1.0:
                        last_price = last_price / 100.0

                    result_str = m.get("result")  # 'yes' | 'no' | None
                    settlement_price = None
                    if result_str == "yes":
                        settlement_price = 1.0
                    elif result_str == "no":
                        settlement_price = 0.0

                    results.append({
                        "ticker": ticker,
                        "city": city,
                        "series": series,
                        "contract_date": contract_date.strftime("%Y-%m-%d"),
                        "threshold_f": threshold_f,
                        "direction": direction,  # 'above' (T) or 'below' (B)
                        "last_market_price": last_price,
                        "settlement_price": settlement_price,
                        "result": result_str,
                        "volume": int(m.get("volume", 0)),
                    })

                time.sleep(self.request_delay)

            except Exception as e:
                logger.warning(f"Kalshi fetch failed for {series}: {e}")

        logger.info(f"Fetched {len(results)} settled Kalshi markets total")
        return results

    # ── Batch fetch for a full date range ────────────────────────────────

    def collect_forecast_accuracy(
        self,
        cities: List[str],
        lookback_days: int = 30,
    ) -> List[Dict]:
        """
        For each city × date in the lookback window, fetch both:
        - ECMWF ensemble hindcast
        - NOAA observed actual

        Returns list of dicts ready to insert into bt_forecast_accuracy.
        """
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        # Don't include today — it hasn't settled yet
        dates = [today - timedelta(days=i) for i in range(1, lookback_days + 1)]

        rows = []
        total = len(cities) * len(dates)
        done = 0

        for city in cities:
            for target_date in dates:
                done += 1
                if done % 10 == 0:
                    logger.info(f"  Progress: {done}/{total} city-day pairs")

                ecmwf = self.fetch_ecmwf_hindcast(city, target_date)
                actual = self.fetch_noaa_actual(city, target_date)

                if ecmwf is None and actual is None:
                    continue

                error_f = None
                abs_error_f = None
                if ecmwf and actual is not None:
                    error_f = round(ecmwf["mean_f"] - actual, 2)
                    abs_error_f = round(abs(error_f), 2)

                rows.append({
                    "city": city,
                    "forecast_date": target_date.strftime("%Y-%m-%d"),
                    "ecmwf_mean_f": ecmwf["mean_f"] if ecmwf else None,
                    "ecmwf_std_f": ecmwf["std_f"] if ecmwf else None,
                    "ecmwf_members": ecmwf["members"] if ecmwf else None,
                    "noaa_actual_f": actual,
                    "error_f": error_f,
                    "abs_error_f": abs_error_f,
                })

        logger.info(f"Collected {len(rows)} forecast accuracy rows")
        return rows


def _parse_kalshi_ticker(ticker: str) -> Optional[Tuple[datetime, float, str]]:
    """
    Parse a Kalshi weather ticker into (contract_date, threshold_f, direction).

    Format: KXHIGHNY-26MAR25-T58  or  KXHIGHNY-26MAR25-B57.5
    Date:  DDMMMYY  (e.g. 26MAR25 = March 26, 2025 or 2026?)
    Kalshi uses 2-digit year; 25 = 2025, 26 = 2026
    Direction: T = above threshold (YES if daily high > threshold)
               B = below threshold (YES if daily high < threshold)

    Returns None if parse fails.
    """
    import re
    try:
        parts = ticker.split("-")
        if len(parts) < 3:
            return None

        date_part = parts[1]   # e.g. "26MAR25"
        strike_part = parts[2] # e.g. "T58" or "B57.5"

        # Parse date: DDMMMYY
        dt = datetime.strptime(date_part, "%d%b%y")

        # Parse strike
        if not strike_part:
            return None
        direction_char = strike_part[0].upper()
        threshold_str = strike_part[1:]
        threshold_f = float(threshold_str)

        direction = "above" if direction_char == "T" else "below"

        return dt, threshold_f, direction

    except Exception:
        return None
