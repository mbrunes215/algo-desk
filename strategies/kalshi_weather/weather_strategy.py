"""
Kalshi Weather Strategy Module

This module implements a quantitative weather trading strategy for Kalshi contracts.
The core approach:
1. Fetches NOAA weather forecast data via api.weather.gov (NWS public API)
2. Builds probability distributions from gridpoint forecast time-series data
3. Compares model probabilities to Kalshi contract implied probabilities
4. Generates buy/sell signals when statistical edge exceeds threshold

The strategy uses real NOAA National Blend of Models (NBM) data exposed through
the NWS gridpoint forecast endpoint, which provides hourly temperature and
precipitation forecasts. We fit a normal distribution to the forecast data
and compute exceedance probabilities for Kalshi contract thresholds.

Falls back to synthetic ensemble data if the API is unreachable.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import logging
from enum import Enum
import requests
try:
    from scipy import stats
except ImportError:
    stats = None  # scipy optional; not used in core probability calculations
import numpy as np
import time as _time

logger = logging.getLogger(__name__)

# ── NOAA City Coordinates ──────────────────────────────────────────────────
# Maps our location codes to (latitude, longitude) for api.weather.gov lookups.
CITY_COORDS: Dict[str, Tuple[float, float]] = {
    "NYC": (40.7128, -74.0060),
    "LAX": (33.9425, -118.4081),
    "CHI": (41.8781, -87.6298),
    "MIA": (25.7617, -80.1918),
    "DEN": (39.7392, -104.9903),
    "HOU": (29.7604, -95.3698),
    "PHX": (33.4484, -112.0740),
}


class SignalType(Enum):
    """Trading signal types."""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    SKIP = "SKIP"  # Insufficient edge or data quality


@dataclass
class WeatherForecast:
    """
    Represents ensemble weather forecast data for a location.

    Attributes:
        location: Geographic identifier (e.g., "NYC", "LAX")
        date: Forecast target date
        ensemble_members: List of ensemble model runs (each run is dict of predictions)
        mean_temp_f: Mean temperature across ensemble (Fahrenheit)
        std_temp_f: Standard deviation of temperature
        mean_precip_in: Mean precipitation across ensemble (inches)
        prob_precip_threshold: Probability of precipitation >= threshold (0-1)
        confidence_score: Confidence in the forecast (0-1, higher is better)
        model_name: Source model (e.g., "GFS", "HRRR", "NAM")
        issued_time: When the forecast was issued
    """
    location: str
    date: datetime
    ensemble_members: List[Dict[str, float]]
    mean_temp_f: float
    std_temp_f: float
    mean_precip_in: float
    prob_precip_threshold: float
    confidence_score: float
    model_name: str
    issued_time: datetime


@dataclass
class ContractSignal:
    """
    Represents a trading signal for a specific Kalshi contract.

    Attributes:
        contract_id: Kalshi contract identifier
        signal: BUY, SELL, HOLD, or SKIP
        model_probability: Our estimated probability
        market_probability: Implied probability from contract price
        edge: Absolute edge (our prob - market prob), in basis points
        confidence: Confidence level of this signal (0-1)
        weather_metric: The weather variable being traded (e.g., "TEMP_ABOVE_72")
        recommended_position_size: Suggested position size based on edge and confidence
        rationale: Human-readable explanation of the signal
    """
    contract_id: str
    signal: SignalType
    model_probability: float
    market_probability: float
    edge: float  # basis points
    confidence: float
    weather_metric: str
    recommended_position_size: float
    rationale: str


class WeatherStrategy:
    """
    Quantitative weather trading strategy for Kalshi weather contracts.

    This strategy combines ensemble weather forecasts with market prices to identify
    mispriced weather contracts. The key insight is that ensemble forecasts provide
    a true probability distribution, while market prices reflect only consensus view
    and may diverge from statistical reality.
    """

    def __init__(
        self,
        noaa_api_base: str = "https://api.weather.gov",
        open_meteo_api_base: str = "https://ensemble-api.open-meteo.com/v1/ensemble",
        min_edge_bps: float = 150.0,
        min_confidence: float = 0.65,
        lookback_days: int = 30,
        noaa_timeout: int = 12,
        open_meteo_timeout: int = 15,
    ):
        """
        Initialize the weather strategy.

        Args:
            noaa_api_base: Base URL for NOAA API
            open_meteo_api_base: Base URL for Open-Meteo ensemble API
            min_edge_bps: Minimum edge in basis points to generate signal
            min_confidence: Minimum confidence threshold (0-1)
            lookback_days: Days of historical data to maintain
            noaa_timeout: HTTP timeout for NOAA API calls in seconds
            open_meteo_timeout: HTTP timeout for Open-Meteo API calls in seconds
        """
        self.noaa_api_base = noaa_api_base
        self.open_meteo_api_base = open_meteo_api_base
        self.min_edge_bps = min_edge_bps
        self.min_confidence = min_confidence
        self.lookback_days = lookback_days
        self.noaa_timeout = noaa_timeout
        self.open_meteo_timeout = open_meteo_timeout

        self.forecasts_cache: Dict[str, WeatherForecast] = {}
        # Cache gridpoint lookups: location → (office, gridX, gridY)
        self._grid_cache: Dict[str, Tuple[str, int, int]] = {}
        # Track API failures per session for fallback decisions
        self._open_meteo_failures: int = 0
        self._open_meteo_successes: int = 0
        self._noaa_failures: int = 0
        self._noaa_successes: int = 0

        # Calibration overrides loaded from bt_calibration table.
        # Key: city (e.g. "NYC"), Value: dict with recommended_min_edge_bps, confidence_floor.
        # Populated by load_calibration(). Falls back to constructor defaults if empty.
        self._calibration: Dict[str, Dict] = {}

    def load_calibration(self, db_path: str = "trading.db") -> int:
        """
        Load per-city calibration data from the bt_calibration table written by the
        backtest pipeline. Overrides min_edge_bps and min_confidence per city based
        on historical model accuracy.

        Called once on startup by main.py (or can be called periodically to pick up
        fresh backtest results).

        Returns number of calibration rows loaded.
        """
        import sqlite3
        from datetime import datetime as _dt

        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='bt_calibration'"
                )
                if not cur.fetchone():
                    logger.debug("bt_calibration table not found — using default thresholds")
                    return 0

                current_month = _dt.now().month
                cur = conn.execute(
                    """SELECT city, recommended_min_edge_bps, confidence_floor,
                              rmse_f, win_rate, n_signals
                       FROM bt_calibration
                       WHERE month = ?
                       ORDER BY city""",
                    (current_month,),
                )
                rows = cur.fetchall()

            if not rows:
                # Try adjacent months if current month has no data
                cur_month_fallback = [(current_month - 1) % 12 or 12,
                                      (current_month + 1) % 12 or 12]
                with sqlite3.connect(db_path) as conn:
                    placeholders = ",".join("?" * len(cur_month_fallback))
                    cur = conn.execute(
                        f"""SELECT city, recommended_min_edge_bps, confidence_floor,
                                  rmse_f, win_rate, n_signals
                           FROM bt_calibration
                           WHERE month IN ({placeholders})
                           ORDER BY city""",
                        cur_month_fallback,
                    )
                    rows = cur.fetchall()

            loaded = 0
            for city, rec_edge, conf_floor, rmse, win_rate, n_signals in rows:
                if n_signals and n_signals < 5:
                    # Not enough data to trust — skip this city's calibration
                    logger.debug(f"Calibration for {city}: too few signals ({n_signals}) — using default")
                    continue
                self._calibration[city] = {
                    "min_edge_bps": rec_edge or self.min_edge_bps,
                    "confidence_floor": conf_floor or self.min_confidence,
                    "rmse_f": rmse,
                    "win_rate": win_rate,
                }
                loaded += 1
                logger.info(
                    f"Calibration loaded for {city}: "
                    f"min_edge={rec_edge:.0f}bps, "
                    f"conf_floor={conf_floor:.2f}, "
                    f"RMSE={rmse:.2f if rmse else 'N/A'}°F, "
                    f"win_rate={win_rate:.0% if win_rate else 'N/A'} "
                    f"(n={n_signals})"
                )

            if loaded:
                logger.info(f"Calibration: loaded {loaded} city overrides from bt_calibration")
            else:
                logger.info("Calibration: no city overrides found — using constructor defaults")

            return loaded

        except Exception as e:
            logger.warning(f"Failed to load calibration from {db_path}: {e}")
            return 0

    def _get_city_thresholds(self, city: str) -> tuple:
        """Return (min_edge_bps, min_confidence) for a city, using calibration if available."""
        cal = self._calibration.get(city)
        if cal:
            return cal["min_edge_bps"], cal["confidence_floor"]
        return self.min_edge_bps, self.min_confidence

    # ── Open-Meteo Ensemble API helpers ──────────────────────────────────

    def fetch_open_meteo_forecast(
        self,
        location: str,
        target_date: datetime,
    ) -> Optional[WeatherForecast]:
        """
        Fetch ECMWF ensemble forecast data from Open-Meteo API.

        The Open-Meteo ensemble API provides 50-member ECMWF IFS ensemble data,
        which is a TRUE ensemble (not single-model hourly data like NOAA).
        Each member represents an independent model run with its own forecast.

        Workflow:
        1. Get latitude/longitude from CITY_COORDS
        2. Call Open-Meteo ensemble API with models=ecmwf_ifs025
        3. Parse hourly temperature data for each of 50 members
        4. For each member, find the daily high (max hourly temp on target_date)
        5. Build ensemble_members list with daily highs from all 50 members
        6. Calculate probability distributions from the ensemble

        Args:
            location: Location identifier (e.g., "NYC", "LAX")
            target_date: Target forecast date

        Returns:
            WeatherForecast object with 50-member ensemble, or None on failure
        """
        ensemble_members: List[Dict[str, float]] = []
        model_name = "OPEN_METEO_ECMWF"

        try:
            coords = CITY_COORDS.get(location)
            if not coords:
                raise ValueError(f"No coordinates mapped for location '{location}'")

            lat, lon = coords

            # Format dates for Open-Meteo API (YYYY-MM-DD)
            date_str = target_date.strftime("%Y-%m-%d")

            # Build request parameters
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": "temperature_2m",
                "models": "ecmwf_ifs025",  # 50-member ECMWF ensemble
                "start_date": date_str,
                "end_date": date_str,
                "temperature_unit": "fahrenheit",
                "timezone": "auto",
            }

            headers = {
                "User-Agent": "(algo-trading-desk, mattbrunetto215@gmail.com)",
                "Accept": "application/json",
            }

            logger.info(f"Fetching Open-Meteo ensemble for {location} on {date_str}")
            resp = requests.get(
                self.open_meteo_api_base,
                params=params,
                headers=headers,
                timeout=self.open_meteo_timeout,
            )
            resp.raise_for_status()
            data = resp.json()

            # Extract hourly data structure
            hourly = data.get("hourly", {})
            if not hourly:
                raise ValueError("No hourly data in Open-Meteo response")

            time_list = hourly.get("time", [])
            if not time_list:
                raise ValueError("No time values in Open-Meteo response")

            # Parse per-member temperature data (temperature_2m_member01 through member50)
            # For each member, find the daily high (max hourly temp on target_date)
            for member_idx in range(1, 51):  # 50 members
                member_key = f"temperature_2m_member{member_idx:02d}"
                member_temps = hourly.get(member_key, [])

                if not member_temps:
                    logger.warning(f"No data for {member_key}")
                    continue

                # Find max hourly temperature for this member on the target date
                daily_high_f = max(member_temps) if member_temps else None
                if daily_high_f is None:
                    continue

                # Add this member's daily high to ensemble
                # Note: Open-Meteo gives daily data; we set precip to 0 (not provided
                # in basic ensemble API; could be extended with preciption_sum if needed)
                ensemble_members.append({
                    "temp": float(daily_high_f),
                    "precip": 0.0,
                })

            if not ensemble_members:
                raise ValueError(
                    f"Failed to extract ensemble members for {location} on {date_str}"
                )

            self._open_meteo_successes += 1
            logger.info(
                f"Open-Meteo ensemble for {location} on {date_str}: "
                f"{len(ensemble_members)} members extracted"
            )

            # Brief rate-limit courtesy
            _time.sleep(0.3)

        except Exception as e:
            self._open_meteo_failures += 1
            logger.warning(
                f"Open-Meteo fetch failed for {location} on {target_date.date()}: {e} "
                f"[failures={self._open_meteo_failures}, successes={self._open_meteo_successes}]"
            )
            return None

        # ── Build WeatherForecast ────────────────────────────────────────
        try:
            temps = [m["temp"] for m in ensemble_members]
            precips = [m["precip"] for m in ensemble_members]

            mean_temp = float(np.mean(temps))
            std_temp = float(np.std(temps)) if len(temps) > 1 else 2.0
            mean_precip = float(np.mean(precips))

            # Probability of precip > 0.1 inches (all zeros in Open-Meteo basic response)
            prob_precip = float(np.mean([p > 0.1 for p in precips])) if precips else 0.0

            # Confidence: 50-member true ensemble gets high confidence
            # Ensemble spread influences confidence slightly
            base_confidence = 0.90  # Higher baseline for true ensemble
            spread_factor = 1.0 / (1.0 + std_temp / 4.0)  # Tighter → higher confidence
            confidence = min(0.98, base_confidence + (0.08 * spread_factor))

            forecast = WeatherForecast(
                location=location,
                date=target_date,
                ensemble_members=ensemble_members,
                mean_temp_f=mean_temp,
                std_temp_f=std_temp,
                mean_precip_in=mean_precip,
                prob_precip_threshold=prob_precip,
                confidence_score=confidence,
                model_name=model_name,
                issued_time=datetime.utcnow(),
            )

            self.forecasts_cache[location] = forecast
            logger.info(
                f"Forecast ready [{model_name}] {location}: "
                f"mean_temp={mean_temp:.1f}F (std={std_temp:.1f}), "
                f"ensemble_size={len(ensemble_members)}, confidence={confidence:.2%}"
            )

            return forecast

        except Exception as e:
            logger.error(f"Failed to build Open-Meteo forecast for {location}: {e}")
            return None

    # ── NOAA API helpers ──────────────────────────────────────────────────

    def _resolve_gridpoint(self, location: str) -> Optional[Tuple[str, int, int]]:
        """
        Resolve a location code to an NWS grid office / X / Y.

        Calls GET /points/{lat},{lon} once per location and caches the result.
        Returns (office, gridX, gridY) or None on failure.
        """
        if location in self._grid_cache:
            return self._grid_cache[location]

        coords = CITY_COORDS.get(location)
        if not coords:
            logger.warning(f"No coordinates mapped for location '{location}'")
            return None

        lat, lon = coords
        url = f"{self.noaa_api_base}/points/{lat},{lon}"
        headers = {
            "User-Agent": "(algo-trading-desk, mattbrunetto215@gmail.com)",
            "Accept": "application/geo+json",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=self.noaa_timeout)
            resp.raise_for_status()
            props = resp.json().get("properties", {})
            office = props.get("gridId")          # e.g. "OKX"
            grid_x = int(props.get("gridX", 0))
            grid_y = int(props.get("gridY", 0))
            if not office:
                logger.error(f"No gridId returned for {location} ({lat},{lon})")
                return None

            self._grid_cache[location] = (office, grid_x, grid_y)
            logger.info(f"Grid resolved: {location} → {office}/{grid_x},{grid_y}")
            return (office, grid_x, grid_y)

        except Exception as e:
            logger.error(f"Grid resolution failed for {location}: {e}")
            return None

    def _fetch_raw_gridpoint(
        self, office: str, grid_x: int, grid_y: int
    ) -> Optional[dict]:
        """
        Fetch the raw gridpoint forecast data from NWS.

        GET /gridpoints/{office}/{X},{Y}

        Returns the full JSON properties dict which contains time-series for
        temperature, maxTemperature, minTemperature, probabilityOfPrecipitation,
        quantitativePrecipitation, etc.
        """
        url = f"{self.noaa_api_base}/gridpoints/{office}/{grid_x},{grid_y}"
        headers = {
            "User-Agent": "(algo-trading-desk, mattbrunetto215@gmail.com)",
            "Accept": "application/geo+json",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=self.noaa_timeout)
            resp.raise_for_status()
            return resp.json().get("properties", {})
        except Exception as e:
            logger.error(f"Gridpoint fetch failed ({office}/{grid_x},{grid_y}): {e}")
            return None

    @staticmethod
    def _extract_values_for_date(
        time_series: List[dict], target_date: datetime
    ) -> List[float]:
        """
        Extract numeric values from an NWS time-series that fall on *target_date*.

        NWS validTime format: "2026-03-25T06:00:00+00:00/PT1H"
        We parse the start time, check if its date matches target_date, and
        collect all matching values.
        """
        values: List[float] = []
        target_str = target_date.strftime("%Y-%m-%d")

        for entry in time_series:
            val = entry.get("value")
            if val is None:
                continue
            valid_time = entry.get("validTime", "")
            # validTime starts with ISO datetime; date is the first 10 chars
            if valid_time[:10] == target_str:
                values.append(float(val))

        return values

    @staticmethod
    def _c_to_f(celsius: float) -> float:
        """Convert Celsius to Fahrenheit."""
        return celsius * 9.0 / 5.0 + 32.0

    # ── Main forecast fetch ─────────────────────────────────────────────

    def fetch_forecast(
        self,
        location: str,
        target_date: datetime,
    ) -> Optional[WeatherForecast]:
        """
        Fetch weather forecast for a location and date, trying multiple sources.

        Tries data sources in priority order:
        1. Open-Meteo ensemble (50-member ECMWF) — PRIMARY
        2. NOAA NWS gridpoint — FALLBACK
        3. Synthetic ensemble — FINAL FALLBACK

        Args:
            location: Location identifier (e.g., "NYC", "LAX")
            target_date: Target forecast date

        Returns:
            WeatherForecast object, or None if all data sources fail
        """
        # Try Open-Meteo first (primary source)
        forecast = self.fetch_open_meteo_forecast(location, target_date)
        if forecast:
            return forecast

        # Fall back to NOAA
        logger.info(f"Open-Meteo unavailable for {location}, falling back to NOAA")
        forecast = self._fetch_noaa_forecast(location, target_date)
        if forecast:
            return forecast

        # If all sources fail, return None and let caller handle it
        logger.error(f"All forecast sources failed for {location} on {target_date.date()}")
        return None

    def _fetch_noaa_forecast(
        self,
        location: str,
        target_date: datetime,
    ) -> Optional[WeatherForecast]:
        """
        Fetch NOAA gridpoint forecast data for a location and date (FALLBACK source).

        Used as fallback when Open-Meteo ensemble API is unavailable.

        Workflow:
        1. Resolve location → NWS gridpoint via /points/{lat},{lon}
        2. Fetch raw gridpoint data via /gridpoints/{office}/{X},{Y}
        3. Extract hourly temperature values for target_date
        4. Build ensemble-like members from the hourly spread
        5. Fall back to synthetic ensemble if API call fails

        Args:
            location: Location identifier (e.g., "NYC", "LAX")
            target_date: Target forecast date

        Returns:
            WeatherForecast object, or None if data unavailable
        """
        ensemble_members: List[Dict[str, float]] = []
        model_name = "NOAA_NBM"
        confidence_boost = 0.0  # extra confidence for real data

        try:
            grid = self._resolve_gridpoint(location)
            if grid is None:
                raise ValueError(f"Could not resolve gridpoint for {location}")

            office, gx, gy = grid
            props = self._fetch_raw_gridpoint(office, gx, gy)
            if props is None:
                raise ValueError(f"Gridpoint data unavailable for {location}")

            # ── Parse temperature ────────────────────────────────────────
            # The raw gridpoint exposes "temperature" (hourly, °C) and also
            # "maxTemperature" / "minTemperature" (daily, °C).
            # We prefer hourly data for distribution building.
            temp_series = props.get("temperature", {}).get("values", [])
            temp_values_c = self._extract_values_for_date(temp_series, target_date)

            if not temp_values_c:
                # Try maxTemperature / minTemperature as fallback
                max_t = props.get("maxTemperature", {}).get("values", [])
                min_t = props.get("minTemperature", {}).get("values", [])
                max_vals = self._extract_values_for_date(max_t, target_date)
                min_vals = self._extract_values_for_date(min_t, target_date)

                if max_vals and min_vals:
                    # Synthesize a distribution from daily max/min
                    hi = max(max_vals)
                    lo = min(min_vals)
                    mid = (hi + lo) / 2.0
                    spread = (hi - lo) / 4.0  # rough std estimate
                    temp_values_c = list(np.linspace(lo, hi, 12))
                    logger.info(f"Used max/min temps for {location} on {target_date.date()}: {lo:.1f}-{hi:.1f}°C")
                else:
                    raise ValueError(
                        f"No temperature data for {location} on {target_date.date()}"
                    )

            # Convert to Fahrenheit
            temp_values_f = [self._c_to_f(t) for t in temp_values_c]

            # ── Parse precipitation ──────────────────────────────────────
            precip_series = props.get("quantitativePrecipitation", {}).get("values", [])
            precip_values_mm = self._extract_values_for_date(precip_series, target_date)
            # Convert mm → inches (1 mm = 0.03937 in)
            precip_values_in = [p * 0.03937 for p in precip_values_mm]

            # Also grab probability of precipitation (%)
            pop_series = props.get("probabilityOfPrecipitation", {}).get("values", [])
            pop_values = self._extract_values_for_date(pop_series, target_date)

            # ── Build ensemble members ──────────────────────────────────
            # Each hourly observation becomes an "ensemble member".
            # This isn't a true ensemble (it's a single-model time-series)
            # but for daily-high contracts it captures the forecast distribution
            # well because the hourly values span the day's range.
            n_temps = len(temp_values_f)
            n_precip = len(precip_values_in)

            for i in range(max(n_temps, 1)):
                temp_f = temp_values_f[i] if i < n_temps else np.mean(temp_values_f)
                precip_in = precip_values_in[i % n_precip] if n_precip > 0 else 0.0
                ensemble_members.append({"temp": float(temp_f), "precip": float(precip_in)})

            self._noaa_successes += 1
            confidence_boost = 0.05  # reward real data with higher confidence
            logger.info(
                f"NOAA data for {location} on {target_date.date()}: "
                f"{n_temps} temp points, {n_precip} precip points"
            )

            # Brief rate-limit courtesy (NWS asks for <1 req/sec)
            _time.sleep(0.5)

        except Exception as e:
            self._noaa_failures += 1
            logger.warning(
                f"NOAA API failed for {location} ({e}), falling back to synthetic ensemble "
                f"[failures={self._noaa_failures}, successes={self._noaa_successes}]"
            )
            ensemble_members = self._generate_mock_ensemble(target_date, location)
            model_name = "SYNTHETIC_FALLBACK"

        # ── Build WeatherForecast ────────────────────────────────────────
        try:
            temps = [m["temp"] for m in ensemble_members]
            precips = [m["precip"] for m in ensemble_members]

            mean_temp = float(np.mean(temps))
            std_temp = float(np.std(temps)) if len(temps) > 1 else 5.0
            mean_precip = float(np.mean(precips))

            # Probability of precip > 0.1 inches
            prob_precip = float(np.mean([p > 0.1 for p in precips]))

            # Confidence: tighter forecast variance → higher confidence
            base_confidence = 0.8 + (0.2 * (1 / (1 + std_temp / 5)))
            confidence = min(0.95, base_confidence + confidence_boost)

            forecast = WeatherForecast(
                location=location,
                date=target_date,
                ensemble_members=ensemble_members,
                mean_temp_f=mean_temp,
                std_temp_f=std_temp,
                mean_precip_in=mean_precip,
                prob_precip_threshold=prob_precip,
                confidence_score=confidence,
                model_name=model_name,
                issued_time=datetime.utcnow(),
            )

            self.forecasts_cache[location] = forecast
            logger.info(
                f"Forecast ready [{model_name}] {location}: "
                f"mean_temp={mean_temp:.1f}F (std={std_temp:.1f}), "
                f"precip_prob={prob_precip:.2%}, confidence={confidence:.2%}"
            )

            return forecast

        except Exception as e:
            logger.error(f"Failed to build forecast for {location}: {e}")
            return None

    def calculate_probability(
        self,
        forecast_data: WeatherForecast,
        threshold: float,
        weather_type: str = "temperature",
    ) -> float:
        """
        Estimate the probability that a weather metric exceeds a threshold.

        CRITICAL CONTEXT: Kalshi KXHIGH/KXLOW contracts settle on the DAILY HIGH
        temperature, NOT on any arbitrary hourly reading. So the question we must
        answer is: P(daily_high > threshold).

        Since our "ensemble members" are actually hourly temps from a single NWS
        forecast (not independent model runs), we cannot just count how many hours
        exceed the threshold. Instead we:

        1. Extract the forecasted daily high = max(hourly temps)
        2. Estimate forecast uncertainty (std) from the hourly spread and a
           model-error term (NWS day-1 forecasts have ~2-3°F RMSE)
        3. Model the true daily high as Normal(forecast_high, uncertainty)
        4. Compute P(true_high > threshold) from that distribution

        For precipitation: uses empirical probability (fraction of periods with
        precip > threshold), since precip contracts are about occurrence, not max.

        Args:
            forecast_data: WeatherForecast with hourly ensemble members
            threshold: Threshold value (temperature in °F or precip in inches)
            weather_type: "temperature" or "precipitation"

        Returns:
            Probability (0-1) that the daily metric exceeds the threshold
        """
        if not forecast_data.ensemble_members:
            logger.warning("No ensemble members available")
            return 0.5

        if weather_type == "temperature":
            temps = [m.get("temp", 0) for m in forecast_data.ensemble_members]
            if not temps:
                return 0.5

            # The forecast daily high is the max of all hourly readings
            forecast_high = max(temps)

            # Uncertainty in the daily high comes from two sources:
            # 1. NWS forecast model error: ~2.5°F RMSE for day-1 high forecasts
            # 2. Internal spread: if hourly temps are tightly clustered near the
            #    max, we're more confident; if spread is large, less confident.
            # We combine them in quadrature.
            nws_model_error = 2.5  # °F, empirical RMSE for NWS day-1 high temp
            # Spread near the top: std of the top quartile of hourly temps
            n = len(temps)
            top_quartile = sorted(temps)[max(0, n - n // 4):]
            internal_spread = float(np.std(top_quartile)) if len(top_quartile) > 1 else 1.0
            total_uncertainty = float(np.sqrt(nws_model_error**2 + internal_spread**2))

            # P(true_daily_high > threshold) using normal CDF
            # = 1 - Phi((threshold - forecast_high) / uncertainty)
            z = (threshold - forecast_high) / max(total_uncertainty, 0.5)
            # Normal CDF: Phi(z) = 0.5 * (1 + erf(z/sqrt(2)))
            import math as _math
            probability = 1.0 - 0.5 * (1.0 + _math.erf(z / _math.sqrt(2)))

        elif weather_type == "precipitation":
            # For precip, empirical probability is appropriate since the question
            # is "will it rain at all / exceed X inches" across the day
            precips = [m.get("precip", 0) for m in forecast_data.ensemble_members]
            if not precips:
                return 0.5
            probability = float(np.mean([p > threshold for p in precips]))

        else:
            logger.warning(f"Unknown weather type: {weather_type}")
            return 0.5

        # Clamp to [0.01, 0.99] — never be fully certain
        probability = max(0.01, min(0.99, probability))

        return float(probability)

    def find_mispriced_contracts(
        self,
        kalshi_markets: List[Dict[str, any]],
        probabilities: Dict[str, float],
        location: str,
    ) -> List[ContractSignal]:
        """
        Compare model probabilities to market prices and identify mispricings.

        For each contract, extracts the implied probability from the contract price.
        In Kalshi, the contract trades between 0 and 100 (equivalent to 0-1 probability).
        We compare our model probability to the market probability and generate
        signals when the edge exceeds our threshold.

        Args:
            kalshi_markets: List of contract dicts with keys: contract_id, price, metric
            probabilities: Dict mapping metric names to our calculated probabilities
            location: Location these markets are for

        Returns:
            List of ContractSignal objects, filtered for sufficient edge/confidence
        """
        signals = []

        for market in kalshi_markets:
            contract_id = market.get("contract_id", "")
            market_price = market.get("price", 50)  # Price between 0-100
            metric = market.get("metric", "")

            # Extract implied probability from market price
            # Note: assumes price = probability * 100, which is standard for Kalshi
            market_prob = market_price / 100.0

            # Get our model probability.
            # TEMP_BELOW contracts pay YES if temp is BELOW threshold — the inverse of
            # TEMP_ABOVE. Map to the corresponding ABOVE probability and invert it.
            if "TEMP_BELOW" in metric:
                above_metric = metric.replace("TEMP_BELOW", "TEMP_ABOVE")
                if above_metric in probabilities:
                    prob_above = probabilities[above_metric]
                else:
                    # Threshold not pre-computed — extract it and calculate on the fly
                    try:
                        parts = above_metric.split("_TEMP_ABOVE_")
                        key_prefix = parts[0]  # e.g. "NYC_20260325"
                        threshold_val = float(parts[1])
                        # Find a forecast for this location/date in probabilities context
                        # by deriving from any existing key with same prefix
                        matching = {k: v for k, v in probabilities.items() if k.startswith(key_prefix)}
                        if matching:
                            # Use nearest computed threshold as proxy (close enough for signal filtering)
                            nearest_key = min(matching.keys(),
                                key=lambda k: abs(float(k.split("_TEMP_ABOVE_")[-1]) - threshold_val)
                                if "_TEMP_ABOVE_" in k else 999)
                            nearest_val = float(nearest_key.split("_TEMP_ABOVE_")[-1])
                            if threshold_val <= nearest_val:
                                prob_above = min(matching[nearest_key] + 0.05, 1.0)
                            else:
                                prob_above = max(matching[nearest_key] - 0.05, 0.0)
                        else:
                            prob_above = 0.5
                    except Exception:
                        prob_above = 0.5
                model_prob = 1.0 - prob_above
            else:
                model_prob = probabilities.get(metric, 0.5)

            # Calculate edge in basis points (1 bp = 0.01%)
            edge_bps = abs(model_prob - market_prob) * 10000

            # Use per-city calibrated thresholds if available, else global defaults
            city_min_edge, city_min_confidence = self._get_city_thresholds(location)

            # Determine signal direction
            if model_prob > market_prob + (city_min_edge / 10000):
                signal = SignalType.BUY
            elif model_prob < market_prob - (city_min_edge / 10000):
                signal = SignalType.SELL
            else:
                signal = SignalType.HOLD

            # Skip if insufficient edge
            if edge_bps < city_min_edge:
                signal = SignalType.SKIP

            # Confidence based on forecast confidence and contract trading volume
            forecast_confidence = self.forecasts_cache.get(location, WeatherForecast(
                location="", date=datetime.now(), ensemble_members=[],
                mean_temp_f=0, std_temp_f=0, mean_precip_in=0,
                prob_precip_threshold=0, confidence_score=0.5,
                model_name="", issued_time=datetime.now()
            )).confidence_score
            confidence = min(0.9, forecast_confidence * 1.1)

            # Apply calibrated confidence floor
            if confidence < city_min_confidence:
                signal = SignalType.SKIP

            # Position sizing: scale with edge and confidence
            position_size = (edge_bps / city_min_edge) * confidence if signal != SignalType.SKIP else 0.0

            contract_signal = ContractSignal(
                contract_id=contract_id,
                signal=signal,
                model_probability=model_prob,
                market_probability=market_prob,
                edge=edge_bps,
                confidence=confidence,
                weather_metric=metric,
                recommended_position_size=position_size,
                rationale=f"Model prob {model_prob:.1%} vs market {market_prob:.1%}, "
                         f"edge {edge_bps:.0f}bps, confidence {confidence:.1%}",
            )

            signals.append(contract_signal)

        return signals

    def generate_signals(
        self,
        locations: List[str],
        target_dates: List[datetime],
        kalshi_markets: List[Dict[str, any]],
    ) -> List[ContractSignal]:
        """
        Main entry point: generate trading signals for all locations and dates.

        Workflow:
        1. Fetch forecasts for each location/date (tries Open-Meteo first, then NOAA)
        2. Calculate probabilities for each weather metric
        3. Find mispriced contracts vs market prices
        4. Return signals ready to execute

        Args:
            locations: List of location identifiers
            target_dates: List of dates to forecast
            kalshi_markets: List of available Kalshi contracts

        Returns:
            List of ContractSignal objects with actionable signals
        """
        all_signals = []
        all_probabilities = {}

        # Fetch forecasts and calculate probabilities
        for location in locations:
            for target_date in target_dates:
                forecast = self.fetch_forecast(location, target_date)
                if not forecast:
                    continue

                # Calculate probabilities for common thresholds
                # These should be calibrated to actual Kalshi contract strikes
                temp_above_70 = self.calculate_probability(forecast, 70.0, "temperature")
                temp_above_75 = self.calculate_probability(forecast, 75.0, "temperature")
                precip_above_0_1 = self.calculate_probability(forecast, 0.1, "precipitation")

                key_prefix = f"{location}_{target_date.strftime('%Y%m%d')}"
                all_probabilities[f"{key_prefix}_TEMP_ABOVE_70"] = temp_above_70
                all_probabilities[f"{key_prefix}_TEMP_ABOVE_75"] = temp_above_75
                all_probabilities[f"{key_prefix}_PRECIP_ABOVE_0.1"] = precip_above_0_1

                # Also calculate for any dynamic thresholds injected at runtime
                dynamic_thresholds = getattr(self, "_dynamic_thresholds", [])
                for threshold in dynamic_thresholds:
                    prob = self.calculate_probability(forecast, threshold, "temperature")
                    all_probabilities[f"{key_prefix}_TEMP_ABOVE_{int(threshold)}"] = prob

        # Find mispriced contracts
        for location in locations:
            signals = self.find_mispriced_contracts(
                kalshi_markets,
                all_probabilities,
                location,
            )
            all_signals.extend(signals)

        # Deduplicate by contract_id — keep highest-edge signal per contract.
        # generate_signals() iterates over all locations but passes ALL markets
        # each time, so the same contract gets evaluated once per location.
        # We only want the single best signal per unique contract.
        seen: Dict[str, "ContractSignal"] = {}
        for sig in all_signals:
            if sig.contract_id not in seen or sig.edge > seen[sig.contract_id].edge:
                seen[sig.contract_id] = sig
        all_signals = list(seen.values())

        # Filter out SKIP signals and sort by edge
        actionable_signals = [s for s in all_signals if s.signal != SignalType.SKIP]
        actionable_signals.sort(key=lambda s: s.edge, reverse=True)

        logger.info(f"Generated {len(actionable_signals)} actionable signals from {len(all_signals)} total (deduplicated)")

        return actionable_signals

    @staticmethod
    def _generate_mock_ensemble(target_date: datetime, location: str = "NYC") -> List[Dict[str, float]]:
        """
        Generate mock ensemble forecast data for testing/demo purposes.

        Creates 20 ensemble members with realistic temperature and precipitation
        distributions. In production, this would come from NOAA API.

        Args:
            target_date: Date for forecast

        Returns:
            List of ensemble member predictions
        """
        np.random.seed(hash(f"{location}{target_date.isoformat()}") % 2**32)  # Reproducible

        month = target_date.month

        # City-specific seasonal base temperatures (°F) — realistic March values
        city_temps = {
            "NYC": {(12,1,2): 35, (3,4,5): 55, (6,7,8): 80, (9,10,11): 62},
            "LAX": {(12,1,2): 65, (3,4,5): 70, (6,7,8): 85, (9,10,11): 78},
            "CHI": {(12,1,2): 28, (3,4,5): 50, (6,7,8): 78, (9,10,11): 58},
            "MIA": {(12,1,2): 70, (3,4,5): 78, (6,7,8): 90, (9,10,11): 83},
            "DEN": {(12,1,2): 35, (3,4,5): 55, (6,7,8): 85, (9,10,11): 62},
            "HOU": {(12,1,2): 55, (3,4,5): 70, (6,7,8): 92, (9,10,11): 76},
            "PHX": {(12,1,2): 60, (3,4,5): 80, (6,7,8): 105, (9,10,11): 88},
        }
        season_map = city_temps.get(location, city_temps["NYC"])
        base_temp = next(
            (v for k, v in season_map.items() if month in k), 65
        )

        ensemble = []
        for _ in range(20):
            temp = np.random.normal(base_temp, 5)
            precip = abs(np.random.lognormal(mean=-2, sigma=1.5))
            ensemble.append({"temp": float(temp), "precip": float(precip)})

        return ensemble
