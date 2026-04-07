# Quick Start Guide

## Installation

```bash
pip install -r requirements.txt
```

## Basic Setup

1. Create configuration files:
```bash
cp .env.example .env
# Edit .env with your settings
```

2. Initialize database:
```bash
python -c "from data.storage import SessionFactory; SessionFactory.initialize('sqlite:///trading.db')"
```

## Running the Application

### Start Dashboard
```bash
python main.py --dashboard
```

### Paper Trading (Safe Testing)
```bash
python main.py --paper --dashboard
```

### Run Specific Strategy
```bash
python main.py --strategy WeatherStrategy --paper
```

## Testing

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test File
```bash
pytest tests/test_risk.py -v
pytest tests/test_strategies.py -v
```

### Run with Coverage
```bash
pytest tests/ --cov=. --cov-report=html
```

### Test Specific Class/Function
```bash
pytest tests/test_risk.py::TestKillSwitch -v
pytest tests/test_strategies.py::TestWeatherStrategy::test_weather_probability_calculation -v
```

## Key Classes Quick Reference

### Monitoring
```python
from monitoring import TradingDashboard, AlertManager, HealthChecker

# Dashboard
dashboard = TradingDashboard()
dashboard.refresh(positions, daily_pnl, daily_pnl_percent,
                 strategy_status, recent_trades, system_health)
print(dashboard.display())

# Alerts
alerts = AlertManager(enable_email=False)
alerts.send_alert(AlertLevel.WARNING, "Market volatility high")
alerts.alert_kill_switch_triggered("Stop loss triggered")

# Health
health = HealthChecker()
report = health.run_all_checks()
print(f"Status: {report.overall_status}")
```

### Data Pipeline
```python
from data.pipelines import MarketDataPipeline

pipeline = MarketDataPipeline()
data = pipeline.fetch_ibkr_historical("AAPL", "1 M", "1 day")
historical = pipeline.get_historical("AAPL", start_date, end_date)
```

### Database
```python
from data.storage import SessionFactory, Trade, Position

SessionFactory.initialize("sqlite:///trading.db")
session = SessionFactory.get_session()

# Add trade
trade = Trade(
    strategy_name="MyStrategy",
    symbol="AAPL",
    trade_type=TradeType.BUY,
    quantity=100,
    price=150.25,
    status=TradeStatus.FILLED
)
session.add(trade)
session.commit()
```

### Testing with Fixtures
```python
def test_something(test_database, db_session, sample_position):
    # test_database: Path to test DB
    # db_session: SQLAlchemy session
    # sample_position: Pre-made position data
    pass
```

## Common Commands

```bash
# Check syntax
python -m py_compile monitoring/*.py data/**/*.py

# List tests
pytest tests/ --collect-only

# Run with detailed output
pytest tests/test_risk.py -vv -s

# Run only unit tests
pytest tests/ -m unit

# Generate coverage report
pytest tests/ --cov --cov-report=term-missing

# Run tests in parallel
pytest tests/ -n auto

# Stop on first failure
pytest tests/ -x

# Run only failed tests
pytest tests/ --lf
```

## Logging Output

The application logs to:
- **Console**: `INFO` and above
- **File**: `logs/trading_desk.log` with all levels

To capture logs:
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Event occurred")
logger.warning("Warning message")
logger.error("Error occurred")
```

## Dashboard Features

### Terminal Dashboard
```python
dashboard = TradingDashboard()
print(dashboard.display())  # Pretty-printed terminal output
summary = dashboard.get_status_summary()  # JSON summary
```

### HTML Dashboard
```python
from monitoring import generate_html_dashboard

generate_html_dashboard(
    positions=positions,
    pnl=daily_pnl,
    pnl_percent=daily_pnl_percent,
    trades=recent_trades,
    health=system_health,
    strategy_status=strategy_status,
    output_path="dashboard.html"
)
# Open dashboard.html in browser
```

## Alert Configuration

```python
from monitoring import AlertManager, AlertLevel

alerts = AlertManager(
    smtp_host="smtp.gmail.com",
    smtp_port=587,
    smtp_user="your-email@gmail.com",
    smtp_password="app-password",
    to_emails=["alerts@example.com"],
    max_alerts_per_hour=10,
    enable_email=True
)

# Custom handler
def my_alert_handler(alert):
    # Do something with alert
    pass

alerts.add_custom_handler(my_alert_handler)

# Send various alert types
alerts.alert_kill_switch_triggered("Manual stop")
alerts.alert_connection_lost("IBKR")
alerts.alert_strategy_error("WeatherStrategy", "Division by zero")
alerts.alert_pnl_threshold(-5000, -1000)
```

## Troubleshooting

### Database Issues
```bash
# Reset database
rm trading.db
python -c "from data.storage import SessionFactory; SessionFactory.initialize('sqlite:///trading.db')"
```

### Import Errors
```bash
# Verify module structure
python -c "from monitoring import TradingDashboard; print('OK')"
```

### Test Failures
```bash
# Run with verbose output
pytest tests/test_risk.py -vv -s

# Debug with pdb
pytest tests/test_risk.py --pdb

# Show print statements
pytest tests/test_risk.py -s
```

## Performance Tips

1. **Incremental data fetching**: Use `incremental=True` in market data pipeline
2. **Alert rate limiting**: Configure `max_alerts_per_hour` to prevent spam
3. **Database indexing**: Market data queries use indexed timestamps
4. **Batch operations**: Insert multiple records in single transaction

## Next Steps

1. Implement actual broker connections (IBKR, Kalshi APIs)
2. Create strategy implementations with real signal logic
3. Add backtesting framework
4. Implement order execution logic
5. Add position sizing calculations
6. Create performance attribution reporting
