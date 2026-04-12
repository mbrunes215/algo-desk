# IBKR ORB Setup Guide

## Prerequisites

1. IBKR Pro account (you already have this)
2. IBKR Desktop (TWS) installed on the trading Mac
3. `ib_insync` Python library installed

## Step 1: Install ib_insync

On the trading Mac:
```bash
pip3 install ib_insync
```

## Step 2: Enable API in TWS

1. Open IBKR Desktop (TWS)
2. Go to: **Edit → Global Configuration → API → Settings**
3. Check: **Enable ActiveX and Socket Clients**
4. Set **Socket port** to `7497` (this is the paper trading port)
5. Check: **Allow connections from localhost only** (security)
6. Uncheck: **Read-Only API** (we need to place orders)
7. Click **Apply** then **OK**

## Step 3: Log into Paper Trading

TWS has a separate paper trading login:
- Username: same as your live account
- Password: same as your live account
- But you log in via: **Login → Paper Trading** (not Live Trading)

Paper trading port is 7497, live is 7496. The code defaults to 7497.

## Step 4: Subscribe to Market Data (if needed)

For /MNQ real-time data, you need:
- **US Futures Value Bundle** (~$10/month) — covers CME, CBOT, NYMEX
- Or individually: CME Real-Time data

Paper trading accounts may get delayed data by default. Check:
**Account → Settings → Market Data Subscriptions**

Delayed data works fine for backtesting but you want real-time for live.

## Step 5: Pull Latest Code

On the trading Mac:
```bash
cd ~/Desktop/algo-desk
git pull
```

## Step 6: Test the Connection

```bash
cd ~/Desktop/algo-desk

# Check strategy status (no connection needed)
python3 -m strategies.ibkr_orb.run_orb --status

# Test connection to TWS (TWS must be running)
python3 -c "
from ib_insync import IB
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=99)
print('Connected!')
print(f'Account: {ib.managedAccounts()}')
for v in ib.accountValues():
    if v.tag == 'NetLiquidation':
        print(f'NLV: \${float(v.value):,.0f}')
ib.disconnect()
"
```

## Step 7: Run a Backtest

This fetches historical data from IBKR and replays it:
```bash
# Backtest last 20 trading days
python3 -m strategies.ibkr_orb.run_orb --backtest --days 20

# Backtest with different params
python3 -m strategies.ibkr_orb.run_orb --backtest --days 30 --range-minutes 30 --rr 1.5
```

## Step 8: Run Live (Paper)

```bash
# Start the ORB strategy (paper trading)
python3 -m strategies.ibkr_orb.run_orb --live

# With custom params
python3 -m strategies.ibkr_orb.run_orb --live --contracts 1 --max-loss 100
```

The strategy will:
1. Connect to TWS
2. Qualify the /MNQ front-month contract
3. Backfill today's bars (if starting mid-session)
4. Stream real-time 5-second bars, aggregate to 1-minute
5. Form the opening range (9:30-9:45 ET)
6. Watch for breakout and enter with bracket order
7. Manage position until stop/target/time stop
8. Shut down at 15:55 ET

## Running as a Background Process

```bash
# Start in background with logging
nohup python3 -m strategies.ibkr_orb.run_orb --live >> logs/orb.log 2>&1 &
echo $! > logs/orb.pid

# Check logs
tail -20 logs/orb.log

# Stop
kill $(cat logs/orb.pid)
```

## Troubleshooting

**"Failed to connect to TWS"**
→ Make sure TWS is running and API is enabled (Step 2)
→ Check the port: paper=7497, live=7496

**"Could not qualify contract: MNQ"**
→ Make sure you have futures trading permissions on your IBKR account
→ Check Account → Settings → Trading Permissions → Futures

**"Incomplete market data"**
→ You may need a market data subscription (Step 4)
→ Delayed data will show as `nan` for bid/ask

**"Order rejected"**
→ Check margin: 1 MNQ needs ~$2K
→ Check that futures trading is enabled on the account
