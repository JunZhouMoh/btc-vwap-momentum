# Configuration Guide

Part of the **[PolyBullLabs Polymarket suite](https://github.com/PolyBullLabs/polymakret-5min-15min-1hour-arbitrage-bot)** · [@terauss](https://t.me/terauss) · [Suite README (all bots)](../README.md)

This file explains **every parameter** in `config.json`. The bot is highly configurable -- you can fine-tune the strategy, risk, execution speed, hedging, and notifications without touching any code.

---

## Portfolio hosting

Portfolio following runs separately with `python portfolio.py`, with its own UI
and persisted settings; the BTC `config.json` does not control this mode.

| Environment variable | Purpose |
|---|---|
| `PORT` | Listening port; defaults to `8082`. `--port` takes precedence. |
| `PORTFOLIO_DASHBOARD_USER` | HTTP Basic login username; defaults to `admin`. |
| `PORTFOLIO_DASHBOARD_PASSWORD` | Required on Railway or when public hostnames are configured; at least 16 printable ASCII characters. Optional for localhost-only use. |
| `RAILWAY_PUBLIC_DOMAIN` | Railway-provided public hostname; automatically added to trusted hosts. |
| `PORTFOLIO_ALLOWED_HOSTS` | Additional exact hostnames, comma-separated; no schemes, ports or wildcards. |
| `DATA_DIR` | Persistent volume root on Railway; defaults to `/data`. Mount a volume there. |
| `PORTFOLIO_STATE_PATH` | SQLite state path; defaults to `logs/portfolio_follow.sqlite3`, resolved under `DATA_DIR` on Railway. `--state` takes precedence. |

Railway is detected from `RAILWAY_PROJECT_ID` or `RAILWAY_ENVIRONMENT`.
The server binds to `0.0.0.0` there and requires dashboard credentials before
starting. Locally it continues to bind to `127.0.0.1`. Use HTTPS for hosted
access. Trusted public-domain origins are checked against HTTPS, independently
of the internal HTTP connection behind Railway's TLS proxy; arbitrary forwarded
headers cannot select the allowed origin.

Set the Railway healthcheck path to `/healthz`. GET/HEAD are unauthenticated
and report process/worker readiness (not Polymarket API availability); a failed
worker returns HTTP 503. Every other endpoint, including API documentation,
requires HTTP Basic authentication when a password is configured.

Use one replica, a separate volume for each service, and different live wallets
for BTC and portfolio strategies. The portfolio process keeps a single-writer
SQLite lock and always pauses copying on startup. A redeployment is not a
signal to resume real orders automatically.

### Perps configuration

`/perps` runs within the same authenticated portfolio service and SQLite
database, using separate tables/settings. Prediction and Perps workers can run
independently; neither starts the BTC strategy. Both are paused on restart.

| Environment variable | Purpose |
|---|---|
| `PERPS_PRIVATE_KEY` | Signer for the dedicated, already registered/funded Perps account. No fallback to `PRIVATE_KEY`. Keep it in Railway Variables, never source control. |
| `PERPS_WALLET_ADDRESS` | Registered account wallet passed to the SDK; it must match the account controlled by the signer. Changing the live account requires a different state database. |
| `PERPS_LOCATION_VERIFIED` | Defaults to `false`. Set `true` only after independently verifying the single operator's current location and eligibility. This trusted deployment configuration is not automatic geolocation/KYC. |
| `PERPS_OPERATOR_COUNTRY` | Verified operator ISO alpha-2 country, not the hosting region. Required for live. US, CA, CU, IR, KP and SY are blocked. |
| `PERPS_OPERATOR_REGION` | Operator subdivision; a valid `UA-xx` code is required for Ukraine. Crimea/Sevastopol (`UA-43`, `UA-40`), Donetsk (`UA-14`) and Luhansk (`UA-09`) are blocked. |
| `SSL_CERT_FILE` | Optional approved CA bundle for networks with a private TLS CA. Do not disable certificate validation. |

Review the latest official geographic restrictions before every deployment or
operator-location change; the list can change and additional legal restrictions
may apply. Do not deploy this single-operator tool as a multi-user brokerage.
Location checks run again before each live submission, including reductions.
Simulation and public data need no trading credentials or location attestation.

Perps settings are saved in the UI, not BTC `config.json`:

- Fixed sizing defaults to **$25 notional**, not $25 collateral. Percentage
  sizing scales only the opening portion of a source fill/flip.
- Defaults: **1x isolated leverage**, $25 max increase, $50 max per instrument
  across followed wallets, $100 total gross notional, and $1,000 paper equity.
  Leverage can be set from 1 to 20, subject to each instrument's exchange limit.
  Effective isolated leverage is explicitly set and verified before increases;
  the exchange's potentially higher default is not used.
- Reductions scale copied quantity by the source position fraction closed and
  use reduce-only orders. Minimums, precision, lack of protected liquidity,
  partial fills and opposing followed positions can prevent a complete flip.
  Skipped/partial actions are not retried automatically.
- Gross limits use current mark values (live exchange position values for live
  preflight), without long/short offsets. An account-margin buffer is reserved
  on increases. Reductions are allowed over exposure limits. Limits do not
  cap losses, trigger stops or flatten positions after price moves.
- The public fill window is paginated completely, delayed two seconds for the
  API cache, and bounded by max fill age. History is scanned for new fills only.
  API downtime beyond that window can miss exits: monitor the dashboard and
  manage the exchange account yourself when necessary.
- Paper fills use available book liquidity. Paper equity includes copied
  realized/unrealized P&L but excludes fees, funding and liquidation mechanics.
  Live account funding/margin/liquidation data is shown separately; copied
  realized P&L subtracts actual fill fees but excludes funding and external P&L.
- Pending intents/client IDs are committed before submission. A timeout,
  nonterminal result or missing fill confirmation pauses Perps copying until
  verified reconciliation. Restarting never retries an uncertain order.
- SDK trading sessions expire (the SDK default is one week). Expired sessions
  or failed live account checks pause copying. Inspect positions and explicitly
  enable live again to establish a fresh session; no orders are retried.
- External trades/liquidations or unmanaged open orders block copying. After
  closing a position on Polymarket, pause and use **Record external / forced
  close** to clear its copied quantity only after the exchange confirms flat.
  Supply evidence; no order is submitted, and external P&L/fees/funding are not
  invented or attributed to the ledger. Other still-open positions are untouched.

The experimental Perps SDK is pinned to the tested `polymarket-client==0.20.0`.
No exchange order/deposit is needed for startup, healthchecks or simulation.

---

## market -- Which Polymarket window to trade

```
"interval_minutes": 5
```

**BTC up/down market length on Polymarket.** Must be **5** or **15**.

- **5** — slug pattern `btc-updown-5m-<unix_start>` (300s window). Example strategy: `min_elapsed_sec` ~150–210, `no_entry_before_end_sec` ~90–120.
- **15** — slug pattern `btc-updown-15m-<unix_start>` (900s window). Example strategy: `min_elapsed_sec` ~480–530, `no_entry_before_end_sec` ~300–335.

The bot aligns Chainlink anchor resets, market discovery, and elapsed-time logic to this interval. **Always** set `min_elapsed_sec` and `no_entry_before_end_sec` so they fit inside the market length (e.g. for 5m, `min_elapsed_sec` must be &lt; 300).

---

## simulation -- Paper trading (no real money)

```
"enabled": false
```

**When `true`:** the bot still connects to market WebSockets, RTDS Chainlink, runs the dashboard, and evaluates the same entry rules. **No** orders are sent to Polymarket, **no** User WebSocket for fills, **no** auto-redeemer loop. Entries are logged as instant hypothetical fills at **best ask + `entry.price_offset`**, subject to `max_entry_price` and the same contract sizing as live. Optional hedge is simulated as a placed GTD with id `SIM-HEDGE` (no on-chain or CLOB effect).

```
"separate_trading_log": true,
"trading_log_path": "logs/trading_log_sim.json"
```

If `separate_trading_log` is **true**, simulated P&L and trades are written only to `trading_log_path`, so your live `logs/trading_log.json` stays untouched. Set to **false** to append simulation results to the same file as live (not recommended if you also run live).

**Trading history for analysis (simulation only):**

- `history_csv_path` — Append-only CSV with **OPEN** rows (each simulated entry) and **CLOSE** rows (each resolved position). CLOSE rows include **trade_pnl_usd**, **cumulative_pnl_usd** after that trade, **win_rate_pct**, and **total_closed_trades**. Open in Excel / pandas.
- `history_jsonl_path` — One JSON object per line (`type`: `open` or `close`) for streaming tools. Set to `""` to disable.
- `history_summary_path` — Rewritten after every close: rolling **summary** (total PnL, wins/losses, win rate, best/worst trade) plus the full **trades** array (same data as `trading_log_path`, convenient for a single analysis file).

The main `trading_log_path` JSON also includes a **`summary`** block (totals and win rate) on each save, for both live and sim logs.

**Credentials:** With `simulation.enabled` true, `PRIVATE_KEY` and Polymarket API keys in `.env` are **not** required. You can still set Telegram tokens for notifications.

---

## strategy -- When to enter a trade

These parameters control **which signals the bot acts on**. Think of them as filters: a trade is only placed when ALL conditions pass simultaneously.

```
"min_price": 0.75
```
**Minimum token price to enter.** The bot only buys tokens priced at or above this value. Lower prices mean higher potential profit but lower probability of winning. At $0.75, you need a 75% win rate to break even. Range: 0.50 - 0.95. Start with 0.75.

```
"max_price": 0.88
```
**Maximum token price to enter.** The bot rejects tokens priced above this. Higher prices mean higher probability but tiny profit margin. At $0.88, you profit only $0.12 per contract on a win but lose $0.88 on a loss (need 88% win rate). Range: 0.80 - 0.95. Start with 0.88.

```
"min_elapsed_sec": 530
```
**Minimum seconds elapsed since market opened before allowing entry.** Each market lasts 900 seconds (15 min). This prevents entering too early when the market direction is unclear. At 530, the bot waits ~8.8 minutes. Range: 300 - 800. Higher = safer but fewer opportunities.

```
"min_deviation_pct": 3
```
**Minimum VWAP deviation (%) to trigger a signal.** VWAP = volume-weighted average price. Deviation measures how far the current price has moved from this average. A deviation of 3% means the token price is 3% above its recent average, indicating strong directional movement. Range: 0 - 15. Set to 0 to disable this filter. Higher = stricter, fewer trades.

```
"max_deviation_pct": 100
```
**Maximum VWAP deviation (%) allowed.** Rejects signals where deviation is abnormally high (potential spike/manipulation). Set to 100 to effectively disable the upper bound. To cap, try 15-25. Range: must be greater than min_deviation_pct.

```
"no_entry_before_end_sec": 335
```
**Stop entering trades if fewer than this many seconds remain.** At 335, the bot stops entering after ~9 min 25 sec (with 5 min 35 sec left). This protects against entering too late when there is not enough time for the position to be meaningful. Range: 60 - 500.

> **Entry window example:** With min_elapsed_sec=530 and no_entry_before_end_sec=335, the bot can only enter between 530s and 565s elapsed -- a 35-second window each market.

```
"momentum_window_sec": 60
```
**Lookback window for momentum calculation (seconds).** Momentum compares current price to the price N seconds ago. At 60, it asks: "Is the price higher than 1 minute ago?" Shorter windows = more reactive, noisier. Longer = smoother, slower to react. Range: 15 - 300.

```
"vwap_window_sec": 30
```
**Lookback window for VWAP calculation (seconds).** Only trades from the last N seconds are used to compute VWAP. Shorter = more responsive to recent trades. Longer = smoother average. Range: 10 - 120.

```
"win_rate_csv": "data/win_rate.csv"
```
**Path to the historical win rate table.** A CSV file containing win probabilities by price range and time bin. The bot uses this to display win rate on the dashboard. Generally no need to change this unless you build your own win rate data.

---

## entry -- How to execute orders

These parameters control **order mechanics**: how much to bet, how aggressively to fill, and what to do on failure.

```
"bet_amount_usd": 5
```
**How much USD to risk per trade.** The bot divides this by the entry price to get the number of contracts. Example: $5 at price $0.80 = 6 contracts. Start small ($1-5) while learning. Scale up only after consistent results. Range: 1 - any amount you are comfortable losing.

```
"price_offset": 0.02
```
**Price offset added to best bid for FAK orders.** FAK (Fill-And-Kill) orders must cross the spread to fill immediately. An offset of 0.02 means: if best bid is $0.80, the order is placed at $0.82. Higher offset = more aggressive fill but worse entry price. Range: 0.01 - 0.05.

```
"order_type": "FAK"
```
**Order type for entry.** FAK = Fill-And-Kill. The order fills immediately at the specified price or gets cancelled. This is the recommended type for fast-moving 15-minute markets. Alternative: "GTC" (Good-Till-Cancel) which stays on the book.

```
"max_retries": 3
```
**How many times to retry a failed order.** If the first attempt fails (rejected, no fill), the bot retries up to this many times. Range: 1 - 10. More retries = better chance of filling but uses more time.

```
"retry_delay_ms": 300
```
**Milliseconds to wait between retry attempts.** Range: 100 - 2000. Shorter = faster retries. Don't set too low or you may hit rate limits.

```
"fill_timeout_ms": 1000
```
**How long to wait for fill confirmation (ms).** After placing a FAK order, the bot waits this long for a WebSocket fill message. If no fill arrives in time, it enters recovery mode. Range: 500 - 5000.

```
"min_contracts": 5
```
**Minimum number of contracts per order.** Polymarket requires at least 5 contracts. If bet_amount_usd / price results in fewer than 5 contracts, the order is skipped. Generally no need to change.

```
"min_order_usd": 1
```
**Minimum order value in USD.** Orders below this value are skipped. Generally no need to change.

```
"max_entry_price": 0.88
```
**Hard price ceiling for entry.** Even if the signal says BUY, the order is rejected if the execution price exceeds this. Acts as a safety net. Should be equal to or less than strategy.max_price.

```
"ws_recovery_timeout_sec": 10
```
**Timeout for WebSocket fill recovery (seconds).** When an order times out, the bot checks the User WebSocket for fills. This is how long it waits during recovery. Range: 5 - 30.

---

## hedge -- Automatic hedging (advanced)

Hedging places a cheap order on the **opposite** token after entry. If your main trade loses, the hedge may fill and partially offset the loss.

```
"enabled": false
```
**Enable or disable automatic hedging.** Set to true to activate. When enabled, after each entry the bot places a GTD order on the opposite token. Recommended to leave false until you understand the mechanics.

```
"hedge_price": 0.02
```
**Price to place the hedge order at.** The hedge buys the opposite token at this price. At $0.02, you pay $0.02 per contract. If your main trade loses, the opposite token resolves to $1.00, netting $0.98 per contract. The hedge only fills if the market strongly moves in your favor (opposite token drops to $0.02). Range: 0.01 - 0.10.

```
"order_type": "GTD"
```
**Hedge order type.** GTD = Good-Till-Date. A limit order that sits on the book until it fills or expires. Expires in 1 hour (market resolves in 15 minutes). No need to change.

```
"max_retries": 3
```
**Retry count for hedge order placement.** If hedge placement fails, retry up to this many times.

```
"retry_delay_ms": 1000
```
**Delay between hedge retry attempts (ms).** Hedge placement is less time-critical than entry, so a longer delay is fine.

---

## redeem -- Automatic on-chain redemption

After a market resolves, winning positions must be redeemed on the blockchain to collect your payout.

```
"enabled": true
```
**Enable automatic redemption.** When true, the bot periodically checks for resolved positions and redeems them. If false, you must redeem manually on polymarket.com. Recommended: true.

```
"interval_seconds": 180
```
**How often to check for redeemable positions (seconds).** Every 180 seconds (3 minutes), the bot scans for positions that can be redeemed. Range: 60 - 600. Lower = more frequent checks but more API calls.

```
"auto_confirm": true
```
**Automatically confirm redemptions.** When true, redemptions happen without manual approval. When false, redemptions are logged but not executed.

---

## telegram -- Notifications

Optional Telegram integration for trade alerts and equity charts. Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env.

```
"enabled": false
```
**Enable Telegram notifications.** Set to true after configuring bot token and chat ID in .env. You will receive messages on trade entry, market end results, and periodic equity charts.

```
"chart_every_n_trades": 5
```
**Send an equity chart every N trades.** After every 5 trades (by default), the bot generates a P&L chart and sends it to your Telegram. Range: 1 - 50.

### telegram.streak_alert -- Consecutive-direction alert

Notifies when N completed BTC windows in a row close the same way (UP, UP, UP... or DOWN, DOWN, DOWN...).

```
"streak_alert": {
  "enabled": true,
  "min_streak": 3,
  "notify_every_extension": true,
  "notify_on_startup": false,
  "use_timer_bot": false
}
```

**Direction** comes from `signed_usd` of each completed window in the rolling buffer -- the same close-vs-open move the up/down market resolves on. Positive is UP, negative is DOWN. A window with a move of exactly `0.0` counts as **no direction and breaks the streak**; bootstrap rows recovered from `bot.log` carry `0.0` because the real move is unknown, so they can shorten a streak that spans a restart.

- **`min_streak`** -- how many same-direction windows trigger the first alert. Must be `>= 2`.
- **`notify_every_extension`** -- `true` sends a fresh alert on each window that extends the run (3x, then 4x, then 5x). `false` sends one alert when the run first reaches `min_streak`, then stays quiet until the direction flips.
- **`notify_on_startup`** -- `false` (default) baselines the existing history at boot so a restart does not re-announce a streak you were already told about. `true` alerts once at startup if the bootstrapped history already qualifies.
- **`use_timer_bot`** -- send via the dedicated timer bot (`TELEGRAM_TIMER_BOT_TOKEN` / `TELEGRAM_TIMER_CHAT_ID`) instead of the main bot.

This alert is **informational only** -- it does not gate entries or change sizing.

### Streak-based auto-buy strategies -- reversal and continuation

The dashboard's **Streak-Based Buy Bot** supports direction rules for streak lengths 1–10. Each length can use the global setting, reverse, same direction, both, or off. The global reverse/continue switches are master enables; a per-length rule only places orders for a strategy whose global switch is enabled. Modes share their buy amount, trigger limit, and time-left price thresholds with that streak length.

**Pre-buy Next Session** can queue a buy for the upcoming market near the current market's close. When time left is at or below its configured limit and the current UP or DOWN token reaches the confirmation price, the bot projects the close: if that direction extends the tracked streak, projected length is current length + 1; if direction differs, projected length is 1. It applies the projected length's direction rule and buy amount, then queues the order for next market startup. Defaults are disabled, 20 seconds, and 0.99 confirmation price. When both same and reverse qualify, select one direction on that mode; the next-window queue only supports one position.

```
"streak_reversal_bot": {
  "enabled": false,
  "same_direction_enabled": false,
  "min_streak_length": 3,
  "time_left_price_pairs": [
    {"time_left_sec": 300, "buy_price": 0.45},
    {"time_left_sec": 150, "buy_price": 0.50},
    {"time_left_sec": 60, "buy_price": 0.55}
  ]
}
```

- **`enabled`** -- global master enable for reverse buys. This is controlled by **Global Reverse Enabled** in the dashboard; continuation can be enabled independently.
- **`same_direction_enabled`** -- global master enable for buys in the active streak direction. This is controlled by the dashboard's **Global Continue Enabled** checkbox and defaults to `false`.
- **Per-mode `direction`** -- `global` (default), `reverse`, `same`, `both`, or `off`. Set individually for streak lengths 1–10 in the dashboard. `global` follows the two global master switches.
- **`min_streak_length`** -- legacy setting for the original single-mode configuration. The dashboard's multi-mode settings use exact streak lengths.
- **`time_left_price_pairs`** -- list of time-based price thresholds. The bot matches the **current time left** against these pairs and buys at the configured price if:
  - The favorite price is **below** the matched threshold, AND
  - The current streak length matches a configured mode (or a dynamically selected streak length).
  - **Reverse** buys the opposite direction; **Same direction** buys the current streak direction.
  - Each enabled strategy tracks its own per-mode trigger limit. If both direction strategies are enabled and qualify, they may each submit a separate order; orders are queued one at a time.
  
  Example: If `{"time_left_sec": 300, "buy_price": 0.45}` and time left is under 300 seconds, the bot can buy UP (if the active streak is DOWN) when favorite price <= 0.45.

**How it works:**
1. The bot checks the current active streak and selects its fixed or dynamic mode.
2. It finds the matching `time_left_price_pairs` entry and checks the favorite price against that `buy_price`.
3. If conditions match, each enabled direction strategy queues its corresponding buy, up to that mode's `max_triggers` limit.

---

## logging -- Log settings

```
"level": "INFO"
```
**Logging verbosity.** Options: "DEBUG" (very verbose), "INFO" (normal), "WARNING" (errors only). Use DEBUG for troubleshooting, INFO for normal operation.

```
"file_rotation_hours": 3
```
**Rotate log files every N hours.** Prevents log files from growing indefinitely. Range: 1 - 24.

---

## buffer -- BTC move filter threshold

```
"buffer": 25.0
```

**Minimum BTC absolute move threshold (USD).**
The entry gate compares current absolute BTC move from anchor vs an adaptive threshold. This value is the hard floor so the threshold never drops too low in quiet windows.

Effective threshold:

- Without decay: `max(buffer, avg_abs_usd_last_N_windows)`
- With decay enabled: `max(buffer, decayed_abs_usd_last_N_windows)`

To reduce this threshold inside the active market window (for example, lower it during the last minute), use:

```
"in_window_buffer_decay": {
  "enabled": true,
  "start_before_end_sec": 60,
  "min_multiplier": 0.6
}
```

- `enabled`: turn on in-window threshold reduction.
- `start_before_end_sec`: start decaying when remaining time is at or below this value.
- `min_multiplier`: multiplier applied at market end (0 sec left).

Example with `buffer=100`, `start_before_end_sec=60`, `min_multiplier=0.6`:

- at 60s left: threshold is 100
- at 30s left: threshold is 80
- at 0s left: threshold is 60

```
"buffer_decay": {
  "enabled": true,
  "half_life_windows": 2.5,
  "min_periods": 2
}
```

**Optional exponential decay on recent BTC windows.**

- `enabled`: if true, newer windows get higher weight.
- `half_life_windows`: how quickly older windows lose influence. Lower = faster adaptation.
- `min_periods`: decay only applies after at least this many completed windows; before that the bot uses simple average.

Practical starting points:

- 5m markets: `half_life_windows` 2.0 - 3.0
- 15m markets: `half_life_windows` 1.5 - 2.5

---

## Quick Presets

### Conservative (low risk, fewer trades)
```json
{
  "strategy": {
    "min_price": 0.80,
    "max_price": 0.85,
    "min_elapsed_sec": 600,
    "min_deviation_pct": 5,
    "no_entry_before_end_sec": 300
  },
  "entry": { "bet_amount_usd": 2 },
  "hedge": { "enabled": true }
}
```

### Moderate (balanced)
```json
{
  "strategy": {
    "min_price": 0.75,
    "max_price": 0.88,
    "min_elapsed_sec": 530,
    "min_deviation_pct": 3,
    "no_entry_before_end_sec": 335
  },
  "entry": { "bet_amount_usd": 10 },
  "hedge": { "enabled": true }
}
```

### Aggressive (more trades, higher risk)
```json
{
  "strategy": {
    "min_price": 0.70,
    "max_price": 0.90,
    "min_elapsed_sec": 480,
    "min_deviation_pct": 0,
    "no_entry_before_end_sec": 120
  },
  "entry": { "bet_amount_usd": 50 },
  "hedge": { "enabled": false }
}
```
