"""Independent portfolio copy-trading mode; never starts the BTC strategy."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Annotated, Callable, Literal

from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.models.data.activity import TradeActivity
from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger("portfolio")
D = Decimal
ZERO = D("0")
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")
Positive = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class FollowSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = False
    environment: Literal["simulation", "live"] = "simulation"
    wallets: list[str] = Field(default_factory=list, max_length=20)
    sizing: Literal["fixed", "percentage"] = "fixed"
    fixed_usd: Positive = 5.0
    copy_percentage: Annotated[float, Field(gt=0, le=100, allow_inf_nan=False)] = 10.0
    max_trade_usd: Positive = 10.0
    max_exposure_usd: Positive = 50.0
    slippage_pct: Annotated[float, Field(ge=0, le=20, allow_inf_nan=False)] = 2.0
    max_age_sec: Annotated[int, Field(ge=10, le=600)] = 120
    poll_seconds: Annotated[int, Field(ge=3, le=60)] = 5

    @field_validator("wallets")
    @classmethod
    def valid_wallets(cls, values: list[str]) -> list[str]:
        normalized = [value.strip().lower() for value in values]
        if any(not ADDRESS.fullmatch(value) for value in normalized):
            raise ValueError("Use public Polymarket wallet/proxy addresses: 0x + 40 hex digits")
        if len(set(normalized)) != len(normalized):
            raise ValueError("Duplicate wallets are not allowed")
        return normalized


@dataclass(frozen=True)
class SourceTrade:
    wallet: str
    transaction: str
    asset: str
    side: Literal["BUY", "SELL"]
    shares: Decimal
    cash: Decimal
    timestamp: int
    title: str
    outcome: str

    @property
    def key(self) -> str:
        # A transaction may contain multiple fills of the same outcome.
        return f"{self.wallet}:{self.transaction}:{self.asset}:{self.side}"

    @property
    def price(self) -> Decimal:
        return self.cash / self.shares


class OrderUncertain(RuntimeError):
    """The exchange may have accepted an order; automatic retry is unsafe."""


class OrderRejected(RuntimeError):
    """The exchange explicitly rejected the order."""


class PortfolioStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_file = path.with_suffix(path.suffix + ".lock").open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                self.lock_file.seek(0)
                self.lock_file.write(b"0")
                self.lock_file.flush()
                self.lock_file.seek(0)
                msvcrt.locking(self.lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock_file.close()
            raise RuntimeError("Another portfolio process is using this state database") from None
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY, wallet TEXT NOT NULL, asset TEXT NOT NULL,
                side TEXT NOT NULL, environment TEXT NOT NULL, timestamp INTEGER NOT NULL,
                title TEXT NOT NULL, outcome TEXT NOT NULL, status TEXT NOT NULL,
                message TEXT NOT NULL DEFAULT '', order_id TEXT NOT NULL DEFAULT '',
                shares TEXT NOT NULL DEFAULT '0', cash TEXT NOT NULL DEFAULT '0'
            );
            CREATE TABLE IF NOT EXISTS holdings (
                wallet TEXT NOT NULL, asset TEXT NOT NULL, environment TEXT NOT NULL,
                title TEXT NOT NULL, outcome TEXT NOT NULL,
                shares TEXT NOT NULL, cost TEXT NOT NULL, realized TEXT NOT NULL,
                PRIMARY KEY (wallet, asset, environment)
            );
        """)

    def close(self) -> None:
        self.db.close()
        self.lock_file.close()

    def get(self, key: str, default: str) -> str:
        row = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def put(self, key: str, value: str) -> None:
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES (?,?)", (key, value))

    def holding(self, wallet: str, asset: str, environment: str) -> tuple[Decimal, Decimal, Decimal]:
        row = self.db.execute(
            "SELECT shares,cost,realized FROM holdings WHERE wallet=? AND asset=? AND environment=?",
            (wallet, asset, environment),
        ).fetchone()
        return (D(row["shares"]), D(row["cost"]), D(row["realized"])) if row else (ZERO, ZERO, ZERO)

    def exposure(self, environment: str) -> Decimal:
        return sum(
            (D(row["cost"]) for row in self.db.execute(
                "SELECT cost FROM holdings WHERE environment=?", (environment,)
            )), ZERO
        )

    def unresolved(self) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM events WHERE status IN ('pending','uncertain') ORDER BY timestamp"
        )]

    def seen(self, event_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM events WHERE id=?", (event_id,)).fetchone() is not None

    def record(self, trade: SourceTrade, environment: str, status: str, message: str = "") -> bool:
        with self.db:
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO events
                   (id,wallet,asset,side,environment,timestamp,title,outcome,status,message)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (trade.key, trade.wallet, trade.asset, trade.side, environment,
                 trade.timestamp, trade.title, trade.outcome, status, message),
            )
        return cursor.rowcount == 1

    def status(self, event_id: str, status: str, message: str, order_id: str = "") -> None:
        with self.db:
            self.db.execute(
                "UPDATE events SET status=?,message=?,order_id=? WHERE id=?",
                (status, message, order_id, event_id),
            )

    def fill(
        self, event_id: str, shares: Decimal, cash: Decimal, message: str = "",
        *, allow_zero_cash: bool = False,
    ) -> None:
        if not shares.is_finite() or not cash.is_finite() or shares < 0 or cash < 0:
            raise ValueError("Fill amounts must be finite and nonnegative")
        if (shares == 0 and cash != 0) or (shares != 0 and cash == 0 and not allow_zero_cash):
            raise ValueError("Shares and cash must both be zero or both be positive")
        with self.db:
            row = self.db.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            if row is None or row["status"] not in ("pending", "uncertain"):
                raise ValueError("Only a pending/uncertain order can be reconciled")
            held, cost, realized = self.holding(row["wallet"], row["asset"], row["environment"])
            if row["side"] == "BUY":
                held, cost = held + shares, cost + cash
            else:
                if shares > held:
                    raise ValueError("Sell fill exceeds this wallet's copied position")
                released = cost * shares / held if held else ZERO
                held, cost, realized = held - shares, cost - released, realized + cash - released
            self.db.execute(
                "INSERT OR REPLACE INTO holdings VALUES (?,?,?,?,?,?,?,?)",
                (row["wallet"], row["asset"], row["environment"], row["title"], row["outcome"],
                 str(held), str(cost), str(realized)),
            )
            self.db.execute(
                "UPDATE events SET status=?,shares=?,cash=?,message=? WHERE id=?",
                ("filled" if shares else "unfilled", str(shares), str(cash), message, event_id),
            )

    def snapshot(self) -> dict:
        return {
            "holdings": [dict(row) for row in self.db.execute("SELECT * FROM holdings")],
            "events": [dict(row) for row in self.db.execute(
                "SELECT * FROM events ORDER BY rowid DESC LIMIT 100"
            )],
            "unresolved": self.unresolved(),
        }


class PortfolioGateway:
    def __init__(self) -> None:
        self.public = AsyncPublicClient()
        self.secure: AsyncSecureClient | None = None

    async def close(self) -> None:
        if self.secure is not None:
            await self.secure.close()
        await self.public.close()

    async def prepare_live(self) -> str:
        key = os.getenv("PRIVATE_KEY", "").strip().strip('"').strip("'")
        if not key:
            raise ValueError("Live mode requires PRIVATE_KEY in .env")
        from eth_account import Account

        wallet = (os.getenv("FUNDER_ADDRESS") or Account.from_key(key).address).strip().lower()
        if not ADDRESS.fullmatch(wallet):
            raise ValueError("FUNDER_ADDRESS is not a valid wallet address")
        if self.secure is None:
            self.secure = await AsyncSecureClient.create(private_key=key, wallet=wallet)
        return wallet

    async def source(self, wallet: str, start: int) -> tuple[list[SourceTrade], list[dict]]:
        trades: dict[str, SourceTrade] = {}
        pages = self.public.list_activity(
            user=wallet, activity_types=["TRADE"], start=start, sort_direction="ASC", page_size=500
        )
        count = 0
        async for page in pages:
            count += 1
            if count > 50:
                raise RuntimeError("Activity scan exceeded 25,000 rows; no partial batch was copied")
            for item in page.items:
                if not isinstance(item, TradeActivity):
                    raise ValueError("Unsupported combo/unknown activity; wallet batch was not copied")
                trade = SourceTrade(
                    wallet, str(item.transaction_hash), str(item.asset_id), item.side,
                    item.shares, item.amount, int(item.timestamp.timestamp()),
                    item.title or str(item.asset_id), item.outcome or "",
                )
                if trade.shares <= 0 or not ZERO < trade.price < 1:
                    raise ValueError("Source trade has an invalid size or price")
                previous = trades.get(trade.key)
                if previous:
                    trade = SourceTrade(
                        wallet, trade.transaction, trade.asset, trade.side,
                        previous.shares + trade.shares, previous.cash + trade.cash,
                        max(previous.timestamp, trade.timestamp), trade.title, trade.outcome,
                    )
                trades[trade.key] = trade

        positions = []
        count = 0
        async for page in self.public.list_positions(user=wallet, page_size=500):
            count += 1
            if count > 50:
                raise RuntimeError("Portfolio scan exceeded 25,000 rows; no partial batch was copied")
            for item in page.items:
                positions.append({
                    "asset": str(item.token_id), "title": item.title or str(item.token_id),
                    "outcome": item.outcome or "", "shares": str(item.current_size),
                    "value": str(item.current_value), "pnl": str(item.total_pnl),
                    "redeemable": item.redeemable,
                })
        return sorted(trades.values(), key=lambda t: (t.timestamp, t.transaction, t.side)), positions

    async def quote(
        self, trade: SourceTrade, settings: FollowSettings, amount: Decimal,
    ) -> tuple[Decimal, Decimal]:
        book = await self.public.get_order_book(asset_id=trade.asset)
        tick = book.tick_size
        slip = D(str(settings.slippage_pct)) / 100
        bound = trade.price * (1 + slip if trade.side == "BUY" else 1 - slip)
        rounding = ROUND_DOWN if trade.side == "BUY" else "ROUND_UP"
        price = (bound / tick).to_integral_value(rounding=rounding) * tick
        if not tick <= price <= 1 - tick:
            raise ValueError("Slippage bound is outside the market's tradable prices")
        levels = sorted(
            book.asks if trade.side == "BUY" else book.bids,
            key=lambda level: level.price, reverse=trade.side == "SELL",
        )
        remaining, shares, cash = amount, ZERO, ZERO
        for level in levels:
            if (trade.side == "BUY" and level.price > price) or (
                trade.side == "SELL" and level.price < price
            ):
                break
            take = min(level.size, remaining / level.price if trade.side == "BUY" else remaining)
            shares += take
            cash += take * level.price
            remaining -= take * level.price if trade.side == "BUY" else take
            if remaining <= D("0.000001"):
                break
        shares = shares.quantize(D("0.01"), rounding=ROUND_DOWN)
        if trade.side == "BUY":
            shares = min(shares, (amount / price).quantize(D("0.01"), rounding=ROUND_DOWN))
        if shares < book.min_order_size or cash <= 0:
            raise ValueError("Insufficient liquidity/order size within the slippage limit")
        return price, shares

    async def submit(
        self, trade: SourceTrade, amount: Decimal, bound: Decimal,
        on_accepted: Callable[[str], None],
    ) -> tuple[str, Decimal, Decimal]:
        if self.secure is None:
            raise RuntimeError("Live client has not been initialized")
        if trade.side == "BUY":
            response = await self.secure.place_market_order(
                asset_id=trade.asset, side="BUY", amount=amount,
                max_spend=amount, max_price=bound, order_type="FAK",
            )
        else:
            response = await self.secure.place_market_order(
                asset_id=trade.asset, side="SELL", shares=amount, min_price=bound, order_type="FAK",
            )
        if not response.ok:
            raise OrderRejected(f"{response.code}: {response.message}")
        on_accepted(str(response.order_id))
        if response.status != "matched" or not response.trade_ids:
            raise OrderUncertain(f"Order {response.order_id}: {response.status}; inspect exchange fills")
        # Settlement failure cannot safely be treated as an unfilled order.
        await self.secure.wait_for_order_fill_settlement(response)
        shares, cash = (
            (response.taking_amount, response.making_amount) if trade.side == "BUY"
            else (response.making_amount, response.taking_amount)
        )
        if shares <= 0 or cash <= 0:
            raise OrderUncertain(f"Order {response.order_id}: matched without confirmed amounts")
        return str(response.order_id), shares, cash


class PortfolioFollower:
    def __init__(self, store: PortfolioStore, gateway: PortfolioGateway):
        self.store = store
        self.gateway = gateway
        self.settings = FollowSettings.model_validate_json(store.get("settings", "{}"))
        # Every process start requires a fresh, explicit activation.
        self.settings.enabled = False
        self.lock = asyncio.Lock()
        self.error = ""
        self.worker_failed = False
        self.pause_revision = 0
        self.last_poll: float | None = None
        self.portfolios: dict[str, list[dict]] = {}
        self.wallet_errors: dict[str, str] = {}
        self.since: dict[str, int] = json.loads(store.get("since", "{}"))

    async def configure(self, settings: FollowSettings, confirm_live: bool = False) -> None:
        async with self.lock:
            revision = self.pause_revision
            if settings.enabled and self.worker_failed:
                raise ValueError("The portfolio worker stopped; restart this process before enabling copying")
            if settings.enabled and not settings.wallets:
                raise ValueError("Add at least one wallet before enabling copying")
            if settings.enabled and settings.environment == "live":
                if not confirm_live:
                    raise ValueError("Explicit live-trading confirmation is required")
                if self.store.unresolved():
                    raise ValueError("Reconcile all uncertain orders before enabling live copying")
                wallet = await self.gateway.prepare_live()
                if wallet in settings.wallets:
                    raise ValueError("Cannot copy your own trading wallet")
                owner = self.store.get("live_wallet", wallet)
                if owner != wallet:
                    raise ValueError("Live wallet changed; use a separate state database for this account")
                self.store.put("live_wallet", wallet)
            if settings.enabled and revision != self.pause_revision:
                raise ValueError("Copying was paused while settings were being applied; enable again explicitly")
            if settings.enabled and self.store.unresolved():
                raise ValueError("Reconcile unresolved orders before enabling copying")
            now = int(time.time())
            for wallet in settings.wallets:
                if wallet not in self.settings.wallets or (
                    settings.enabled and (
                        not self.settings.enabled or settings.environment != self.settings.environment
                    )
                ):
                    self.since[wallet] = now
            removed = set(self.settings.wallets) - set(settings.wallets)
            for wallet in removed:
                self.portfolios.pop(wallet, None)
                self.wallet_errors.pop(wallet, None)
            self.store.put("since", json.dumps(self.since))
            self.store.put("settings", settings.model_dump_json())
            self.settings = settings
            self.error = ""

    def pause(self) -> None:
        self.pause_revision += 1
        self.settings.enabled = False
        self.store.put("settings", self.settings.model_dump_json())

    async def reconcile(self, event_id: str, shares: Decimal, cash: Decimal, note: str) -> None:
        async with self.lock:
            if self.settings.enabled:
                raise ValueError("Pause copying before reconciling an order")
            if not note.strip():
                raise ValueError("An exchange reconciliation note is required")
            self.store.fill(event_id, shares, cash, "Manually reconciled: " + note.strip())
            self.error = ""

    async def external_close(
        self, wallet: str, asset: str, environment: Literal["simulation", "live"],
        cash: Decimal, note: str,
    ) -> None:
        async with self.lock:
            if self.settings.enabled or self.store.unresolved():
                raise ValueError("Pause copying and reconcile pending orders before recording a close")
            if not note.strip() or not cash.is_finite() or cash < 0:
                raise ValueError("A settlement note and nonnegative cash proceeds are required")
            shares, _, _ = self.store.holding(wallet, asset, environment)
            if shares <= 0:
                raise ValueError("There is no copied position to close")
            position = self.store.db.execute(
                "SELECT title,outcome FROM holdings WHERE wallet=? AND asset=? AND environment=?",
                (wallet, asset, environment),
            ).fetchone()
            event = SourceTrade(
                wallet, f"external-{time.time_ns()}", asset, "SELL",
                shares, cash, int(time.time()), position["title"], position["outcome"],
            )
            self.store.record(event, environment, "pending")
            # A resolved losing token has zero proceeds but a positive extinguished balance.
            self.store.fill(event.key, shares, cash, "External close: " + note.strip(), allow_zero_cash=True)

    async def poll(self) -> None:
        async with self.lock:
            cfg = self.settings
            now = int(time.time())
            for wallet in cfg.wallets:
                if wallet not in self.since:
                    self.since[wallet] = now
                    self.store.put("since", json.dumps(self.since))
                try:
                    start = max(self.since[wallet], now - cfg.max_age_sec)
                    trades, positions = await self.gateway.source(wallet, start)
                    self.portfolios[wallet] = positions
                    self.wallet_errors.pop(wallet, None)
                    # Reconstruct pre-sale inventory backwards from the current source portfolio.
                    balances = {p["asset"]: D(p["shares"]) for p in positions}
                    before: dict[str, Decimal] = {}
                    for trade in reversed(trades):
                        balance = balances.get(trade.asset, ZERO)
                        balance += trade.shares if trade.side == "SELL" else -trade.shares
                        balances[trade.asset] = balance
                        before[trade.key] = balance
                    for trade in trades:
                        if self.store.seen(trade.key):
                            continue
                        if trade.timestamp <= self.since[wallet] or now - trade.timestamp > cfg.max_age_sec:
                            self.store.record(trade, cfg.environment, "skipped", "Before activation or stale")
                            continue
                        if not cfg.enabled or self.store.unresolved():
                            self.store.record(trade, cfg.environment, "skipped", "Copying paused")
                            continue
                        await self.copy(trade, before[trade.key])
                except Exception as exc:
                    # Boundary for one wallet: errors remain visible, never become empty portfolios.
                    logger.exception("Portfolio poll failed for %s", wallet)
                    self.wallet_errors[wallet] = str(exc)
            self.last_poll = time.time()

    async def copy(self, trade: SourceTrade, source_before: Decimal) -> None:
        cfg = self.settings
        held, _, _ = self.store.holding(trade.wallet, trade.asset, cfg.environment)
        if trade.side == "BUY":
            desired = D(str(cfg.fixed_usd)) if cfg.sizing == "fixed" else (
                trade.cash * D(str(cfg.copy_percentage)) / 100
            )
            amount = min(
                desired, D(str(cfg.max_trade_usd)),
                D(str(cfg.max_exposure_usd)) - self.store.exposure(cfg.environment),
            ).quantize(D("0.01"), rounding=ROUND_DOWN)
        else:
            if source_before < trade.shares:
                self.store.record(trade, cfg.environment, "skipped", "Source position snapshot inconsistent")
                return
            amount = (held * trade.shares / source_before).quantize(D("0.01"), rounding=ROUND_DOWN)
        if amount <= 0:
            self.store.record(trade, cfg.environment, "skipped", "Exposure limit or no copied shares to sell")
            return
        try:
            bound, quoted_shares = await self.gateway.quote(trade, cfg, amount)
        except Exception as exc:
            logger.warning("Copy quote skipped: %s", exc)
            self.store.record(trade, cfg.environment, "skipped", str(exc))
            return
        if not cfg.enabled or time.time() - trade.timestamp > cfg.max_age_sec:
            self.store.record(trade, cfg.environment, "skipped", "Paused or stale before submission")
            return
        # Commit before sending: crashes/timeouts must never cause a duplicate submission.
        if not self.store.record(trade, cfg.environment, "pending"):
            return
        try:
            if cfg.environment == "simulation":
                shares = min(quoted_shares, (amount / bound).quantize(
                    D("0.01"), rounding=ROUND_DOWN
                )) if trade.side == "BUY" else quoted_shares
                # Conservative paper fills use the worst permitted execution price.
                cash = shares * bound
                order_id = "simulation"
            else:
                order_id, shares, cash = await self.gateway.submit(
                    trade, amount, bound,
                    lambda order_id: self.store.status(trade.key, "pending", "", order_id),
                )
            self.store.status(trade.key, "pending", "", order_id)
            self.store.fill(trade.key, shares, cash)
        except OrderRejected as exc:
            logger.warning("Copy order rejected: %s", exc)
            self.store.status(trade.key, "rejected", str(exc))
        except Exception as exc:
            logger.exception("Copy order requires reconciliation: %s", trade.key)
            row = self.store.db.execute(
                "SELECT order_id FROM events WHERE id=?", (trade.key,)
            ).fetchone()
            self.store.status(trade.key, "uncertain", str(exc), row["order_id"])
            self.pause()
            self.error = "Copying paused: an order outcome is uncertain. Reconcile it before restarting."

    def snapshot(self) -> dict:
        result = self.store.snapshot()
        result.update(
            mode="portfolio", settings=self.settings.model_dump(),
            portfolios=self.portfolios, wallet_errors=self.wallet_errors,
            error=self.error, last_poll=self.last_poll,
            exposure={mode: str(self.store.exposure(mode)) for mode in ("simulation", "live")},
        )
        return result
