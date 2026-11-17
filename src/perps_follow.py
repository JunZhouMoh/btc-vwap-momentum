"""Polymarket Perps copying, with accounting independent of prediction tokens."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hashlib
import json
import logging
import os
import time
from typing import Annotated, Callable, Literal

import httpx
from polymarket import AsyncPublicClient, AsyncSecureClient
from polymarket.models.perps.account import PerpsPortfolio
from polymarket.models.perps.market import PerpsInstrument, PerpsTicker
from polymarket.models.perps.orders import PerpsFill
from polymarket.perps import PerpsSession
from pydantic import BaseModel, ConfigDict, Field

from src.portfolio_follow import ADDRESS, FollowSettings, PortfolioStore, Positive, OrderUncertain

D = Decimal
ZERO = D("0")
logger = logging.getLogger("portfolio.perps")
COUNTRY_CODES = frozenset((
    "AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ "
    "CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR "
    "GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP "
    "KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ "
    "NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ "
    "TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW"
).split())


class PerpsSettings(FollowSettings):
    fixed_usd: Positive = 25.0
    max_trade_usd: Positive = 25.0
    max_exposure_usd: Positive = 100.0
    max_position_usd: Positive = 50.0
    leverage: Annotated[int, Field(ge=1, le=20)] = 1
    simulation_equity_usd: Positive = 1000.0


class PublicPosition(BaseModel):
    instrument_id: int
    symbol: str
    size: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal

    model_config = ConfigDict(allow_inf_nan=False)


class PublicPortfolio(BaseModel):
    positions: list[PublicPosition]
    equity: Decimal
    timestamp: int

    model_config = ConfigDict(allow_inf_nan=False)


@dataclass(frozen=True)
class PerpsTrade:
    wallet: str
    order_id: int
    instrument: int
    timestamp: int
    previous: Decimal
    delta: Decimal
    price: Decimal
    forced: bool = False

    @property
    def key(self) -> str:
        return f"perps:{self.wallet}:{self.order_id}:{self.timestamp}:{self.instrument}:{self.previous.normalize()}:{self.delta > 0}"

    @property
    def close_quantity(self) -> Decimal:
        return min(abs(self.previous), abs(self.delta)) if self.previous * self.delta < 0 else ZERO

    @property
    def open_quantity(self) -> Decimal:
        return abs(self.delta) - self.close_quantity


def verify_operator_location() -> None:
    if os.getenv("PERPS_LOCATION_VERIFIED", "").strip().lower() != "true":
        raise ValueError("Live perps requires independently verified operator eligibility (PERPS_LOCATION_VERIFIED=true)")
    country = os.getenv("PERPS_OPERATOR_COUNTRY", "").strip().upper()
    region = os.getenv("PERPS_OPERATOR_REGION", "").strip().upper()
    if country not in COUNTRY_CODES:
        raise ValueError("Live perps requires PERPS_OPERATOR_COUNTRY after verifying the operator's location")
    if country in {"US", "CA", "CU", "IR", "KP", "SY"}:
        raise ValueError("Perps trading is not permitted in the configured operator jurisdiction")
    if country == "UA" and region not in {
        "UA-05", "UA-07", "UA-09", "UA-12", "UA-14", "UA-18", "UA-21", "UA-23", "UA-26",
        "UA-30", "UA-32", "UA-35", "UA-40", "UA-43", "UA-46", "UA-48", "UA-51", "UA-53",
        "UA-56", "UA-59", "UA-61", "UA-63", "UA-65", "UA-68", "UA-71", "UA-74", "UA-77",
    }:
        raise ValueError("A valid ISO 3166-2 PERPS_OPERATOR_REGION is required for Ukraine")
    if region in {"UA-43", "UA-40", "UA-14", "UA-09", "CRIMEA", "DONETSK", "LUHANSK"}:
        raise ValueError("Perps trading is not permitted in the configured operator region")


class PerpsStore:
    def __init__(self, store: PortfolioStore):
        self.store = store
        self.db = store.db
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS perps_positions (
                wallet TEXT NOT NULL, instrument INTEGER NOT NULL, environment TEXT NOT NULL,
                quantity TEXT NOT NULL, entry TEXT NOT NULL, realized TEXT NOT NULL, fees TEXT NOT NULL,
                PRIMARY KEY(wallet,instrument,environment)
            );
            CREATE TABLE IF NOT EXISTS perps_events (
                id TEXT PRIMARY KEY, client_id TEXT NOT NULL, wallet TEXT NOT NULL,
                instrument INTEGER NOT NULL, environment TEXT NOT NULL, timestamp INTEGER NOT NULL,
                side INTEGER NOT NULL, reduce_only INTEGER NOT NULL, requested TEXT NOT NULL,
                status TEXT NOT NULL, quantity TEXT NOT NULL DEFAULT '0', price TEXT NOT NULL DEFAULT '0',
                fee TEXT NOT NULL DEFAULT '0', order_id TEXT NOT NULL DEFAULT '', message TEXT NOT NULL DEFAULT ''
            );
        """)

    def positions(self, environment: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM perps_positions WHERE environment=?", (environment,)
        )]

    def position(self, wallet: str, instrument: int, environment: str) -> tuple[Decimal, Decimal]:
        row = self.db.execute(
            "SELECT quantity,entry FROM perps_positions WHERE wallet=? AND instrument=? AND environment=?",
            (wallet, instrument, environment),
        ).fetchone()
        return (D(row["quantity"]), D(row["entry"])) if row else (ZERO, ZERO)

    def unresolved(self) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM perps_events WHERE status IN ('pending','uncertain') ORDER BY rowid"
        )]

    def record(
        self, event_id: str, trade: PerpsTrade, cfg: PerpsSettings, reduce: bool,
        quantity: Decimal, status: str, message: str = "",
    ) -> bool:
        with self.db:
            cursor = self.db.execute(
                """INSERT OR IGNORE INTO perps_events
                   (id,client_id,wallet,instrument,environment,timestamp,side,reduce_only,requested,status,message)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (event_id, hashlib.sha256(event_id.encode()).hexdigest()[:32], trade.wallet,
                 trade.instrument, cfg.environment, trade.timestamp, 1 if trade.delta > 0 else -1,
                 int(reduce), str(quantity), status, message),
            )
        return cursor.rowcount == 1

    def event(self, event_id: str):
        return self.db.execute("SELECT * FROM perps_events WHERE id=?", (event_id,)).fetchone()

    def status(self, event_id: str, status: str, message: str, order_id: str | None = None) -> None:
        with self.db:
            self.db.execute(
                "UPDATE perps_events SET status=?,message=?,order_id=COALESCE(?,order_id) WHERE id=?",
                (status, message, order_id, event_id),
            )

    def fill(self, event_id: str, quantity: Decimal, price: Decimal, fee: Decimal, note: str = "") -> None:
        if any(not x.is_finite() for x in (quantity, price, fee)) or quantity < 0 or price < 0:
            raise ValueError("Fill amounts must be finite; quantity and price cannot be negative")
        if quantity > 0 and price <= 0 or quantity == 0 and (price != 0 or fee != 0):
            raise ValueError("An executed fill needs a price; no-fill reconciliation must use zero amounts")
        with self.db:
            event = self.event(event_id)
            if event is None or event["status"] not in ("pending", "uncertain"):
                raise ValueError("Only pending/uncertain perps orders can be reconciled")
            if quantity > D(event["requested"]):
                raise ValueError("Fill exceeds requested perps quantity")
            held, entry = self.position(event["wallet"], event["instrument"], event["environment"])
            position = self.db.execute(
                "SELECT realized,fees FROM perps_positions WHERE wallet=? AND instrument=? AND environment=?",
                (event["wallet"], event["instrument"], event["environment"]),
            ).fetchone()
            realized, fees = (D(position["realized"]), D(position["fees"])) if position else (ZERO, ZERO)
            delta = quantity * event["side"]
            if quantity:
                if event["reduce_only"]:
                    if held * delta >= 0 or quantity > abs(held):
                        raise ValueError("Reduce-only fill would increase or reverse the copied position")
                    realized += (price - entry) * quantity * (1 if held > 0 else -1)
                else:
                    if held * delta < 0:
                        raise ValueError("Opening fill conflicts with an existing copied position")
                    entry = (abs(held) * entry + quantity * price) / (abs(held) + quantity)
            held += delta
            self.db.execute(
                "INSERT OR REPLACE INTO perps_positions VALUES (?,?,?,?,?,?,?)",
                (event["wallet"], event["instrument"], event["environment"], str(held),
                 str(entry if held else ZERO), str(realized), str(fees + fee)),
            )
            self.db.execute(
                "UPDATE perps_events SET status=?,quantity=?,price=?,fee=?,message=? WHERE id=?",
                ("filled" if quantity else "unfilled", str(quantity), str(price), str(fee), note, event_id),
            )

    def snapshot(self) -> dict:
        return {
            "positions": self.positions("simulation") + self.positions("live"),
            "events": [dict(row) for row in self.db.execute(
                "SELECT * FROM perps_events ORDER BY rowid DESC LIMIT 100"
            )],
            "unresolved": self.unresolved(),
        }


class PerpsGateway:
    def __init__(self):
        self.public = AsyncPublicClient()
        self.http = httpx.AsyncClient(base_url="https://api.perpetuals.polymarket.com", timeout=20)
        self.secure: AsyncSecureClient | None = None
        self.session: PerpsSession | None = None
        self.wallet: str | None = None

    async def close(self) -> None:
        if self.session is not None:
            await self.session.close()
        if self.secure is not None:
            await self.secure.close()
        await self.public.close()
        await self.http.aclose()

    async def prepare_live(self) -> str:
        verify_operator_location()
        key = os.getenv("PERPS_PRIVATE_KEY", "").strip()
        wallet = os.getenv("PERPS_WALLET_ADDRESS", "").strip().lower()
        if not key or not ADDRESS.fullmatch(wallet):
            raise ValueError("Live perps requires PERPS_PRIVATE_KEY and PERPS_WALLET_ADDRESS")
        if not await self.public.fetch_perps_registration(address=wallet):
            raise ValueError("The configured wallet has no registered Perps account; fund/register it on Polymarket")
        if self.session is not None:
            await self.session.close()
            self.session = None
            self.wallet = None
        if self.secure is not None:
            await self.secure.close()
        self.secure = await AsyncSecureClient.create(private_key=key, wallet=wallet)
        self.session = await self.secure.open_perps_session(label="portfolio-following")
        self.wallet = wallet
        return wallet

    async def market_data(self) -> tuple[dict[int, PerpsInstrument], dict[int, PerpsTicker]]:
        instruments = await self.public.fetch_perps_instruments()
        tickers = await self.public.fetch_perps_tickers()
        return {int(i.id): i for i in instruments}, {int(t.instrument_id): t for t in tickers}

    async def source(self, wallet: str, start: int, end: int) -> tuple[list[PerpsTrade], dict]:
        if not await self.public.fetch_perps_registration(address=wallet):
            raise ValueError("Follow the registered Perps account owner address, not a prediction proxy without Perps")
        params: dict[str, str | int] = {
            "address": wallet, "start_timestamp": min(start, end), "end_timestamp": end, "sort": "asc",
        }
        groups: dict[tuple, list[PerpsFill]] = {}
        cursors: set[str] = set()
        seen: set[int] = set()
        for _ in range(50):
            response = await self.http.get("/v1/info/fills", params=params)
            response.raise_for_status()
            page = response.json()
            for raw in page["data"]:
                fill = PerpsFill.model_validate(raw)
                if fill.trade_id in seen:
                    continue
                seen.add(fill.trade_id)
                if fill.quantity <= 0 or fill.price <= 0:
                    raise ValueError("Public Perps fill has an invalid quantity/price")
                key = (int(fill.timestamp.timestamp() * 1000), int(fill.order_id),
                       int(fill.instrument_id), fill.side, fill.previous_size)
                groups.setdefault(key, []).append(fill)
            if not page["more"]:
                break
            cursor = page["cursor"]
            if not cursor or cursor in cursors:
                raise ValueError("Perps fills pagination did not advance")
            cursors.add(cursor)
            params["cursor"] = cursor
        else:
            raise ValueError("Perps fill scan exceeded 5,000 rows; no partial batch was copied")
        trades = []
        chronological = sorted(groups.items(), key=lambda item: (
            item[0][0], min(f.trade_id for f in item[1]),
        ))
        for (timestamp, order, instrument, side, previous), fills in chronological:
            quantity = sum((f.quantity for f in fills), ZERO)
            price = sum((f.quantity * f.price for f in fills), ZERO) / quantity
            trades.append(PerpsTrade(
                wallet, order, instrument, timestamp, previous,
                quantity if side == "long" else -quantity, price,
                any(f.liquidation or f.adl or f.settlement for f in fills),
            ))
        response = await self.http.get("/v1/info/portfolio", params={"address": wallet})
        response.raise_for_status()
        portfolio = PublicPortfolio.model_validate(response.json())
        return trades, portfolio.model_dump(mode="json")

    async def account(self) -> PerpsPortfolio:
        if self.session is None:
            raise ValueError("Open a live Perps session first")
        if await self.session.fetch_open_orders():
            raise ValueError("Unmanaged/open Perps orders exist; cancel them before copying")
        return await self.session.fetch_portfolio()

    async def set_leverage(self, instrument: int, leverage: int) -> None:
        if self.session is None:
            raise ValueError("No live Perps session")
        result = await self.session.update_leverage(
            instrument_id=instrument, leverage=leverage, cross_margin=False,
        )
        if result.leverage != leverage or result.cross_margin:
            raise ValueError("Exchange did not confirm the requested isolated leverage")

    async def quote(
        self, trade: PerpsTrade, instrument: PerpsInstrument, cfg: PerpsSettings,
        quantity: Decimal, reduce: bool, *, max_notional: Decimal | None = None,
    ) -> tuple[Decimal, Decimal, Decimal]:
        if instrument.settlement is not None or instrument.close_only and not reduce:
            raise ValueError("Instrument is settled or close-only")
        book = await self.public.fetch_perps_book(instrument_id=trade.instrument)
        if abs(time.time() - book.timestamp.timestamp()) > 30:
            raise ValueError("Perps order book is stale")
        buy = trade.delta > 0
        bound = trade.price * (1 + D(str(cfg.slippage_pct)) / 100 * (1 if buy else -1))
        bound = bound.quantize(
            D(1).scaleb(-instrument.price_decimals), rounding=ROUND_DOWN if buy else ROUND_UP,
        )
        if bound <= 0:
            raise ValueError("Invalid Perps slippage price")
        levels = sorted(book.asks if buy else book.bids, key=lambda x: x.price, reverse=not buy)
        if any(not v.is_finite() or v <= 0 for level in levels for v in (level.price, level.quantity)):
            raise ValueError("Invalid Perps book price or quantity")
        if not reduce and max_notional is not None:
            largest_price = max(
                (level.price for level in levels if (level.price <= bound if buy else level.price >= bound)),
                default=ZERO,
            )
            if largest_price:
                quantity = min(quantity, max_notional / largest_price)
        available = ZERO
        for level in levels:
            if buy and level.price > bound or not buy and level.price < bound:
                break
            take = min(level.quantity, quantity - available)
            available += take
            if available >= quantity:
                break
        available = available.quantize(D(1).scaleb(-instrument.quantity_decimals), rounding=ROUND_DOWN)
        if available <= 0 or not reduce and available * bound < instrument.min_notional:
            raise ValueError("Perps order below minimum or no liquidity inside slippage limit")
        remaining, cash = available, ZERO
        for level in levels:
            take = min(level.quantity, remaining)
            cash += take * level.price
            remaining -= take
            if not remaining:
                break
        return bound, available, cash / available

    async def resolve(self, client_id: str) -> tuple[str, Decimal, Decimal, Decimal]:
        if self.session is None:
            raise ValueError("No live Perps session")
        for _ in range(25):
            orders = await self.session.fetch_orders(client_order_id=client_id)
            if len(orders) == 1:
                order = orders[0]
                if order.status in {"duplicate_order", "order_not_found", "order_already_terminal"}:
                    raise OrderUncertain("Perps response cannot establish the original order outcome")
                if order.status not in {"accepted", "open", "partial", "untriggered", "armed", "triggered"}:
                    if order.resting_quantity != 0:
                        raise OrderUncertain("Perps order still has resting quantity")
                    if order.filled_quantity == 0:
                        return str(order.id), ZERO, ZERO, ZERO
                    fills: dict[int, PerpsFill] = {}
                    count = 0
                    async for page in self.session.list_fills(
                        instrument_id=int(order.instrument_id), start=order.created_at, sort="asc",
                    ):
                        count += 1
                        if count > 50:
                            raise OrderUncertain("Perps fill reconciliation exceeded its page limit")
                        fills.update((int(f.trade_id), f) for f in page.items if f.order_id == order.id)
                    if any(f.fee_asset != "pUSD" for f in fills.values()):
                        raise OrderUncertain("Unsupported Perps fee currency; reconcile actual USD-equivalent fees")
                    quantity = sum((f.quantity for f in fills.values()), ZERO)
                    if quantity == order.filled_quantity:
                        price = sum((f.quantity * f.price for f in fills.values()), ZERO) / quantity
                        return str(order.id), quantity, price, sum((f.total_fee for f in fills.values()), ZERO)
            await asyncio.sleep(.2)
        raise OrderUncertain("Perps IOC outcome/fills are not confirmed; reconcile by client order ID")

    async def submit(
        self, trade: PerpsTrade, quantity: Decimal, price: Decimal, reduce: bool,
        client_id: str, accepted: Callable[[str], None],
    ) -> tuple[str, Decimal, Decimal, Decimal]:
        verify_operator_location()
        if self.session is None:
            raise ValueError("No live Perps session")
        result = await self.session.place_order(
            instrument_id=trade.instrument, side="BUY" if trade.delta > 0 else "SELL",
            quantity=quantity, price=price, time_in_force="ioc", reduce_only=reduce,
            client_order_id=client_id,
        )
        accepted(str(result.order.id))
        return await self.resolve(client_id)


class PerpsFollower:
    def __init__(self, store: PerpsStore, gateway: PerpsGateway):
        self.store, self.gateway = store, gateway
        self.settings = PerpsSettings.model_validate_json(store.store.get("perps_settings", "{}"))
        self.settings.enabled = False
        self.since: dict[str, int] = json.loads(store.store.get("perps_since", "{}"))
        self.lock = asyncio.Lock()
        self.pause_revision = 0
        self.worker_failed = False
        self.error = ""
        self.last_poll: float | None = None
        self.portfolios: dict[str, dict] = {}
        self.wallet_errors: dict[str, str] = {}
        self.instruments: dict[int, PerpsInstrument] = {}
        self.tickers: dict[int, PerpsTicker] = {}
        self.live_account: PerpsPortfolio | None = None

    def pause(self) -> None:
        self.pause_revision += 1
        self.settings.enabled = False
        self.store.store.put("perps_settings", self.settings.model_dump_json())

    def totals(self, environment: str) -> tuple[Decimal, Decimal, dict[int, Decimal]]:
        gross, profit, by_instrument = ZERO, ZERO, {}
        for position in self.store.positions(environment):
            quantity, entry = D(position["quantity"]), D(position["entry"])
            profit += D(position["realized"]) - D(position["fees"])
            if quantity:
                mark = self.tickers[position["instrument"]].mark_price
                if not mark.is_finite() or mark <= 0:
                    raise ValueError("A held Perps instrument has no valid mark price; exposure cannot be verified")
                notional = abs(quantity) * mark
                gross += notional
                by_instrument[position["instrument"]] = by_instrument.get(position["instrument"], ZERO) + notional
                profit += (mark - entry) * quantity
        return gross, profit, by_instrument

    async def check_account(self) -> PerpsPortfolio:
        owner = self.store.store.get("perps_live_wallet", "")
        if owner and self.gateway.wallet != owner:
            raise ValueError("Live Perps session wallet differs from the persisted account")
        account = await self.gateway.account()
        if account.in_liquidation:
            raise ValueError("Perps account is in liquidation; copying is blocked")
        expected: dict[int, Decimal] = {}
        for position in self.store.positions("live"):
            key = position["instrument"]
            expected[key] = expected.get(key, ZERO) + D(position["quantity"])
        actual = {int(p.instrument_id): p.size for p in account.positions if p.size}
        if {k: v for k, v in expected.items() if v} != actual:
            raise ValueError("Live Perps inventory differs from copied positions (external trade/liquidation). Pause and reconcile.")
        self.live_account = account
        return account

    async def configure(self, cfg: PerpsSettings, confirm_live: bool = False) -> None:
        async with self.lock:
            revision = self.pause_revision
            if cfg.enabled:
                if self.worker_failed or self.store.unresolved():
                    raise ValueError("Restart failed workers and reconcile uncertain Perps orders before enabling")
                if not cfg.wallets:
                    raise ValueError("Add at least one Perps account owner address")
                if cfg.environment == "live":
                    if not confirm_live:
                        raise ValueError("Explicit live Perps confirmation is required")
                    wallet = await self.gateway.prepare_live()
                    if wallet in cfg.wallets:
                        raise ValueError("Cannot copy your own Perps account")
                    if self.store.store.get("perps_live_wallet", wallet) != wallet:
                        raise ValueError("Perps live wallet changed; use a different database")
                    await self.check_account()
                    self.store.store.put("perps_live_wallet", wallet)
            if cfg.enabled and revision != self.pause_revision:
                raise ValueError("Perps copying was paused during activation; enable explicitly again")
            now = int(time.time() * 1000)
            for wallet in cfg.wallets:
                if wallet not in self.settings.wallets or cfg.enabled and (
                    not self.settings.enabled or cfg.environment != self.settings.environment
                ):
                    self.since[wallet] = now
            self.store.store.put("perps_since", json.dumps(self.since))
            self.store.store.put("perps_settings", cfg.model_dump_json())
            self.settings = cfg
            self.error = ""

    async def poll(self) -> None:
        async with self.lock:
            cfg = self.settings
            if not cfg.wallets and not any(D(p["quantity"]) for p in self.store.snapshot()["positions"]):
                self.last_poll = time.time()
                return
            try:
                self.instruments, self.tickers = await self.gateway.market_data()
                if self.gateway.session is not None and not self.store.unresolved():
                    await self.check_account()
                now = int(time.time() * 1000)
                for wallet in cfg.wallets:
                    since = self.since.setdefault(wallet, now)
                    try:
                        trades, portfolio = await self.gateway.source(wallet, max(since, now - cfg.max_age_sec * 1000), now - 2000)
                        self.portfolios[wallet] = portfolio
                        self.wallet_errors.pop(wallet, None)
                        for trade in trades:
                            for reduce, size in ((True, trade.close_quantity), (False, trade.open_quantity)):
                                if size:
                                    await self.copy(trade, reduce, size)
                    except Exception as exc:
                        logger.exception("Perps source poll failed for %s", wallet)
                        self.wallet_errors[wallet] = str(exc)
                self.error = "" if cfg.enabled else self.error
            except Exception as exc:
                logger.exception("Perps market/account risk check failed")
                self.error = str(exc)
                if cfg.environment == "live":
                    self.pause()
            self.last_poll = time.time()

    async def copy(self, trade: PerpsTrade, reduce: bool, source_quantity: Decimal) -> None:
        cfg = self.settings
        event_id = trade.key + (":close" if reduce else ":open")
        if self.store.event(event_id):
            return
        quantity, _ = self.store.position(trade.wallet, trade.instrument, cfg.environment)
        side = 1 if trade.delta > 0 else -1

        def skip(message: str) -> None:
            self.store.record(event_id, trade, cfg, reduce, ZERO, "skipped", message)

        if not cfg.enabled or self.store.unresolved():
            skip("Perps copying paused or awaiting reconciliation")
            return
        if trade.timestamp <= self.since.get(trade.wallet, 0) or not 0 <= time.time() * 1000 - trade.timestamp <= cfg.max_age_sec * 1000:
            skip("Before activation or stale source fill")
            return
        instrument, ticker = self.instruments[trade.instrument], self.tickers[trade.instrument]
        if ticker.mark_price <= 0 or not ticker.mark_price.is_finite():
            raise ValueError("Invalid Perps mark price")
        try:
            account = await self.check_account() if cfg.environment == "live" else None
            if reduce:
                if quantity * side >= 0 or trade.previous == 0:
                    skip("No matching copied position to reduce")
                    return
                desired = abs(quantity) * source_quantity / abs(trade.previous)
            else:
                if trade.forced:
                    skip("Forced/liquidation fills cannot open copied exposure")
                    return
                peers = self.store.positions(cfg.environment)
                if any(p["instrument"] == trade.instrument and D(p["quantity"]) * side < 0 for p in peers):
                    skip("Opposing copied position remains; no hedging/netting across followed accounts")
                    return
                if cfg.leverage > instrument.max_leverage:
                    skip("Configured leverage exceeds this instrument's limit")
                    return
                gross, pnl, by_instrument = self.totals(cfg.environment)
                if account:
                    by_instrument = {int(p.instrument_id): abs(p.position_value) for p in account.positions}
                    gross = sum(by_instrument.values(), ZERO)
                budget = D(str(cfg.fixed_usd)) if cfg.sizing == "fixed" else (
                    source_quantity * trade.price * D(str(cfg.copy_percentage)) / 100
                )
                available = (
                    account.margin.available_order_margin if account else
                    D(str(cfg.simulation_equity_usd)) + pnl - gross / cfg.leverage
                )
                budget = min(
                    budget, D(str(cfg.max_trade_usd)), instrument.max_limit_notional, instrument.max_market_notional,
                    D(str(cfg.max_position_usd)) - by_instrument.get(trade.instrument, ZERO),
                    D(str(cfg.max_exposure_usd)) - gross, max(ZERO, available) * cfg.leverage * D(".98"),
                )
                worst = max(ticker.mark_price, trade.price * (1 + D(str(cfg.slippage_pct)) / 100))
                desired = max(ZERO, budget) / worst
            desired = desired.quantize(D(1).scaleb(-instrument.quantity_decimals), rounding=ROUND_DOWN)
            if desired <= 0:
                skip("Position/exposure/margin limit or quantity below instrument precision")
                return
            bound, filled, average = await self.gateway.quote(
                trade, instrument, cfg, desired, reduce, max_notional=None if reduce else budget,
            )
            if cfg.environment == "live" and not reduce:
                await self.gateway.set_leverage(trade.instrument, cfg.leverage)
                account = await self.check_account()
                position = next((p for p in account.positions if p.instrument_id == trade.instrument), None)
                if position and (position.cross or position.leverage != cfg.leverage):
                    raise ValueError("Perps account leverage/margin mode does not match configured isolated risk")
                if any(p.cross or p.leverage > cfg.leverage for p in account.positions if p.size):
                    raise ValueError("Existing Perps positions exceed the configured isolated leverage")
                gross = sum((abs(p.position_value) for p in account.positions), ZERO)
                value = abs(position.position_value) if position else ZERO
                remaining = min(
                    D(str(cfg.max_exposure_usd)) - gross, D(str(cfg.max_position_usd)) - value,
                    account.margin.available_order_margin * cfg.leverage * D(".98"),
                )
                if filled * max(bound, ticker.mark_price) > remaining:
                    raise ValueError("Perps account margin/exposure changed during preflight")
            if not cfg.enabled or time.time() * 1000 - trade.timestamp > cfg.max_age_sec * 1000:
                skip("Paused or stale before Perps submission")
                return
        except Exception as exc:
            logger.warning("Perps trade skipped: %s", exc)
            skip(str(exc))
            return
        if not self.store.record(event_id, trade, cfg, reduce, filled, "pending"):
            return
        try:
            if cfg.environment == "simulation":
                self.store.fill(event_id, filled, average, ZERO, "Paper fill; fees/funding/liquidation not simulated")
            else:
                event = self.store.event(event_id)
                order_id, executed, price, fee = await self.gateway.submit(
                    trade, filled, bound, reduce, event["client_id"],
                    lambda oid: self.store.status(event_id, "pending", "", oid),
                )
                self.store.status(event_id, "pending", "", order_id)
                self.store.fill(event_id, executed, price, fee)
        except Exception as exc:
            logger.exception("Perps order requires reconciliation")
            self.store.status(event_id, "uncertain", str(exc))
            self.pause()
            self.error = "Perps order outcome uncertain. Copying paused; reconcile the persisted client order ID."

    async def reconcile(
        self, event_id: str, quantity: Decimal, price: Decimal, fee: Decimal, note: str,
    ) -> None:
        async with self.lock:
            if self.settings.enabled or not note.strip():
                raise ValueError("Pause Perps copying and provide verified exchange settlement evidence")
            self.store.fill(event_id, quantity, price, fee, "Manually reconciled: " + note.strip())
            self.error = ""

    async def record_flat(self, instrument: int, environment: Literal["simulation", "live"], note: str) -> None:
        async with self.lock:
            if self.settings.enabled or self.store.unresolved() or not note.strip():
                raise ValueError("Pause, reconcile pending orders, and provide evidence before recording a flat position")
            if not any(p["instrument"] == instrument and D(p["quantity"]) for p in self.store.positions(environment)):
                raise ValueError("No open copied position exists for this instrument/environment")
            if environment == "live":
                wallet = await self.gateway.prepare_live()
                if self.store.store.get("perps_live_wallet", "") != wallet:
                    raise ValueError("Cannot reconcile a different or unknown Perps live wallet")
                account = await self.gateway.account()
                if any(p.instrument_id == instrument and p.size for p in account.positions):
                    raise ValueError("Exchange position is not flat; this accounting action never sends orders")
            with self.store.db:
                self.store.db.execute(
                    "UPDATE perps_positions SET quantity='0',entry='0' WHERE instrument=? AND environment=?",
                    (instrument, environment),
                )
                self.store.store.put(f"perps_external_close:{environment}:{instrument}", note.strip())
            self.error = "External close recorded. External P&L/fees/funding are not attributed to the copy ledger."

    def snapshot(self) -> dict:
        result = self.store.snapshot()
        result.update(
            settings=self.settings.model_dump(), error=self.error, last_poll=self.last_poll,
            portfolios=self.portfolios, wallet_errors=self.wallet_errors,
            instruments={str(k): v.symbol for k, v in self.instruments.items()},
            marks={str(k): str(v.mark_price) for k, v in self.tickers.items()},
            account=self.live_account.model_dump(mode="json") if self.live_account else None,
            external_closes={row["key"]: row["value"] for row in self.store.db.execute(
                "SELECT key,value FROM metadata WHERE key LIKE 'perps_external_close:%'"
            )},
        )
        return result
