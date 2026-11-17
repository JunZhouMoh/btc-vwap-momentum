#!/usr/bin/env python3
"""Launch portfolio following, independently of main.py / BTC trading."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from decimal import Decimal
import logging
import os
from pathlib import Path
import re
import secrets
from typing import Annotated, Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware
import uvicorn

from src.path_utils import is_railway_runtime, resolve_runtime_path
from src.portfolio_follow import FollowSettings, PortfolioFollower, PortfolioGateway, PortfolioStore
from src.perps_follow import PerpsFollower, PerpsGateway, PerpsSettings, PerpsStore

ROOT = Path(__file__).resolve().parent
logger = logging.getLogger("portfolio")

LOCAL_HOSTS = ("127.0.0.1", "localhost")
HEALTHCHECK_HOST = "healthcheck.railway.app"


@dataclass(frozen=True)
class DashboardConfig:
    host: str = "127.0.0.1"
    port: int = 8082
    allowed_hosts: tuple[str, ...] = LOCAL_HOSTS
    username: str = "admin"
    password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if not 1 <= self.port <= 65535:
            raise ValueError("Dashboard port must be between 1 and 65535")
        for host in self.allowed_hosts:
            if len(host) > 253 or not re.fullmatch(
                r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*"
                r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", host,
            ):
                raise ValueError("Dashboard allowed hosts must be exact hostnames without schemes or ports")
        remote = self.host not in LOCAL_HOSTS or bool(self.public_origins)
        if remote or self.password:
            if len(self.password) < 16 or not self.password.isascii() or not self.password.isprintable():
                raise ValueError("Set PORTFOLIO_DASHBOARD_PASSWORD to at least 16 printable ASCII characters")
            if not self.username or ":" in self.username or not self.username.isascii() or not self.username.isprintable():
                raise ValueError("PORTFOLIO_DASHBOARD_USER must be a nonempty ASCII username without ':'")

    @property
    def public_origins(self) -> tuple[str, ...]:
        return tuple(
            f"https://{host}" for host in self.allowed_hosts
            if host not in (*LOCAL_HOSTS, HEALTHCHECK_HOST)
        )

    @classmethod
    def from_environment(cls, port: int | None = None) -> DashboardConfig:
        railway = is_railway_runtime()
        hosts = [*LOCAL_HOSTS]
        if railway:
            hosts.append(HEALTHCHECK_HOST)
        hosts.extend(
            host.strip().lower() for host in (
                os.getenv("RAILWAY_PUBLIC_DOMAIN", "") + "," + os.getenv("PORTFOLIO_ALLOWED_HOSTS", "")
            ).split(",") if host.strip()
        )
        return cls(
            host="0.0.0.0" if railway else "127.0.0.1",
            port=port if port is not None else int(os.getenv("PORT", "8082")),
            allowed_hosts=tuple(dict.fromkeys(hosts)),
            username=os.getenv("PORTFOLIO_DASHBOARD_USER", "admin"),
            password=os.getenv("PORTFOLIO_DASHBOARD_PASSWORD", ""),
        )


class SettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    settings: FollowSettings
    confirm_live: bool = False


class ReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str
    shares: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
    cash: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
    note: Annotated[str, Field(min_length=1, max_length=1000)]


class ExternalCloseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    wallet: str
    asset: str
    environment: Literal["simulation", "live"]
    cash: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
    note: Annotated[str, Field(min_length=1, max_length=1000)]


class PerpsSettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    settings: PerpsSettings
    confirm_live: bool = False


class PerpsReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str
    quantity: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
    price: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
    fee: Annotated[Decimal, Field(allow_inf_nan=False)]
    note: Annotated[str, Field(min_length=1, max_length=1000)]


class PerpsFlatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instrument: Annotated[int, Field(ge=0)]
    environment: Literal["simulation", "live"]
    note: Annotated[str, Field(min_length=1, max_length=1000)]


def build_app(
    follower: PortfolioFollower, *, run_worker: bool = True,
    hosting: DashboardConfig | None = None, perps: PerpsFollower | None = None,
) -> FastAPI:
    hosting = hosting or DashboardConfig.from_environment()
    basic = HTTPBasic(auto_error=False)

    async def worker(engine: PortfolioFollower | PerpsFollower) -> None:
        while True:
            try:
                await engine.poll()
            except Exception:
                logger.exception("Copy worker failed; stopping to prevent unsafe copying")
                engine.pause()
                engine.worker_failed = True
                engine.error = "Worker stopped unexpectedly. Restart portfolio mode; inspect server logs."
                return
            await asyncio.sleep(engine.settings.poll_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engines = [follower] + ([perps] if perps is not None else [])
        tasks = [asyncio.create_task(worker(engine)) for engine in engines] if run_worker else []
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with suppress(asyncio.CancelledError):
                    await task
            if perps is not None:
                await perps.gateway.close()
            await follower.gateway.close()
            follower.store.close()

    app = FastAPI(title="Portfolio Following", lifespan=lifespan)

    @app.middleware("http")
    async def dashboard_controls(request: Request, call_next):
        response = None
        healthcheck = request.url.path == "/healthz" and request.method in ("GET", "HEAD")
        if hosting.password and not healthcheck:
            try:
                credentials = await basic(request)
            except HTTPException:
                credentials = None
            username_ok = secrets.compare_digest(
                credentials.username.encode() if credentials else b"", hosting.username.encode()
            )
            password_ok = secrets.compare_digest(
                credentials.password.encode() if credentials else b"", hosting.password.encode()
            )
            if not (username_ok and password_ok):
                response = JSONResponse(
                    {"detail": "Dashboard authentication required"}, status_code=401,
                    headers={"WWW-Authenticate": 'Basic realm="Portfolio dashboard"'},
                )
        if response is None and request.method == "POST":
            origin = request.headers.get("origin")
            expected_origin = (
                str(request.base_url).rstrip("/") if request.url.hostname in LOCAL_HOSTS
                else f"https://{request.url.netloc}"
            )
            if origin and origin != expected_origin:
                response = JSONResponse({"detail": "Cross-origin controls are not allowed"}, status_code=403)
            elif request.headers.get("content-type", "").split(";")[0] != "application/json":
                response = JSONResponse({"detail": "Controls require application/json"}, status_code=415)
        if response is None:
            response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    app.add_middleware(
        TrustedHostMiddleware, allowed_hosts=list(hosting.allowed_hosts), www_redirect=False,
    )

    @app.api_route("/healthz", methods=["GET", "HEAD"])
    async def health():
        failed = follower.worker_failed or perps is not None and perps.worker_failed
        return JSONResponse(
            {"status": "unhealthy" if failed else "ok"},
            status_code=503 if failed else 200,
        )

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTMLResponse((ROOT / "src" / "portfolio_dashboard.html").read_text(encoding="utf-8"))

    @app.get("/api/state")
    async def state():
        return follower.snapshot()

    @app.post("/api/settings")
    async def settings(payload: SettingsRequest):
        try:
            await follower.configure(payload.settings, payload.confirm_live)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Unable to apply portfolio settings")
            raise HTTPException(
                status_code=503, detail="Unable to initialize live trading; inspect server logs"
            ) from exc
        return {"settings": follower.settings.model_dump()}

    @app.post("/api/pause")
    async def pause():
        # Do not queue the kill switch behind a slow source request or exchange response.
        follower.pause()
        return {"enabled": False}

    @app.post("/api/reconcile")
    async def reconcile(payload: ReconcileRequest):
        try:
            await follower.reconcile(payload.event_id, payload.shares, payload.cash, payload.note)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True}

    @app.post("/api/external-close")
    async def external_close(payload: ExternalCloseRequest):
        try:
            await follower.external_close(
                payload.wallet, payload.asset, payload.environment, payload.cash, payload.note
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"ok": True}

    if perps is not None:
        @app.get("/perps", response_class=HTMLResponse)
        async def perps_index():
            return HTMLResponse((ROOT / "src" / "perps_dashboard.html").read_text(encoding="utf-8"))

        @app.get("/api/perps/state")
        async def perps_state():
            return perps.snapshot()

        @app.post("/api/perps/settings")
        async def perps_settings(payload: PerpsSettingsRequest):
            try:
                await perps.configure(payload.settings, payload.confirm_live)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                logger.exception("Unable to apply Perps settings")
                raise HTTPException(status_code=503, detail="Unable to initialize Perps; inspect server logs") from exc
            return {"settings": perps.settings.model_dump()}

        @app.post("/api/perps/pause")
        async def perps_pause():
            perps.pause()
            return {"enabled": False}

        @app.post("/api/perps/reconcile")
        async def perps_reconcile(payload: PerpsReconcileRequest):
            try:
                await perps.reconcile(payload.event_id, payload.quantity, payload.price, payload.fee, payload.note)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            return {"ok": True}

        @app.post("/api/perps/record-flat")
        async def perps_record_flat(payload: PerpsFlatRequest):
            try:
                await perps.record_flat(payload.instrument, payload.environment, payload.note)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                logger.exception("Unable to verify external Perps close")
                raise HTTPException(status_code=503, detail="Unable to verify Perps position; inspect server logs") from exc
            return {"ok": True}

    return app


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Portfolio following (no BTC strategy)")
    parser.add_argument("--port", type=int, default=None, help="Defaults to PORT, otherwise 8082")
    parser.add_argument("--state", default=os.getenv("PORTFOLIO_STATE_PATH", "logs/portfolio_follow.sqlite3"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    hosting = DashboardConfig.from_environment(args.port)
    store = PortfolioStore(resolve_runtime_path(args.state))
    gateway = PortfolioGateway()
    follower = PortfolioFollower(store, gateway)
    perps = PerpsFollower(PerpsStore(store), PerpsGateway())
    logger.info("Portfolio dashboard listening on %s:%s (copying paused)", hosting.host, hosting.port)
    if hosting.host == "0.0.0.0" and not hosting.public_origins:
        logger.warning("Generate a Railway domain or set PORTFOLIO_ALLOWED_HOSTS to access the dashboard")
    uvicorn.run(
        build_app(follower, hosting=hosting, perps=perps), host=hosting.host, port=hosting.port,
        proxy_headers=False,
    )


if __name__ == "__main__":
    main()
