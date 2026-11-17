#!/usr/bin/env python3
"""Launch portfolio following, independently of main.py / BTC trading."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager, suppress
from decimal import Decimal
import logging
from pathlib import Path
from typing import Annotated, Literal

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware
import uvicorn

from src.path_utils import resolve_runtime_path
from src.portfolio_follow import FollowSettings, PortfolioFollower, PortfolioGateway, PortfolioStore

ROOT = Path(__file__).resolve().parent
logger = logging.getLogger("portfolio")


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


def build_app(follower: PortfolioFollower, *, run_worker: bool = True) -> FastAPI:
    async def worker() -> None:
        while True:
            try:
                await follower.poll()
            except Exception:
                logger.exception("Portfolio worker failed; stopping to prevent unsafe copying")
                follower.settings.enabled = False
                follower.worker_failed = True
                follower.error = "Worker stopped unexpectedly. Restart portfolio mode; inspect server logs."
                return
            await asyncio.sleep(follower.settings.poll_seconds)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = asyncio.create_task(worker()) if run_worker else None
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            await follower.gateway.close()
            follower.store.close()

    app = FastAPI(title="Portfolio Following", lifespan=lifespan)
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["127.0.0.1", "localhost"])

    @app.middleware("http")
    async def local_controls(request: Request, call_next):
        if request.method == "POST":
            origin = request.headers.get("origin")
            if origin and origin != str(request.base_url).rstrip("/"):
                return JSONResponse({"detail": "Cross-origin controls are not allowed"}, status_code=403)
            if request.headers.get("content-type", "").split(";")[0] != "application/json":
                return JSONResponse({"detail": "Controls require application/json"}, status_code=415)
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

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

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Portfolio following (no BTC strategy)")
    parser.add_argument("--port", type=int, default=8082)
    parser.add_argument("--state", default="logs/portfolio_follow.sqlite3")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    store = PortfolioStore(resolve_runtime_path(args.state))
    gateway = PortfolioGateway()
    follower = PortfolioFollower(store, gateway)
    logger.info("Portfolio dashboard: http://127.0.0.1:%s (copying paused)", args.port)
    uvicorn.run(build_app(follower), host="127.0.0.1", port=args.port)


if __name__ == "__main__":
    main()
