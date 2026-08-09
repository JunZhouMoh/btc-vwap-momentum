#!/usr/bin/env python3
"""
BTC 5/15-min Live Trading Bot with Real-time Dashboard

Single file that combines:
- Visual terminal dashboard (rich)
- Real order execution
- Hedge management
- Auto-redemption
- Telegram notifications

Usage:
    python main.py
"""

import asyncio
import json
import os
import time
import csv
import math
import statistics
import logging
import re
import signal
import sys
import threading
from datetime import datetime, timezone, timedelta
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Tuple
from pathlib import Path

import aiohttp
import websockets
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout

# Setup logging
Path("logs").mkdir(exist_ok=True)

# Main logger - stderr only (no file output)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stderr),
    ]
)
logger = logging.getLogger("btc_live")

# Detailed order execution logger - no file handler
order_logger = logging.getLogger("btc_live.orders")
order_logger.setLevel(logging.DEBUG)

# Detailed hedge logger - no file handler
hedge_logger = logging.getLogger("btc_live.hedges")
hedge_logger.setLevel(logging.DEBUG)

# Signals logger - disabled (no-op)
class NoOpLogger:
    def info(self, *args, **kwargs): pass
    def debug(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass

signal_logger = NoOpLogger()

# Project imports
from src.config_loader import load_config, validate_config
from src.web_dashboard import WebSnapshotHolder, start_web_dashboard, build_app
from src.order_executor import OrderExecutor, ExecutionConfig
from src.hedge_manager import HedgeManager, HedgeConfig as HedgeManagerConfig, HedgeResult
from src.auto_redeemer import AsyncAutoRedeemer
from src.telegram_notifier import TelegramNotifier
from src.path_utils import resolve_runtime_path
try:
    # Current layout: UserWebSocket lives in websocket_client.
    from src.websocket_client import UserWebSocket
except ImportError:
    # Backward compatibility with older layouts.
    from src.user_websocket import UserWebSocket
from src.simulation_history import SimulationHistoryLogger
from src.btc_volume_feed import BTCVolumeFeed

# Constants
GAMMA_API = "https://gamma-api.polymarket.com"
CRYPTO_PRICE_API = "https://polymarket.com/api/crypto/crypto-price"
WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAINLINK_RTDS_URL = "wss://ws-live-data.polymarket.com"
BINANCE_BTC_WSS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

console = Console()

# ASGI entrypoint for `uvicorn main:app`.
# This standalone app is separate from the in-process dashboard server that the bot starts.
_asgi_holder = WebSnapshotHolder()
app = build_app(_asgi_holder)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Trade:
    """Single trade record"""
    timestamp: float
    price: float
    size: float
    side: str


@dataclass
class TokenData:
    """Data for a single token (Up or Down)"""
    token_id: str
    name: str
    
    best_bid: float = 0.0
    best_bid_size: float = 0.0
    best_ask: float = 0.0
    best_ask_size: float = 0.0
    
    trades: deque = field(default_factory=lambda: deque(maxlen=5000))
    
    last_price: float = 0.0
    last_trade_time: float = 0.0
    
    trade_count: int = 0
    volume_total: float = 0.0
    volume_buy: float = 0.0
    volume_sell: float = 0.0
    
    def reset(self):
        self.best_bid = 0.0
        self.best_bid_size = 0.0
        self.best_ask = 0.0
        self.best_ask_size = 0.0
        self.trades.clear()
        self.last_price = 0.0
        self.last_trade_time = 0.0
        self.trade_count = 0
        self.volume_total = 0.0
        self.volume_buy = 0.0
        self.volume_sell = 0.0


@dataclass
class MarketState:
    """Current market state"""
    market_id: str = ""
    condition_id: str = ""
    slug: str = ""
    end_time: float = 0.0
    
    up_token: Optional[TokenData] = None
    down_token: Optional[TokenData] = None
    
    connected: bool = False
    last_update: float = 0.0
    
    # Chainlink BTC/USD price tracking
    btc_anchor_price: float = 0.0    # Price at market start
    btc_market_anchor_price: float = 0.0  # Fixed anchor for current market (reset only on new market)
    btc_market_anchor_source: str = "none"  # none | fallback_tick | feed_tick | official_ptb
    btc_current_price: float = 0.0   # Latest Chainlink price
    btc_last_update: float = 0.0     # Timestamp of last price update
    btc_anchor_history: deque = field(default_factory=lambda: deque(maxlen=5))  # Recent BTC/anchor samples
    btc_connected: bool = False      # RTDS connection status
    btc_feed_source: str = "chainlink"  # chainlink | binance
    btc_window_moves: deque = field(default_factory=lambda: deque(maxlen=50))  # Completed window abs moves
    btc_current_window_min_usd: float = 0.0
    btc_current_window_max_usd: float = 0.0
    
    # Binance BTC/USD price tracking (parallel to Chainlink)
    binance_current_price: float = 0.0   # Latest Binance price
    binance_last_update: float = 0.0     # Timestamp of last Binance price update
    binance_connected: bool = False      # Binance feed connection status


@dataclass
class Position:
    """Current open position"""
    token_name: str
    token_id: str
    opposite_token_id: str
    entry_price: float
    contracts: int
    entry_time: float
    market_slug: str
    hedged: bool = False
    hedge_contracts: int = 0
    hedge_price: float = 0.0
    min_price_seen: float = 0.0  # Lowest price after entry (for drawdown tracking)
    btc_price_at_entry: float = 0.0    # Chainlink BTC/USD when order was submitted
    btc_anchor_at_entry: float = 0.0   # BTC anchor (market-open price) at order submission
    entry_mode: str = "normal"        # normal | volume_eval_mode | mode_60s | mode_40s | mode_30s | mode_20s | manual
    mode_legs: List[Dict[str, Any]] = field(default_factory=list)  # [{mode, contracts, entry_price}]


@dataclass
class TradeRecord:
    """Completed trade record"""
    market_slug: str
    token_name: str
    entry_price: float
    exit_price: float
    contracts: int
    pnl: float
    won: bool
    timestamp: float
    max_drawdown_abs: float = 0.0   # Max absolute price drop from entry
    max_drawdown_pct: float = 0.0   # Max percentage drawdown from entry
    btc_price_at_entry: float = 0.0        # Chainlink BTC/USD when order was submitted
    btc_anchor_price_at_entry: float = 0.0 # BTC anchor (market-open price) at submission
    btc_price_at_close: float = 0.0        # Chainlink BTC/USD used at market close
    btc_diff_from_anchor: float = 0.0      # btc_price_at_close - btc_anchor_price_at_entry
    entry_mode: str = "unknown"           # Copied from Position.entry_mode
    mode_breakdown: Dict[str, Dict[str, float]] = field(default_factory=dict)  # mode -> {trigger_count,wins,losses,total_pnl_usd}


# =============================================================================
# UTILITIES
# =============================================================================

class IndicatorCalculator:
    @staticmethod
    def get_trades_in_window(trades: deque, window_seconds: float) -> List[Trade]:
        now = time.time()
        cutoff = now - window_seconds
        return [t for t in trades if t.timestamp >= cutoff]
    
    @staticmethod
    def calc_vwap(trades: List[Trade]) -> float:
        if not trades:
            return 0.0
        total_value = sum(t.price * t.size for t in trades)
        total_volume = sum(t.size for t in trades)
        return total_value / total_volume if total_volume > 0 else 0.0
    
    @staticmethod
    def calc_deviation(current_price: float, vwap: float) -> float:
        if vwap == 0:
            return 0.0
        return ((current_price - vwap) / vwap) * 100
    
    @staticmethod
    def calc_momentum(trades: deque, current_price: float, window: float = 120, avg_band: float = 1.5) -> Optional[float]:
        """
        Price change vs average price ~window seconds ago.
        
        Takes all trades in [now-window-avg_band, now-window+avg_band] (3s band),
        computes arithmetic mean, returns % change from that to current_price.
        
        Returns None if no trades found in the band (not enough history).
        """
        now = time.time()
        band_start = now - window - avg_band
        band_end = now - window + avg_band
        
        band_prices = [t.price for t in trades if band_start <= t.timestamp <= band_end]
        
        if not band_prices:
            return None
        
        avg_price_ago = sum(band_prices) / len(band_prices)
        if avg_price_ago == 0:
            return None
        
        return ((current_price - avg_price_ago) / avg_price_ago) * 100
    
    @staticmethod
    def calc_zscore(trades: deque, current_price: float, window: float = 5) -> float:
        now = time.time()
        recent = [t for t in trades if t.timestamp >= now - window]
        if len(recent) < 2:
            return 0.0
        prices = [t.price for t in recent]
        mean_price = statistics.mean(prices)
        std_price = statistics.stdev(prices) if len(prices) > 1 else 0.001
        return (current_price - mean_price) / std_price if std_price > 0 else 0.0
    
    @staticmethod
    def calc_ema(trades: deque, period: int = 9, window: float = 3600) -> Optional[float]:
        """
        Calculate EMA (Exponential Moving Average) for a given period.
        
        Args:
            trades: deque of Trade objects
            period: EMA period (e.g., 9 or 21)
            window: time window in seconds to extract trades from
        
        Returns:
            EMA value or None if not enough data
        """
        if not trades or period < 1:
            return None
        
        now = time.time()
        recent_trades = [t for t in trades if t.timestamp >= now - window]
        
        if len(recent_trades) < period:
            return None
        
        prices = [t.price for t in recent_trades]
        
        # Calculate multiplier
        multiplier = 2.0 / (period + 1)
        
        # Start with SMA for first value
        ema = sum(prices[:period]) / period
        
        # Calculate EMA for remaining prices
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        
        return ema
    
    @staticmethod
    def calc_ema_9(trades: deque, window: float = 3600) -> Optional[float]:
        """Calculate 9-period EMA."""
        return IndicatorCalculator.calc_ema(trades, period=9, window=window)
    
    @staticmethod
    def calc_ema_21(trades: deque, window: float = 3600) -> Optional[float]:
        """Calculate 21-period EMA."""
        return IndicatorCalculator.calc_ema(trades, period=21, window=window)
    
    @staticmethod
    def calc_price_trend(trades: deque, window: float = 10.0) -> Optional[float]:
        """
        Calculate price trend over recent window.
        Returns: positive if price rising, negative if falling, None if insufficient data.
        
        Compares average price from [now-window, now-window/2] to [now-window/2, now].
        """
        if not trades or len(trades) < 2:
            return None
        
        now = time.time()
        mid_point = now - window / 2.0
        old_point = now - window
        
        # Trades in first half [now-window, now-window/2]
        early = [t.price for t in trades if old_point <= t.timestamp <= mid_point]
        # Trades in second half [now-window/2, now]
        recent = [t.price for t in trades if mid_point < t.timestamp <= now]
        
        if not early or not recent:
            return None
        
        avg_early = sum(early) / len(early)
        avg_recent = sum(recent) / len(recent)
        
        # Return change: positive if rising, negative if falling
        return avg_recent - avg_early


class WinRateTable:
    def __init__(self, csv_path: str):
        self.data = {}
        self.price_ranges = []
        self._load(csv_path)
    
    def _load(self, csv_path):
        try:
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    if not row or not row[0]:
                        continue
                    price_range = row[0]
                    self.price_ranges.append(price_range)
                    self.data[price_range] = {}
                    for i, val in enumerate(row[1:], start=0):
                        if val:
                            try:
                                self.data[price_range][i] = float(val)
                            except ValueError:
                                pass
        except Exception as e:
            logger.warning(f"Could not load win_rate.csv: {e}")
    
    def get_winrate(self, price: float, minute: int, interval_minutes: int = 15) -> Optional[float]:
        price_range = None
        for pr in self.price_ranges:
            try:
                low, high = pr.split('-')
                if float(low) <= price <= float(high):
                    price_range = pr
                    break
            except:
                continue
        if not price_range and price > 0.99 and self.price_ranges:
            price_range = self.price_ranges[-1]
        if not price_range:
            return None
        cap = max(0, interval_minutes - 1)
        minute = max(0, min(cap, minute))
        return self.data.get(price_range, {}).get(minute)


# =============================================================================
# TRADING STATS
# =============================================================================

class TradingStats:
    def __init__(self, log_file: str = "logs/trading_log.json"):
        self.log_file = resolve_runtime_path(log_file, json_only=True)
        self.position: Optional[Position] = None
        self.trades: List[TradeRecord] = []
        self.markets_seen: int = 0
        self.current_market_slug: str = ""
        self.position_closed_this_market: bool = False
        self.entry_blocked: bool = False  # Блокировка повторных попыток после таймаута
        self.mode_entry_counts: Dict[str, int] = {}
        self._load()
    
    def _load(self):
        try:
            if self.log_file.exists():
                with open(self.log_file, 'r') as f:
                    data = json.load(f)
                    import dataclasses
                    known = {f.name for f in dataclasses.fields(TradeRecord)}
                    self.trades = [TradeRecord(**{k: v for k, v in t.items() if k in known}) for t in data.get('trades', [])]
                    self.markets_seen = data.get('markets_seen', 0)
        except Exception:
            pass
    
    def summary_dict(self) -> Dict[str, Any]:
        """Aggregates for dashboards and simulation summary files."""
        tc = len(self.trades)
        wins = sum(1 for t in self.trades if t.won)
        losses = tc - wins
        total = sum(t.pnl for t in self.trades)
        pnls = [t.pnl for t in self.trades]
        wr = (wins / tc * 100.0) if tc else 0.0
        by_mode: Dict[str, Dict[str, float]] = {}
        for t in self.trades:
            breakdown = getattr(t, "mode_breakdown", None)
            if isinstance(breakdown, dict) and breakdown:
                for mode_key, mode_data in breakdown.items():
                    mode = str(mode_key or "unknown").strip() or "unknown"
                    bucket = by_mode.setdefault(
                        mode,
                        {
                            "trade_count": 0,
                            "wins": 0,
                            "losses": 0,
                            "win_rate_pct": 0.0,
                            "total_pnl_usd": 0.0,
                        },
                    )
                    triggers = int(float(mode_data.get("trigger_count", 0.0) or 0.0))
                    wins_i = int(float(mode_data.get("wins", 0.0) or 0.0))
                    losses_i = int(float(mode_data.get("losses", 0.0) or 0.0))
                    pnl_i = float(mode_data.get("total_pnl_usd", 0.0) or 0.0)
                    bucket["trade_count"] += triggers
                    bucket["wins"] += wins_i
                    bucket["losses"] += losses_i
                    bucket["total_pnl_usd"] += pnl_i
                continue

            # Backward compatibility for old records without per-mode breakdown.
            mode = str((getattr(t, "entry_mode", "") or "unknown")).strip() or "unknown"
            bucket = by_mode.setdefault(
                mode,
                {
                    "trade_count": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate_pct": 0.0,
                    "total_pnl_usd": 0.0,
                },
            )
            bucket["trade_count"] += 1
            if t.won:
                bucket["wins"] += 1
            else:
                bucket["losses"] += 1
            bucket["total_pnl_usd"] += float(t.pnl)

        for mode_stats in by_mode.values():
            mt = int(mode_stats["trade_count"])
            mw = int(mode_stats["wins"])
            mode_stats["win_rate_pct"] = round((mw / mt * 100.0) if mt else 0.0, 4)
            mode_stats["total_pnl_usd"] = round(float(mode_stats["total_pnl_usd"]), 6)

        return {
            "total_pnl_usd": round(total, 6),
            "trade_count": tc,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(wr, 4),
            "win_rate_by_mode": by_mode,
            "avg_trade_pnl_usd": round(total / tc, 6) if tc else 0.0,
            "best_trade_pnl_usd": round(max(pnls), 6) if pnls else None,
            "worst_trade_pnl_usd": round(min(pnls), 6) if pnls else None,
            "last_close_unix": max((t.timestamp for t in self.trades), default=None),
        }

    def _save(self):
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            trades_data = []
            for t in self.trades:
                trade_dict = t.__dict__.copy()
                readable_time = datetime.fromtimestamp(t.timestamp).strftime('%Y-%m-%d %H:%M:%S')
                trade_dict['timestamp_readable'] = readable_time
                trades_data.append(trade_dict)
            
            summary = self.summary_dict()
            if summary['last_close_unix']:
                readable_last = datetime.fromtimestamp(summary['last_close_unix']).strftime('%Y-%m-%d %H:%M:%S')
                summary['last_close_readable'] = readable_last
            
            data = {
                'trades': trades_data,
                'markets_seen': self.markets_seen,
                'summary': summary,
            }
            with open(self.log_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass
    
    def new_market(self, slug: str):
        if slug != self.current_market_slug:
            self.current_market_slug = slug
            self.markets_seen += 1
            self.position = None
            self.position_closed_this_market = False
            self.entry_blocked = False  # Сброс блокировки для нового рынка
            self.mode_entry_counts = {}
            self._save()
    
    def can_enter(self) -> bool:
        return self.position is None and not self.position_closed_this_market and not self.entry_blocked

    def late_mode_trade_count(self, mode_name: str) -> int:
        return int(self.mode_entry_counts.get(mode_name, 0))

    def can_enter_late_mode(self, mode_name: str, max_trades: int) -> bool:
        return self.late_mode_trade_count(mode_name) < max_trades

    def total_late_mode_trade_count(self) -> int:
        return int(sum(self.mode_entry_counts.values()))

    def can_enter_late_mode_total(self, max_trades: int) -> bool:
        return self.total_late_mode_trade_count() < max_trades

    def record_late_mode_entry(self, mode_name: str):
        self.mode_entry_counts[mode_name] = self.late_mode_trade_count(mode_name) + 1
    
    def block_entry(self, reason: str = ""):
        """Блокирует повторные попытки входа на текущем рынке."""
        self.entry_blocked = True
        if reason:
            logger.warning(f"Entry blocked: {reason}")
    
    def record_entry(self, token_name: str, token_id: str, opposite_token_id: str,
                     price: float, contracts: int, market_slug: str,
                     btc_price_at_entry: float = 0.0, btc_anchor_at_entry: float = 0.0,
                     entry_mode: str = "normal"):
        mode = (str(entry_mode).strip() or "normal")
        self.position = Position(
            token_name=token_name,
            token_id=token_id,
            opposite_token_id=opposite_token_id,
            entry_price=price,
            contracts=contracts,
            entry_time=time.time(),
            market_slug=market_slug,
            min_price_seen=price,  # Start tracking from entry price
            btc_price_at_entry=btc_price_at_entry,
            btc_anchor_at_entry=btc_anchor_at_entry,
            entry_mode=mode,
            mode_legs=[{
                "mode": mode,
                "contracts": int(contracts),
                "entry_price": float(price),
            }],
        )

    def add_to_position(
        self,
        price: float,
        contracts: int,
        btc_price_at_entry: float = 0.0,
        entry_mode: str = "",
    ):
        """Scale into an existing position by averaging entry price and contracts."""
        if not self.position or contracts <= 0:
            return

        existing_contracts = self.position.contracts
        total_contracts = existing_contracts + contracts
        self.position.entry_price = (
            (self.position.entry_price * existing_contracts) + (price * contracts)
        ) / total_contracts

        if btc_price_at_entry > 0:
            if self.position.btc_price_at_entry > 0:
                self.position.btc_price_at_entry = (
                    (self.position.btc_price_at_entry * existing_contracts)
                    + (btc_price_at_entry * contracts)
                ) / total_contracts
            else:
                self.position.btc_price_at_entry = btc_price_at_entry

        # If user manually adds to an existing position, classify it as manual.
        mode = str(entry_mode or "").strip().lower()
        if mode == "manual":
            self.position.entry_mode = "manual"

        self.position.mode_legs.append({
            "mode": (str(entry_mode).strip() or self.position.entry_mode or "unknown"),
            "contracts": int(contracts),
            "entry_price": float(price),
        })

        self.position.contracts = total_contracts
    
    def record_hedge(self, contracts: int, price: float):
        if self.position:
            self.position.hedged = True
            self.position.hedge_contracts = contracts
            self.position.hedge_price = price
    
    def update_drawdown(self, current_price: float):
        """Track minimum price seen since entry for drawdown calculation."""
        if self.position and current_price > 0:
            if current_price < self.position.min_price_seen:
                self.position.min_price_seen = current_price
    
    def close_position(self, final_price: float, btc_price_at_close: float = 0.0) -> Optional[TradeRecord]:
        if not self.position:
            return None
        
        won = final_price >= 0.70  # Win threshold
        entry_cost = self.position.contracts * self.position.entry_price
        
        if won:
            pnl = self.position.contracts - entry_cost
        else:
            pnl = -entry_cost
        
        # Calculate max drawdown from entry
        dd_abs = max(0, self.position.entry_price - self.position.min_price_seen)
        dd_pct = (dd_abs / self.position.entry_price * 100) if self.position.entry_price > 0 else 0
        
        btc_e = self.position.btc_price_at_entry
        btc_a = self.position.btc_anchor_at_entry
        btc_c = btc_price_at_close if btc_price_at_close > 0 else 0.0
        btc_diff = (btc_c - btc_a) if (btc_c > 0 and btc_a > 0) else 0.0

        mode_breakdown: Dict[str, Dict[str, float]] = {}
        legs = list(getattr(self.position, "mode_legs", []) or [])
        if not legs:
            legs = [{
                "mode": (str(self.position.entry_mode).strip() or "unknown"),
                "contracts": int(self.position.contracts),
                "entry_price": float(self.position.entry_price),
            }]

        for leg in legs:
            mode = str(leg.get("mode", "") or "unknown").strip() or "unknown"
            leg_contracts = int(float(leg.get("contracts", 0) or 0))
            leg_entry_price = float(leg.get("entry_price", 0.0) or 0.0)
            if leg_contracts <= 0 or leg_entry_price <= 0:
                continue

            leg_cost = leg_contracts * leg_entry_price
            leg_pnl = (leg_contracts - leg_cost) if won else (-leg_cost)

            bucket = mode_breakdown.setdefault(
                mode,
                {
                    "trigger_count": 0.0,
                    "wins": 0.0,
                    "losses": 0.0,
                    "total_pnl_usd": 0.0,
                },
            )
            bucket["trigger_count"] += 1.0
            if won:
                bucket["wins"] += 1.0
            else:
                bucket["losses"] += 1.0
            bucket["total_pnl_usd"] += float(leg_pnl)

        unique_modes = list(mode_breakdown.keys())
        if len(unique_modes) == 1:
            entry_mode_for_record = unique_modes[0]
        elif len(unique_modes) > 1:
            # Keep a concrete mode label for dashboards; breakdown still preserves all legs.
            mode_contracts: Dict[str, int] = {}
            for leg in legs:
                mode = str(leg.get("mode", "") or "unknown").strip() or "unknown"
                try:
                    mode_contracts[mode] = mode_contracts.get(mode, 0) + int(float(leg.get("contracts", 0) or 0))
                except (TypeError, ValueError):
                    continue
            if mode_contracts:
                entry_mode_for_record = sorted(mode_contracts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]
            else:
                entry_mode_for_record = unique_modes[0]
        else:
            entry_mode_for_record = (str(self.position.entry_mode).strip() or "unknown")
        
        record = TradeRecord(
            market_slug=self.position.market_slug,
            token_name=self.position.token_name,
            entry_price=self.position.entry_price,
            exit_price=final_price,
            contracts=self.position.contracts,
            pnl=pnl,
            won=won,
            timestamp=self.position.entry_time,
            max_drawdown_abs=dd_abs,
            max_drawdown_pct=dd_pct,
            btc_price_at_entry=btc_e,
            btc_anchor_price_at_entry=btc_a,
            btc_price_at_close=btc_c,
            btc_diff_from_anchor=btc_diff,
            entry_mode=entry_mode_for_record,
            mode_breakdown=mode_breakdown,
        )
        
        self.trades.append(record)
        self.position = None
        self.position_closed_this_market = True
        self._save()
        return record

    def close_position_early(self, exit_price: float, btc_price_at_close: float = 0.0) -> Optional[TradeRecord]:
        """Close an open position at current market price (manual exit)."""
        if not self.position:
            return None

        px = max(0.0, float(exit_price))
        pnl = self.position.contracts * (px - self.position.entry_price)
        won = pnl >= 0.0

        # Keep drawdown stats for consistency with normal close records.
        dd_abs = max(0, self.position.entry_price - self.position.min_price_seen)
        dd_pct = (dd_abs / self.position.entry_price * 100) if self.position.entry_price > 0 else 0

        btc_e = self.position.btc_price_at_entry
        btc_a = self.position.btc_anchor_at_entry
        btc_c = btc_price_at_close if btc_price_at_close > 0 else 0.0
        btc_diff = (btc_c - btc_a) if (btc_c > 0 and btc_a > 0) else 0.0

        mode_breakdown: Dict[str, Dict[str, float]] = {}
        legs = list(getattr(self.position, "mode_legs", []) or [])
        if not legs:
            legs = [{
                "mode": (str(self.position.entry_mode).strip() or "unknown"),
                "contracts": int(self.position.contracts),
                "entry_price": float(self.position.entry_price),
            }]

        for leg in legs:
            mode = str(leg.get("mode", "") or "unknown").strip() or "unknown"
            leg_contracts = int(float(leg.get("contracts", 0) or 0))
            leg_entry_price = float(leg.get("entry_price", 0.0) or 0.0)
            if leg_contracts <= 0 or leg_entry_price <= 0:
                continue

            leg_pnl = leg_contracts * (px - leg_entry_price)
            bucket = mode_breakdown.setdefault(
                mode,
                {
                    "trigger_count": 0.0,
                    "wins": 0.0,
                    "losses": 0.0,
                    "total_pnl_usd": 0.0,
                },
            )
            bucket["trigger_count"] += 1.0
            if leg_pnl >= 0:
                bucket["wins"] += 1.0
            else:
                bucket["losses"] += 1.0
            bucket["total_pnl_usd"] += float(leg_pnl)

        unique_modes = list(mode_breakdown.keys())
        if len(unique_modes) == 1:
            entry_mode_for_record = unique_modes[0]
        elif len(unique_modes) > 1:
            mode_contracts: Dict[str, int] = {}
            for leg in legs:
                mode = str(leg.get("mode", "") or "unknown").strip() or "unknown"
                try:
                    mode_contracts[mode] = mode_contracts.get(mode, 0) + int(float(leg.get("contracts", 0) or 0))
                except (TypeError, ValueError):
                    continue
            if mode_contracts:
                entry_mode_for_record = sorted(mode_contracts.items(), key=lambda item: (item[1], item[0]), reverse=True)[0][0]
            else:
                entry_mode_for_record = unique_modes[0]
        else:
            entry_mode_for_record = (str(self.position.entry_mode).strip() or "unknown")

        record = TradeRecord(
            market_slug=self.position.market_slug,
            token_name=self.position.token_name,
            entry_price=self.position.entry_price,
            exit_price=px,
            contracts=self.position.contracts,
            pnl=pnl,
            won=won,
            timestamp=self.position.entry_time,
            max_drawdown_abs=dd_abs,
            max_drawdown_pct=dd_pct,
            btc_price_at_entry=btc_e,
            btc_anchor_price_at_entry=btc_a,
            btc_price_at_close=btc_c,
            btc_diff_from_anchor=btc_diff,
            entry_mode=entry_mode_for_record,
            mode_breakdown=mode_breakdown,
        )

        self.trades.append(record)
        self.position = None
        self.position_closed_this_market = True
        self._save()
        return record
    
    @property
    def total_pnl(self) -> float:
        return sum(t.pnl for t in self.trades)
    
    @property
    def win_count(self) -> int:
        return sum(1 for t in self.trades if t.won)
    
    @property
    def trade_count(self) -> int:
        return len(self.trades)
    
    @property
    def win_rate(self) -> float:
        if not self.trades:
            return 0.0
        return (self.win_count / self.trade_count) * 100


# =============================================================================
# WEBSOCKET CLIENT
# =============================================================================

class WebSocketClient:
    def __init__(self, state: MarketState):
        self.state = state
        self.running = False
        self._tokens_validated = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
    
    def _validate_tokens(self):
        """Log token prices after first WebSocket data received.
        
        NOTE: Token swap logic was REMOVED because it was buggy.
        The API token assignment should be trusted.
        """
        if self._tokens_validated:
            return
        
        up = self.state.up_token
        down = self.state.down_token
        
        if not up or not down:
            return
        
        up_price = up.best_bid or up.best_ask or up.last_price
        down_price = down.best_bid or down.best_ask or down.last_price
        
        # Only log once we have valid prices
        if up_price > 0.05 and down_price > 0.05:
            price_sum = up_price + down_price
            logger.info(f"Tokens validated: UP={up_price:.2f}, DOWN={down_price:.2f}, sum={price_sum:.2f}")
            self._tokens_validated = True
    
    async def connect(self):
        self.running = True
        
        while self.running:
            try:
                async with websockets.connect(WSS_URL) as ws:
                    self._ws = ws
                    self.state.connected = True
                    
                    token_ids = []
                    if self.state.up_token:
                        token_ids.append(self.state.up_token.token_id)
                    if self.state.down_token:
                        token_ids.append(self.state.down_token.token_id)
                    
                    # Log exact token_ids being subscribed
                    logger.info(f"WebSocket subscribing to tokens:")
                    logger.info(f"  UP: {self.state.up_token.token_id[:40]}..." if self.state.up_token else "  UP: None")
                    logger.info(f"  DOWN: {self.state.down_token.token_id[:40]}..." if self.state.down_token else "  DOWN: None")
                    
                    await ws.send(json.dumps({"assets_ids": token_ids, "type": "market"}))
                    
                    async for message in ws:
                        if not self.running:
                            break
                        await self._handle_message(message)
                    
                    self._ws = None
                        
            except websockets.ConnectionClosed:
                self._ws = None
                self.state.connected = False
                if self.running:
                    await asyncio.sleep(1)
            except Exception:
                self._ws = None
                self.state.connected = False
                if self.running:
                    await asyncio.sleep(2)
    
    async def disconnect(self):
        """Gracefully close WebSocket connection with code 1000 (normal closure)."""
        self.running = False
        if self._ws:
            try:
                await self._ws.close(code=1000, reason="Normal shutdown")
                logger.info("WebSocket closed gracefully (code 1000)")
            except Exception as e:
                logger.warning(f"Error during WebSocket close: {e}")
            finally:
                self._ws = None
        self.state.connected = False
    
    async def _handle_message(self, message: str):
        try:
            data = json.loads(message)
            
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        await self._process_item(item)
            elif isinstance(data, dict):
                await self._process_item(data)
            
            self.state.last_update = time.time()
            
            # Validate tokens after receiving price data
            if not self._tokens_validated:
                self._validate_tokens()
        except Exception:
            pass
    
    async def _process_item(self, data: dict):
        event_type = data.get("event_type", "")
        
        if event_type == "last_trade_price":
            asset_id = data.get("asset_id")
            token = self._get_token(asset_id)
            
            if not token and asset_id:
                # Asset ID doesn't match our tokens - might indicate subscription issue
                logger.warning(f"Received price for unknown asset: {asset_id[:30]}...")
                logger.warning(f"  Our UP token: {self.state.up_token.token_id[:30] if self.state.up_token else 'None'}...")
                logger.warning(f"  Our DOWN token: {self.state.down_token.token_id[:30] if self.state.down_token else 'None'}...")
            
            if token:
                price = float(data.get("price", 0))
                size = float(data.get("size", 0))
                side = data.get("side", "BUY")
                
                if price > 0 and size > 0:
                    token.last_price = price
                    token.last_trade_time = time.time()
                    token.trades.append(Trade(time.time(), price, size, side))
                    token.trade_count += 1
                    token.volume_total += size
                    if side == "BUY":
                        token.volume_buy += size
                    else:
                        token.volume_sell += size
        
        elif event_type == "price_change":
            for change in data.get("price_changes", []):
                token = self._get_token(change.get("asset_id"))
                if token:
                    if change.get("best_bid"):
                        token.best_bid = float(change["best_bid"])
                    if change.get("best_ask"):
                        token.best_ask = float(change["best_ask"])
        
        elif event_type == "book":
            token = self._get_token(data.get("asset_id"))
            if token:
                bids = data.get("bids", [])
                if bids:
                    bids.sort(key=lambda x: float(x["price"]), reverse=True)
                    token.best_bid = float(bids[0]["price"])
                    token.best_bid_size = float(bids[0]["size"])
                asks = data.get("asks", [])
                if asks:
                    asks.sort(key=lambda x: float(x["price"]))
                    token.best_ask = float(asks[0]["price"])
                    token.best_ask_size = float(asks[0]["size"])
    
    def _get_token(self, asset_id: str) -> Optional[TokenData]:
        if self.state.up_token and asset_id == self.state.up_token.token_id:
            return self.state.up_token
        elif self.state.down_token and asset_id == self.state.down_token.token_id:
            return self.state.down_token
        return None
    
    def stop(self):
        """Stop WebSocket (sync version - just sets flag)."""
        self.running = False
    
    async def stop_graceful(self):
        """Stop WebSocket gracefully with proper close."""
        await self.disconnect()


# =============================================================================
# CHAINLINK BTC PRICE CLIENT
# =============================================================================

class ChainlinkPriceClient:
    """
    Always-on BTC price stream with source switch (chainlink | binance).
    
    Autonomously tracks market boundaries (epoch-aligned to interval length)
    and snapshots the anchor price at the exact boundary crossing, independent
    of the bot's market finding flow. This ensures the anchor is captured
    within ~1 second of the real boundary, not 5-15s later.
    """
    
    def __init__(
        self,
        state: 'MarketState',
        market_duration_sec: int,
        feed_source: str = "chainlink",
        feed_url: Optional[str] = None,
    ):
        self.state = state
        self._market_duration = int(market_duration_sec)
        if self._market_duration <= 0:
            self._market_duration = 900
        source = (feed_source or "chainlink").strip().lower()
        self._feed_source = source if source in {"chainlink", "binance"} else "chainlink"
        self._feed_url = feed_url or (
            CHAINLINK_RTDS_URL if self._feed_source == "chainlink" else BINANCE_BTC_WSS_URL
        )
        self.running = False
        self._ws = None
        self._ping_task: Optional[asyncio.Task] = None
        self._ptb_task: Optional[asyncio.Task] = None
        self._ptb_slug: str = ""
        self._ptb_next_retry_ts: float = 0.0
        # Track which window the current anchor belongs to
        self._current_window: int = 0
        # Buffer: last price before boundary (for most accurate anchor)
        self._last_price_before_boundary: float = 0.0
        self._last_price_ts: float = 0.0
        # Signed USD move extrema within the current window (relative to anchor).
        self._window_min_move_usd: float = 0.0
        self._window_max_move_usd: float = 0.0
    
    def _get_window(self, ts: float) -> int:
        """Window start timestamp (epoch) for the configured interval."""
        d = self._market_duration
        return int(ts) // d * d
    
    DATA_TIMEOUT = 30  # seconds without any message → force reconnect

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return (symbol or "").strip().lower().replace("/", "").replace("-", "").replace("_", "")

    @classmethod
    def _is_btc_symbol(cls, symbol: str) -> bool:
        s = cls._normalize_symbol(symbol)
        return s in {"btcusd", "btcusdt"}

    def _ptb_variant(self) -> str:
        override = (os.getenv("POLY_CRYPTO_PRICE_VARIANT") or "").strip()
        if override:
            return override
        # For Polymarket BTC up/down 5m and 15m windows, variant='fifteen' matches UI behavior.
        if self._market_duration in (300, 900):
            return "fifteen"
        return "fifteen"

    @staticmethod
    def _iso_from_epoch(ts: float) -> str:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _market_start_from_slug(slug: str) -> Optional[int]:
        m = re.search(r"btc-updown-[^-]+-(\d+)$", str(slug or ""))
        if not m:
            return None
        try:
            return int(m.group(1))
        except (TypeError, ValueError):
            return None

    async def _fetch_official_market_anchor(self, slug: str, end_time: float) -> Optional[float]:
        start_ts = self._market_start_from_slug(slug)
        if not start_ts or end_time <= 0:
            return None

        params = {
            "symbol": "BTC",
            "eventStartTime": self._iso_from_epoch(float(start_ts)),
            "variant": self._ptb_variant(),
            "endDate": self._iso_from_epoch(float(end_time)),
        }
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://polymarket.com/",
        }

        timeout = aiohttp.ClientTimeout(total=8)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(CRYPTO_PRICE_API, params=params, headers=headers) as resp:
                    if resp.status != 200:
                        logger.debug(f"PTB request HTTP {resp.status} for {slug}")
                        return None
                    payload = await resp.json()
        except Exception as e:
            logger.debug(f"PTB request failed for {slug}: {e}")
            return None

        try:
            open_price = float(payload.get("openPrice") or 0.0)
        except (TypeError, ValueError):
            open_price = 0.0

        if open_price > 0:
            return open_price

        return None

    def _maybe_schedule_official_anchor_fetch(self):
        slug = str(self.state.slug or "")
        if not slug:
            return
        if self.state.btc_market_anchor_source == "official_ptb":
            return

        now = time.time()
        if slug != self._ptb_slug:
            self._ptb_slug = slug
            self._ptb_next_retry_ts = 0.0

        if now < self._ptb_next_retry_ts:
            return

        if self._ptb_task and not self._ptb_task.done():
            return

        end_time_snapshot = float(self.state.end_time or 0.0)
        self._ptb_next_retry_ts = now + 5.0
        self._ptb_task = asyncio.create_task(
            self._resolve_official_anchor_once(slug, end_time_snapshot)
        )

    async def _resolve_official_anchor_once(self, slug: str, end_time_snapshot: float):
        price = await self._fetch_official_market_anchor(slug, end_time_snapshot)
        if price and price > 0 and slug == (self.state.slug or ""):
            prev = self.state.btc_market_anchor_price
            self.state.btc_market_anchor_price = price
            self.state.btc_market_anchor_source = "official_ptb"
            if prev > 0 and abs(prev - price) > 0.01:
                logger.info(
                    f"BTC market anchor overridden by official PTB: ${prev:,.2f} -> ${price:,.2f} "
                    f"(market {slug})"
                )
            else:
                logger.info(
                    f"BTC market anchor set from official PTB: ${price:,.2f} (market {slug})"
                )
    
    async def connect(self):
        """Connect to BTC price stream. Always on."""
        self.running = True
        self._last_msg_time = time.time()
        
        while self.running:
            try:
                async with websockets.connect(self._feed_url) as ws:
                    self._ws = ws
                    self.state.btc_connected = True
                    self._last_msg_time = time.time()
                    logger.info(f"BTC feed connected ({self._feed_source}): {self._feed_url}")

                    if self._feed_source == "chainlink":
                        subscribe_msg = json.dumps({
                            "action": "subscribe",
                            "subscriptions": [
                                {
                                    "topic": "crypto_prices_chainlink",
                                    "type": "*",
                                    "filters": ""
                                }
                            ]
                        })
                        await ws.send(subscribe_msg)
                    
                    # Start ping task and watchdog
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    watchdog_task = asyncio.create_task(self._watchdog(ws))
                    
                    try:
                        async for message in ws:
                            if not self.running:
                                break
                            self._last_msg_time = time.time()
                            self._handle_message(message)
                    finally:
                        watchdog_task.cancel()
                        try:
                            await watchdog_task
                        except asyncio.CancelledError:
                            pass
                    
                    self._ws = None
                    
            except websockets.ConnectionClosed:
                self._ws = None
                self.state.btc_connected = False
                if self.running:
                    logger.warning("BTC feed disconnected, reconnecting in 2s...")
                    await asyncio.sleep(2)
            except Exception as e:
                self._ws = None
                self.state.btc_connected = False
                if self.running:
                    logger.warning(f"BTC feed error: {e}, reconnecting in 5s...")
                    await asyncio.sleep(5)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except:
                        pass
                    self._ping_task = None
    
    async def _watchdog(self, ws):
        """Force-close WebSocket if no messages received for DATA_TIMEOUT seconds."""
        try:
            while self.running:
                await asyncio.sleep(5)
                silence = time.time() - self._last_msg_time
                if silence > self.DATA_TIMEOUT:
                    logger.warning(
                        f"RTDS Chainlink watchdog: no data for {silence:.0f}s, forcing reconnect"
                    )
                    self.state.btc_connected = False
                    await ws.close()
                    break
        except asyncio.CancelledError:
            pass
    
    def _handle_message(self, message: str):
        """Parse incoming BTC price message and auto-detect market boundaries."""
        try:
            if not isinstance(message, str) or not message.strip():
                return
            
            data = json.loads(message)

            if self._feed_source == "chainlink":
                topic = data.get("topic", "")
                if topic != "crypto_prices_chainlink":
                    return

                payload = data.get("payload", {}) if isinstance(data, dict) else {}
                symbol = str(payload.get("symbol", ""))
                if not self._is_btc_symbol(symbol):
                    return

                price = float(payload.get("value", 0))
                ts_ms = payload.get("timestamp") or 0
            else:
                # Binance trade stream payload: {"e":"trade","s":"BTCUSDT","p":"...","T":...}
                # Binance bookTicker payload: {"s":"BTCUSDT","b":"...","a":"...","E":...}
                symbol = str(data.get("s", "") or data.get("symbol", ""))
                if not self._is_btc_symbol(symbol):
                    return

                if "p" in data:
                    price = float(data.get("p", 0))
                else:
                    bid = float(data.get("b", 0) or 0)
                    ask = float(data.get("a", 0) or 0)
                    if bid > 0 and ask > 0:
                        price = (bid + ask) / 2.0
                    else:
                        price = bid or ask
                ts_ms = data.get("T") or data.get("E") or 0

            if price <= 0:
                return
            
            # Use feed timestamp (ms) for precise boundary detection.
            # Chainlink uses payload timestamp; Binance uses T (trade time) or E (event time).
            if ts_ms:
                price_ts = float(ts_ms) / 1000.0
            else:
                price_ts = time.time()
            
            now = time.time()
            
            # Update current price (always)
            self.state.btc_current_price = price
            self.state.btc_last_update = now

            # Capture fixed anchor once per market (settlement anchor).
            if self.state.slug and self.state.btc_market_anchor_price <= 0:
                self.state.btc_market_anchor_price = price
                self.state.btc_market_anchor_source = "feed_tick"
                logger.info(
                    f"BTC market anchor set: ${price:,.2f} (market {self.state.slug})"
                )

            self._maybe_schedule_official_anchor_fetch()
            
            # === CALIBRATION LOG: every tick within [-15s..+5s] of any boundary ===
            price_window = self._get_window(price_ts)
            next_boundary = price_window + self._market_duration
            secs_to_next = next_boundary - price_ts
            secs_from_prev = price_ts - price_window
            
            # Log if within 15s before next boundary OR 5s after current boundary start
            if secs_to_next <= 15.0 or secs_from_prev <= 5.0:
                cl_time = datetime.fromtimestamp(price_ts, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]
                local_time = datetime.fromtimestamp(now, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]
                if secs_from_prev <= 5.0:
                    offset_str = f"+{secs_from_prev:.3f}s after {datetime.fromtimestamp(price_window, tz=timezone.utc).strftime('%H:%M:%S')}"
                else:
                    offset_str = f"-{secs_to_next:.3f}s before {datetime.fromtimestamp(next_boundary, tz=timezone.utc).strftime('%H:%M:%S')}"
                logger.info(
                    f"BTC_TICK {cl_time} (local {local_time}) ${price:,.2f} [{offset_str}]"
                )
            
            # Detect window boundary crossing
            
            if self._current_window == 0:
                # First price ever — initialize
                self._current_window = price_window
                self.state.btc_anchor_price = price
                self._window_min_move_usd = 0.0
                self._window_max_move_usd = 0.0
                self.state.btc_current_window_min_usd = 0.0
                self.state.btc_current_window_max_usd = 0.0
                logger.info(
                    f"BTC feed init: ${price:,.2f} "
                    f"(window {self._current_window}, "
                    f"ts={datetime.fromtimestamp(price_ts, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]})"
                )
            elif price_window != self._current_window:
                # === NEW WINDOW === use FIRST tick of new window as anchor
                # Calibrated: reference program uses the first tick AT or AFTER boundary
                old_anchor = self.state.btc_anchor_price
                old_window = self._current_window

                # Finalize completed window movement before switching anchor.
                old_close = self._last_price_before_boundary if self._last_price_before_boundary > 0 else price
                if old_anchor > 0 and old_close > 0:
                    signed_usd = old_close - old_anchor
                    signed_pct = (signed_usd / old_anchor * 100)
                    min_move_usd = min(float(self._window_min_move_usd), float(self._window_max_move_usd), float(signed_usd), 0.0)
                    max_move_usd = max(float(self._window_min_move_usd), float(self._window_max_move_usd), float(signed_usd), 0.0)
                    self.state.btc_window_moves.append({
                        "window": old_window,
                        "signed_usd": signed_usd,
                        "signed_pct": signed_pct,
                        "abs_usd": abs(signed_usd),
                        "abs_pct": abs(signed_pct),
                        "min_usd": min_move_usd,
                        "max_usd": max_move_usd,
                    })
                
                self.state.btc_anchor_price = price  # First tick of new window
                self._current_window = price_window
                self._window_min_move_usd = 0.0
                self._window_max_move_usd = 0.0
                self.state.btc_current_window_min_usd = 0.0
                self.state.btc_current_window_max_usd = 0.0
                
                boundary_time = datetime.fromtimestamp(price_window, tz=timezone.utc).strftime('%H:%M:%S')
                price_time = datetime.fromtimestamp(price_ts, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3]
                delay_ms = (price_ts - price_window) * 1000
                
                logger.info(
                    f"BTC anchor reset: ${self.state.btc_anchor_price:,.2f} "
                    f"(boundary {boundary_time}, first tick at {price_time}, "
                    f"delay {delay_ms:.0f}ms, prev anchor ${old_anchor:,.2f})"
                )
            
            # Always buffer the latest price for next boundary crossing
            self._last_price_before_boundary = price
            self._last_price_ts = price_ts

            # Track min/max signed move for the active window.
            anchor = float(self.state.btc_anchor_price or 0.0)
            if anchor > 0:
                signed_move_usd = float(price - anchor)
                if signed_move_usd < self._window_min_move_usd:
                    self._window_min_move_usd = signed_move_usd
                if signed_move_usd > self._window_max_move_usd:
                    self._window_max_move_usd = signed_move_usd
                self.state.btc_current_window_min_usd = float(self._window_min_move_usd)
                self.state.btc_current_window_max_usd = float(self._window_max_move_usd)

            # Keep a short history for web dashboard visibility (one row per window).
            history_row = {
                "ts": int(now),
                "window_ts": int(price_window),
                "btc_price": float(self.state.btc_current_price or 0.0),
                "anchor_price": float(self.state.btc_anchor_price or 0.0),
                "market_anchor_price": float(self.state.btc_market_anchor_price or 0.0),
            }
            if self.state.btc_anchor_history and int(self.state.btc_anchor_history[-1].get("window_ts", -1)) == int(price_window):
                self.state.btc_anchor_history[-1] = history_row
            else:
                self.state.btc_anchor_history.append(history_row)
            
        except (json.JSONDecodeError, ValueError, KeyError, TypeError):
            pass
    
    async def _ping_loop(self, ws):
        """Send ping every 5 seconds to keep connection alive."""
        try:
            while self.running:
                await asyncio.sleep(5)
                try:
                    await ws.ping()
                except Exception:
                    break
        except asyncio.CancelledError:
            pass
    
    async def disconnect(self):
        """Gracefully close BTC price WebSocket connection."""
        self.running = False

        if self._ptb_task and not self._ptb_task.done():
            self._ptb_task.cancel()
            try:
                await self._ptb_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            self._ptb_task = None
        
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except:
                pass
            self._ping_task = None
        
        if self._ws:
            try:
                if self._feed_source == "chainlink":
                    unsub_msg = json.dumps({
                        "action": "unsubscribe",
                        "subscriptions": [
                            {
                                "topic": "crypto_prices_chainlink",
                                "type": "*",
                                "filters": ""
                            }
                        ]
                    })
                    await self._ws.send(unsub_msg)
                await self._ws.close(code=1000, reason="Normal shutdown")
                logger.info("BTC feed closed gracefully")
            except Exception as e:
                logger.warning(f"BTC feed close error: {e}")
            finally:
                self._ws = None
        
        self.state.btc_connected = False


# =============================================================================
# BTC PRICE MOVEMENT LOGGER (tracks BTC price after each buy)
# =============================================================================

class BTCPriceMovementLogger:
    """
    Tracks BTC price movements after each buy order.
    Records BTC price at buy time and periodically updates until the 5-minute
    window per buy expires, then flushes that buy immediately to the log file.
    """

    WINDOW_SEC = 300  # 5 minutes per buy

    def __init__(self, log_file: str = "logs/btc_price_movements.jsonl"):
        self.log_file = resolve_runtime_path(log_file, json_only=True)
        self.buy_events: List[Dict[str, Any]] = []  # Each buy with its BTC price snapshots
        self._lock = threading.Lock()

    def record_buy(self, market_slug: str, token_name: str, entry_price: float,
                   contracts: int, btc_price: float, timestamp: float):
        """Record a new buy with initial BTC price."""
        with self._lock:
            self.buy_events.append({
                "buy_id": len(self.buy_events),
                "market_slug": market_slug,
                "token_name": token_name,
                "entry_price": entry_price,
                "contracts": contracts,
                "buy_timestamp": timestamp,
                "buy_time_readable": datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "window_end_timestamp": timestamp + self.WINDOW_SEC,
                "btc_price_at_buy": btc_price,
                "btc_price_snapshots": [
                    {
                        "timestamp": timestamp,
                        "btc_price": btc_price,
                        "btc_change_usd": 0.0,
                        "btc_change_pct": 0.0,
                        "time_elapsed_sec": 0,
                    }
                ],
                "_logged": False,
            })

    def _write_buy_event(self, buy_event: Dict[str, Any]):
        """Flush a single buy event to the JSONL log (caller must NOT hold lock)."""
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(buy_event) + '\n')
            logger.info(
                f"BTC price movement logged to {self.log_file} "
                f"(buy_id={buy_event['buy_id']}, "
                f"final_change={buy_event.get('final_btc_change_usd', 'n/a')} USD)"
            )
        except Exception as e:
            logger.error(f"Failed to write BTC price movement: {e}")

    def update_btc_prices(self, btc_price: float, current_timestamp: float):
        """Update BTC price snapshots for all active buys.

        When a buy's 5-minute window expires the snapshot is finalised and
        immediately written to the log so we don't have to wait for session end.
        """
        if btc_price <= 0:
            return

        to_flush = []  # collect events to write outside the lock

        with self._lock:
            for buy_event in self.buy_events:
                if buy_event["_logged"]:
                    continue  # already written, skip

                buy_timestamp = buy_event["buy_timestamp"]
                btc_at_buy = buy_event["btc_price_at_buy"]

                # Calculate change
                change_usd = btc_price - btc_at_buy
                change_pct = (change_usd / btc_at_buy * 100) if btc_at_buy > 0 else 0.0
                time_elapsed = int(current_timestamp - buy_timestamp)

                # Add new snapshot
                buy_event["btc_price_snapshots"].append({
                    "timestamp": current_timestamp,
                    "btc_price": btc_price,
                    "btc_change_usd": round(change_usd, 2),
                    "btc_change_pct": round(change_pct, 4),
                    "time_elapsed_sec": time_elapsed,
                })

                # Keep only snapshots (every ~5 seconds to avoid too much data)
                # Keep at least the first, last, and ones every 5 seconds
                if len(buy_event["btc_price_snapshots"]) > 2:
                    snapshots = buy_event["btc_price_snapshots"]
                    first = snapshots[0]
                    last = snapshots[-1]
                    filtered = [first]
                    last_kept_ts = first["timestamp"]
                    for snap in snapshots[1:-1]:
                        if snap["timestamp"] - last_kept_ts >= 5.0:
                            filtered.append(snap)
                            last_kept_ts = snap["timestamp"]
                    filtered.append(last)
                    buy_event["btc_price_snapshots"] = filtered

                # Finalise and schedule flush once the 5-min window has passed
                if current_timestamp >= buy_event["window_end_timestamp"]:
                    last_snap = buy_event["btc_price_snapshots"][-1]
                    buy_event["final_btc_change_usd"] = last_snap["btc_change_usd"]
                    buy_event["final_btc_change_pct"] = last_snap["btc_change_pct"]
                    buy_event["total_time_tracked_sec"] = last_snap["time_elapsed_sec"]
                    buy_event["_logged"] = True
                    to_flush.append(buy_event)

        for buy_event in to_flush:
            self._write_buy_event(buy_event)

    def finalize_session(self, session_end_timestamp: float):
        """Write any buys whose 5-min window did not finish before session end."""
        to_flush = []

        with self._lock:
            for buy_event in self.buy_events:
                if buy_event["_logged"]:
                    continue
                if buy_event["btc_price_snapshots"]:
                    buy_event["session_end_timestamp"] = session_end_timestamp
                    last_snap = buy_event["btc_price_snapshots"][-1]
                    buy_event["final_btc_change_usd"] = last_snap["btc_change_usd"]
                    buy_event["final_btc_change_pct"] = last_snap["btc_change_pct"]
                    buy_event["total_time_tracked_sec"] = last_snap["time_elapsed_sec"]
                    buy_event["_logged"] = True
                    to_flush.append(buy_event)

        if to_flush:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(self.log_file, 'a') as f:
                    for buy_event in to_flush:
                        f.write(json.dumps(buy_event) + '\n')
                logger.info(
                    f"BTC price movements (session-end flush) logged to {self.log_file} "
                    f"({len(to_flush)} buys)"
                )
            except Exception as e:
                logger.error(f"Failed to write BTC price movements: {e}")
        else:
            logger.info(f"BTC price movements: all {len(self.buy_events)} buys already logged within their 5-min windows")
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics across all buys."""
        with self._lock:
            if not self.buy_events:
                return {}
            
            btc_changes = []
            btc_changes_pct = []
            
            for buy_event in self.buy_events:
                if buy_event["btc_price_snapshots"]:
                    last_snap = buy_event["btc_price_snapshots"][-1]
                    btc_changes.append(last_snap["btc_change_usd"])
                    btc_changes_pct.append(last_snap["btc_change_pct"])
            
            if not btc_changes:
                return {}
            
            return {
                "total_buys": len(self.buy_events),
                "avg_btc_change_usd": round(sum(btc_changes) / len(btc_changes), 2),
                "avg_btc_change_pct": round(sum(btc_changes_pct) / len(btc_changes_pct), 4),
                "max_btc_change_usd": round(max(btc_changes), 2),
                "min_btc_change_usd": round(min(btc_changes), 2),
                "buys_with_positive_movement": sum(1 for x in btc_changes if x > 0),
                "buys_with_negative_movement": sum(1 for x in btc_changes if x < 0),
            }


class MarketMicrostructureLogger:
    """Writes periodic market microstructure snapshots into data/ as JSONL."""

    def __init__(self, log_file: str = "data/market_microstructure.jsonl", interval_sec: float = 5.0):
        self.log_file = resolve_runtime_path(log_file, json_only=True)
        self.interval_sec = max(1.0, float(interval_sec))
        self._next_emit_ts = 0.0
        self._lock = threading.Lock()

    def maybe_log(self, snapshot: Dict[str, Any], now: Optional[float] = None) -> None:
        now_ts = float(now if now is not None else time.time())
        if not isinstance(snapshot, dict):
            return

        with self._lock:
            if now_ts < self._next_emit_ts:
                return
            self._next_emit_ts = now_ts + self.interval_sec

        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.log_file, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(snapshot, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"Failed to write market microstructure snapshot: {e}")


# =============================================================================
# BINANCE BTC PRICE CLIENT (parallel feed for price comparison)
# =============================================================================

class BinancePriceClient:
    """
    Parallel BTC price stream from Binance for price comparison.
    Runs independently to get real-time Binance price alongside Chainlink.
    """
    
    def __init__(self, state: 'MarketState'):
        self.state = state
        self.running = False
        self._ws = None
        self._ping_task: Optional[asyncio.Task] = None
        self._feed_url = BINANCE_BTC_WSS_URL
    
    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return (symbol or "").strip().lower().replace("/", "").replace("-", "").replace("_", "")

    @classmethod
    def _is_btc_symbol(cls, symbol: str) -> bool:
        s = cls._normalize_symbol(symbol)
        return s in {"btcusd", "btcusdt"}
    
    DATA_TIMEOUT = 30  # seconds without any message → force reconnect

    async def connect(self):
        """Connect to Binance BTC price stream."""
        self.running = True
        self._last_msg_time = time.time()
        
        while self.running:
            try:
                async with websockets.connect(self._feed_url) as ws:
                    self._ws = ws
                    self.state.binance_connected = True
                    self._last_msg_time = time.time()
                    logger.info(f"Binance BTC feed connected: {self._feed_url}")
                    
                    # Start ping task and watchdog
                    self._ping_task = asyncio.create_task(self._ping_loop(ws))
                    watchdog_task = asyncio.create_task(self._watchdog(ws))
                    
                    try:
                        async for message in ws:
                            if not self.running:
                                break
                            self._last_msg_time = time.time()
                            self._handle_message(message)
                    finally:
                        watchdog_task.cancel()
                        try:
                            await watchdog_task
                        except asyncio.CancelledError:
                            pass
                    
                    self._ws = None
                    
            except websockets.ConnectionClosed:
                self._ws = None
                self.state.binance_connected = False
                if self.running:
                    logger.warning("Binance BTC feed disconnected, reconnecting in 2s...")
                    await asyncio.sleep(2)
            except Exception as e:
                self._ws = None
                self.state.binance_connected = False
                if self.running:
                    logger.warning(f"Binance BTC feed error: {e}, reconnecting in 5s...")
                    await asyncio.sleep(5)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except:
                        pass
                    self._ping_task = None

    async def _watchdog(self, ws):
        """Force-close WebSocket if no messages received for DATA_TIMEOUT seconds."""
        try:
            while self.running:
                await asyncio.sleep(5)
                silence = time.time() - self._last_msg_time
                if silence > self.DATA_TIMEOUT:
                    logger.warning(
                        f"Binance watchdog: no data for {silence:.0f}s, forcing reconnect"
                    )
                    self.state.binance_connected = False
                    await ws.close()
                    break
        except asyncio.CancelledError:
            pass

    def _handle_message(self, message: str):
        """Parse Binance trade/bookTicker message."""
        try:
            if not isinstance(message, str) or not message.strip():
                return
            
            data = json.loads(message)
            
            # Handle both trade stream and bookTicker stream
            symbol = str(data.get("s", "") or data.get("symbol", ""))
            if not self._is_btc_symbol(symbol):
                return
            
            # Trade stream: {"e":"trade","s":"BTCUSDT","p":"...","T":...}
            if "p" in data:
                price = float(data.get("p", 0))
            # BookTicker stream: {"s":"BTCUSDT","b":"...","a":"...","E":...}
            else:
                bid = float(data.get("b", 0) or 0)
                ask = float(data.get("a", 0) or 0)
                if bid > 0 and ask > 0:
                    price = (bid + ask) / 2.0
                else:
                    price = bid or ask
            
            if price <= 0:
                return
            
            ts_ms = data.get("T") or data.get("E") or 0
            
            # Update Binance price
            self.state.binance_current_price = price
            self.state.binance_last_update = time.time()
            
            logger.debug(f"Binance BTC: ${price:,.2f}")
            
        except (json.JSONDecodeError, ValueError, KeyError, TypeError):
            pass

    async def _ping_loop(self, ws):
        """Send ping every 5 seconds to keep connection alive."""
        try:
            while self.running:
                await asyncio.sleep(5)
                try:
                    await ws.ping()
                except Exception:
                    break
        except asyncio.CancelledError:
            pass

    async def disconnect(self):
        """Gracefully close Binance price WebSocket connection."""
        self.running = False
        
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except:
                pass
            self._ping_task = None
        
        if self._ws:
            try:
                await self._ws.close(code=1000, reason="Normal shutdown")
                logger.info("Binance BTC feed closed gracefully")
            except Exception as e:
                logger.warning(f"Binance BTC feed close error: {e}")
            finally:
                self._ws = None
        
        self.state.binance_connected = False


# =============================================================================
# DASHBOARD
# =============================================================================

class Dashboard:
    def __init__(self, state: MarketState, stats: TradingStats, config: Any):
        self.state = state
        self.stats = stats
        self.config = config
        self.calc = IndicatorCalculator()
        self._signals_csv_path = Path("logs") / "signals.csv"
        self._bot_log_path = Path("logs") / "bot.log"
        resolved_file = Path(__file__).resolve()
        root_dir = resolved_file.parents[2] if len(resolved_file.parents) > 2 else Path.cwd()
        self._latest5_csv_path = root_dir / "logs" / "latest_5_windows.csv"
        self._bootstrap_btc_window_moves(periods=5)
        self._write_latest_5_windows_from_history(periods=5)
        self._display_latest_5_windows()
        
        win_rate_path = Path(__file__).parent / config.strategy.win_rate_csv
        self.winrate_table = WinRateTable(str(win_rate_path))
        
        self.last_signal = ""
        self.manual_signal_pending = ""
        self.manual_sell_pending = ""
        self.manual_buy_live_status = "idle"
        self.entry_flash = False
        self.hedge_flash = False
        self.btc_volume_feed: Optional[BTCVolumeFeed] = None
        self._btc_roll_cache: Dict[str, Optional[float]] = {
            "UP": None,
            "DOWN": None,
        }
        self._indicator_controls: Dict[str, bool] = {
            "momentum": True,
            "vwap_deviation": True,
            "zscore": True,
        }
        self._zscore_abs_min: float = 0.25

    def _get_btc_roll_key(self, label: str) -> str:
        text = (label or "").upper()
        return "UP" if text.startswith("UP") else "DOWN"

    def _get_btc_rolling_indicators(
        self,
        label: str,
        vwap_window: float,
    ) -> Optional[float]:
        """
        Keep BTC vol ratio rolling across minute boundaries.
        Binance 1m klines are timestamped at minute open, so very short windows
        (e.g. 30s) often return empty results between closes.
        """
        key = self._get_btc_roll_key(label)
        cached = self._btc_roll_cache.get(key)

        if self.btc_volume_feed and self.btc_volume_feed.is_connected:
            btc_window = max(int(vwap_window), 120)
            vol_ratio = self.btc_volume_feed.get_volume_ratio(window_seconds=btc_window)

            if vol_ratio is not None:
                cached = vol_ratio
                self._btc_roll_cache[key] = cached

        return cached

    def get_indicator_controls(self) -> Dict[str, bool]:
        return {
            "momentum": bool(self._indicator_controls.get("momentum", True)),
            "vwap_deviation": bool(self._indicator_controls.get("vwap_deviation", True)),
            "zscore": bool(self._indicator_controls.get("zscore", True)),
        }

    def update_indicator_controls(self, payload: Dict[str, Any]) -> Dict[str, bool]:
        if isinstance(payload, dict):
            if "momentum" in payload:
                self._indicator_controls["momentum"] = bool(payload.get("momentum"))
            if "vwap_deviation" in payload:
                self._indicator_controls["vwap_deviation"] = bool(payload.get("vwap_deviation"))
            if "zscore" in payload:
                self._indicator_controls["zscore"] = bool(payload.get("zscore"))
        return self.get_indicator_controls()

    def _window_indicator_status(self, token: Optional[TokenData], window_sec: int) -> Dict[str, Any]:
        if not token or not token.trades or token.last_price <= 0:
            return {
                "window_sec": int(window_sec),
                "momentum_pct": None,
                "deviation_pct": None,
                "zscore": None,
                "momentum_ok": False,
                "vwap_deviation_ok": False,
                "zscore_ok": False,
                "all_ok": False,
            }

        trades_in_window = self.calc.get_trades_in_window(token.trades, float(window_sec))
        vwap = self.calc.calc_vwap(trades_in_window)
        deviation = self.calc.calc_deviation(token.last_price, vwap)
        momentum = self.calc.calc_momentum(token.trades, token.last_price, window=float(window_sec))
        zscore = self.calc.calc_zscore(token.trades, token.last_price, window=float(window_sec))

        min_dev = self.config.strategy.min_deviation_pct
        max_dev = self.config.strategy.max_deviation_pct
        min_mom = float(getattr(self.config.strategy, "momentum_min_pct", 0.0) or 0.0)

        mom_ok = momentum is not None and momentum > min_mom
        dev_ok = deviation > min_dev and deviation < max_dev
        z_ok = zscore is not None and abs(zscore) >= self._zscore_abs_min

        controls = self.get_indicator_controls()
        all_ok = True
        if controls["momentum"]:
            all_ok = all_ok and mom_ok
        if controls["vwap_deviation"]:
            all_ok = all_ok and dev_ok
        if controls["zscore"]:
            all_ok = all_ok and z_ok

        return {
            "window_sec": int(window_sec),
            "momentum_pct": momentum,
            "deviation_pct": deviation,
            "zscore": zscore,
            "momentum_ok": bool(mom_ok),
            "vwap_deviation_ok": bool(dev_ok),
            "zscore_ok": bool(z_ok),
            "all_ok": bool(all_ok),
        }

    def _favorite_window_checks(self) -> Dict[str, Dict[str, Any]]:
        up = self.state.up_token
        down = self.state.down_token
        if not up and not down:
            token = None
        elif not down:
            token = up
        elif not up:
            token = down
        else:
            token = up if up.last_price >= down.last_price else down

        return {
            "s5": self._window_indicator_status(token, 5),
            "s15": self._window_indicator_status(token, 15),
        }

    def _display_latest_5_windows(self) -> None:
        """Load and log the latest 5 completed windows from CSV."""
        try:
            if not self._latest5_csv_path.exists():
                return
            
            with open(self._latest5_csv_path, "r", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)
            
            if not rows:
                logger.info("No completed windows data available yet")
                return
            
            logger.info(f"Latest 5 windows ({len(rows)} rows):")
            for i, row in enumerate(rows, 1):
                ts = int(row.get("window_ts", 0))
                start = row.get("window_start_local", "")
                end = row.get("window_end_local", "")
                usd = float(row.get("abs_usd_move", 0.0))
                pct = float(row.get("abs_pct_move", 0.0))
                source = row.get("source", "")
                logger.info(f"  [{i}] {start} -> {end} | ${usd:.2f} ({pct:.4f}%) | {source}")
        except Exception as e:
            logger.warning(f"Failed to display latest 5 windows: {e}")

    def _write_latest_5_windows_from_history(self, periods: int = 5) -> None:
        """Load and export latest N completed trades from trading_log.json (past events only)."""
        try:
            trading_log = self.stats.log_file
            if not trading_log.exists():
                logger.info("No trading history to export for latest_5_windows")
                return
            
            with open(trading_log, "r") as handle:
                data = json.load(handle)
                trades = data.get("trades", [])
            
            # Filter: only trades with valid BTC anchor and entry prices
            valid_trades = []
            for t in trades:
                slug = (t.get("market_slug") or "").strip()
                parts = slug.split("-")
                if len(parts) < 4 or parts[0] != "btc" or parts[1] != "updown":
                    continue
                
                btc_anchor = float(t.get("btc_anchor_price_at_entry") or 0.0)
                btc_entry = float(t.get("btc_price_at_entry") or 0.0)
                if btc_anchor <= 0.0 or btc_entry <= 0.0:
                    continue
                
                try:
                    window_ts = int(parts[-1])
                except ValueError:
                    continue
                
                abs_usd = abs(btc_entry - btc_anchor)
                abs_pct = abs((btc_entry - btc_anchor) / btc_anchor * 100)
                signed_usd = btc_entry - btc_anchor
                signed_pct = (signed_usd / btc_anchor * 100)
                
                valid_trades.append({
                    "window_ts": window_ts,
                    "window": window_ts,
                    "signed_usd": signed_usd,
                    "signed_pct": signed_pct,
                    "abs_usd": abs_usd,
                    "abs_pct": abs_pct,
                    "source": "history",
                })
            
            if not valid_trades:
                logger.info("No historical trades with BTC movement to export")
                return
            
            # Sort by window_ts and take latest N
            latest_trades = sorted(valid_trades, key=lambda x: x["window_ts"])[-periods:]
            
            # Write to CSV
            self._latest5_csv_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._latest5_csv_path, "w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    "window_ts",
                    "window_start_local",
                    "window_end_local",
                    "abs_usd_move",
                    "abs_pct_move",
                    "source",
                ])
                for row in latest_trades:
                    window_ts = row["window_ts"]
                    start_dt = datetime.fromtimestamp(window_ts)
                    end_dt = start_dt + timedelta(seconds=self.config.market.duration_sec)
                    writer.writerow([
                        window_ts,
                        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        f"{row['abs_usd']:.6f}",
                        f"{row['abs_pct']:.6f}",
                        row["source"],
                    ])
            
            logger.info(f"Latest 5 windows from history: exported {len(latest_trades)} trades to CSV")
        except Exception as e:
            logger.warning(f"Failed to export latest 5 from history: {e}")

    def _is_completed_window(self, window_ts: int) -> bool:
        """A window is complete only after its configured duration has elapsed."""
        return (window_ts + int(self.config.market.duration_sec)) <= int(time.time())

    def _bootstrap_btc_window_moves(self, periods: int = 5) -> None:
        """Seed live rolling buffer from latest windows at startup."""
        if self.state.btc_window_moves:
            return

        try:
            window_map: Dict[int, Dict[str, float]] = {}

            # 1) Prefer signals.csv if available in current runtime folder.
            if self._signals_csv_path.exists():
                with open(self._signals_csv_path, "r", newline="") as handle:
                    reader = csv.DictReader(handle)
                    by_window: Dict[str, Dict[str, str]] = {}
                    for row in reader:
                        window_ts = (row.get("window_ts") or "").strip()
                        if not window_ts:
                            continue
                        opening_price = (row.get("opening_price") or "").strip()
                        btc_price = (row.get("btc_price") or "").strip()
                        btc_delta_pct = (row.get("btc_delta_pct") or "").strip()
                        if not opening_price or not btc_price or not btc_delta_pct:
                            continue
                        by_window[window_ts] = row

                completed_rows = [
                    row for row in by_window.values()
                    if self._is_completed_window(int(row["window_ts"]))
                ]
                latest_rows = sorted(completed_rows, key=lambda row: int(row["window_ts"]))[-periods:]
                for row in latest_rows:
                    opening_price = float(row["opening_price"])
                    btc_price = float(row["btc_price"])
                    window_ts = int(row["window_ts"])
                    signed_usd = btc_price - opening_price
                    signed_pct = float(row["btc_delta_pct"])
                    window_map[window_ts] = {
                        "window": int(row["window_ts"]),
                        "signed_usd": signed_usd,
                        "signed_pct": signed_pct,
                        "abs_usd": abs(signed_usd),
                        "abs_pct": abs(signed_pct),
                        "min_usd": min(0.0, signed_usd),
                        "max_usd": max(0.0, signed_usd),
                        "source": "signals_csv",
                    }

            # 2) Fallback to latest trade windows from current stats log.
            if len(window_map) < periods and self.stats and self.stats.trades:
                for t in reversed(self.stats.trades):
                    slug = (t.market_slug or "").strip()
                    parts = slug.split("-")
                    if len(parts) < 4 or parts[0] != "btc" or parts[1] != "updown":
                        continue
                    try:
                        window_ts = int(parts[-1])
                    except ValueError:
                        continue
                    if not self._is_completed_window(window_ts):
                        continue

                    btc_anchor = float(getattr(t, "btc_anchor_price_at_entry", 0.0) or 0.0)
                    btc_entry = float(getattr(t, "btc_price_at_entry", 0.0) or 0.0)
                    if btc_anchor <= 0 or btc_entry <= 0:
                        continue

                    abs_usd = abs(btc_entry - btc_anchor)
                    abs_pct = abs((btc_entry - btc_anchor) / btc_anchor * 100)
                    signed_usd = btc_entry - btc_anchor
                    signed_pct = (signed_usd / btc_anchor * 100)
                    window_map[window_ts] = {
                        "window": window_ts,
                        "signed_usd": signed_usd,
                        "signed_pct": signed_pct,
                        "abs_usd": abs_usd,
                        "abs_pct": abs_pct,
                        "min_usd": min(0.0, signed_usd),
                        "max_usd": max(0.0, signed_usd),
                        "source": "trade_log",
                    }

            # 3) If still short, fill missing latest windows from bot.log market slugs.
            if len(window_map) < periods and self._bot_log_path.exists():
                slug_re = re.compile(r"market=btc-updown-\d+m-(\d+)")
                found_ts: List[int] = []
                with open(self._bot_log_path, "r", encoding="utf-8", errors="ignore") as handle:
                    for line in handle:
                        m = slug_re.search(line)
                        if m:
                            found_ts.append(int(m.group(1)))

                completed_bot_log_ts = [ts for ts in sorted(set(found_ts)) if self._is_completed_window(ts)]
                for window_ts in completed_bot_log_ts[-periods:]:
                    if window_ts not in window_map:
                        window_map[window_ts] = {
                            "window": window_ts,
                            "signed_usd": 0.0,
                            "signed_pct": 0.0,
                            "abs_usd": 0.0,
                            "abs_pct": 0.0,
                            "min_usd": 0.0,
                            "max_usd": 0.0,
                            "source": "bot_log",
                        }

            selected = [window_map[k] for k in sorted(window_map.keys())[-periods:]]
            for row in selected:
                self.state.btc_window_moves.append(row)

            if selected:
                logger.info(f"BTC buffer bootstrap: loaded {len(selected)} recent windows")
            else:
                logger.info("BTC buffer bootstrap: no historical windows found")
        except Exception as e:
            logger.warning(f"BTC buffer bootstrap failed: {e}")

    def _get_recent_btc_buffer(self, periods: int = 5) -> Optional[Dict[str, float]]:
        """Average absolute BTC move over the last N completed windows (live rolling)."""
        # Newest -> oldest weights for the last 5 windows.
        recency_weights = [1.2, 1.0, 1.0, 0.8, 0.8]

        def _weighted_stats(rows_newest_first: List[Dict[str, Any]]) -> Dict[str, float]:
            count = len(rows_newest_first)
            if count <= 0:
                return {"periods": 0.0, "avg_abs_pct": 0.0, "avg_abs_usd": 0.0}

            # For fewer than 5 rows, keep the newest-biased prefix.
            weights = recency_weights[:count]
            weight_sum = sum(weights)
            if weight_sum <= 0:
                weights = [1.0] * count
                weight_sum = float(count)

            weighted_abs_pct = 0.0
            weighted_abs_usd = 0.0
            for row, w in zip(rows_newest_first, weights):
                weighted_abs_pct += abs(float(row.get("abs_pct", 0.0))) * w
                weighted_abs_usd += abs(float(row.get("abs_usd", 0.0))) * w

            return {
                "periods": float(count),
                "avg_abs_pct": weighted_abs_pct / weight_sum,
                "avg_abs_usd": weighted_abs_usd / weight_sum,
            }

        live_rows = list(self.state.btc_window_moves)[-periods:]
        if live_rows:
            # Periodically refresh the display of latest windows (every 20 calls)
            if int(time.time()) % 20 < 1:
                self._display_latest_5_windows()
            # live_rows is oldest->newest; reverse so weights apply newest first.
            return _weighted_stats(list(reversed(live_rows)))

        if not self._signals_csv_path.exists():
            return None

        try:
            with open(self._signals_csv_path, "r", newline="") as handle:
                reader = csv.DictReader(handle)
                by_window: Dict[str, Dict[str, str]] = {}
                for row in reader:
                    window_ts = (row.get("window_ts") or "").strip()
                    if not window_ts:
                        continue

                    opening_price = (row.get("opening_price") or "").strip()
                    btc_price = (row.get("btc_price") or "").strip()
                    btc_delta_pct = (row.get("btc_delta_pct") or "").strip()
                    if not opening_price or not btc_price or not btc_delta_pct:
                        continue

                    by_window[window_ts] = row

            if not by_window:
                return None

            recent_rows = sorted(by_window.values(), key=lambda row: int(row["window_ts"]), reverse=True)[:periods]
            if not recent_rows:
                return None

            normalized_rows: List[Dict[str, float]] = []
            for row in recent_rows:
                opening_price = float(row["opening_price"])
                btc_price = float(row["btc_price"])
                normalized_rows.append({
                    "abs_pct": abs(float(row["btc_delta_pct"])),
                    "abs_usd": abs(btc_price - opening_price),
                })

            # recent_rows is already newest->oldest.
            return _weighted_stats(normalized_rows)
        except Exception:
            return None

    def _format_recent_btc_buffer(self, periods: int = 5) -> Optional[str]:
        buffer_stats = self._get_recent_btc_buffer(periods)
        if not buffer_stats:
            return None

        return (
            f"Buffer({int(buffer_stats['periods'])}): +/-${buffer_stats['avg_abs_usd']:,.2f} "
            f"(+/-{buffer_stats['avg_abs_pct']:.3f}%)"
        )

    def _late_mode_keys(self) -> List[str]:
        modes = getattr(self.config.strategy, "late_entry_modes", None)
        if not modes:
            return []

        entries: List[Tuple[str, float]] = []
        for key, value in vars(modes).items():
            if not isinstance(key, str) or not key.startswith("mode_") or not key.endswith("s"):
                continue
            mode_cfg = getattr(modes, key, None)
            if mode_cfg is None:
                continue
            try:
                time_left = float(getattr(mode_cfg, "time_left_sec", 0) or 0)
            except (TypeError, ValueError):
                time_left = 0.0
            entries.append((key, time_left))

        # Evaluate wider windows first; final selection still picks tightest active window.
        entries.sort(key=lambda item: item[1], reverse=True)
        return [item[0] for item in entries]

    def _get_late_entry_mode(self, time_left: float) -> Optional[Dict[str, float]]:
        modes = getattr(self.config.strategy, "late_entry_modes", None)
        if not modes or not modes.enabled:
            return None

        candidates: List[Dict[str, float]] = []
        for mode_name in self._late_mode_keys():
            mode_cfg = getattr(modes, mode_name, None)
            if mode_cfg is None:
                continue
            window_sec = float(max(0, mode_cfg.time_left_sec))
            if time_left <= window_sec and mode_cfg.enabled:
                candidates.append(
                    {
                        "window_sec": float(window_sec),
                        "min_contracts": float(mode_cfg.min_contracts),
                        "max_trades": float(mode_cfg.max_trades),
                        "buffer_avg_multiplier": float(mode_cfg.buffer_avg_multiplier),
                        "min_buffer_threshold_usd": float(getattr(mode_cfg, "min_buffer_threshold_usd", self.config.buffer)),
                        "min_price": float(mode_cfg.min_price),
                        "max_price": float(mode_cfg.max_price),
                        "name": f"mode_{int(window_sec)}s",
                    }
                )

        if not candidates:
            return None

        # Prioritize the tightest active window (20s over 40s over 60s).
        return sorted(candidates, key=lambda item: item["window_sec"])[0]

    def _get_volume_eval_mode(self, time_left: float) -> Optional[Dict[str, float]]:
        mode_cfg = getattr(self.config.strategy, "volume_eval_mode", None)
        if not mode_cfg or not bool(getattr(mode_cfg, "enabled", False)):
            return None

        window_sec = float(max(0, getattr(mode_cfg, "time_left_sec", 0)))
        if time_left > window_sec:
            return None

        return {
            "window_sec": window_sec,
            "min_contracts": float(getattr(mode_cfg, "min_contracts", self.config.entry.min_contracts)),
            "max_trades": float(getattr(mode_cfg, "max_trades", 1)),
            "buffer_avg_multiplier": float(getattr(mode_cfg, "buffer_avg_multiplier", 1.0)),
            "min_buffer_threshold_usd": float(getattr(mode_cfg, "min_buffer_threshold_usd", self.config.buffer)),
            "volume_check_enabled": bool(getattr(mode_cfg, "volume_check_enabled", True)),
            "min_price": float(getattr(mode_cfg, "min_price", self.config.strategy.min_price)),
            "max_price": float(getattr(mode_cfg, "max_price", self.config.strategy.max_price)),
            "name": "volume_eval_mode",
        }

    def _get_active_entry_modes(self, time_left: float) -> List[Dict[str, float]]:
        modes: List[Dict[str, float]] = []
        volume_mode = self._get_volume_eval_mode(time_left)
        if volume_mode:
            modes.append(volume_mode)
        late_mode = self._get_late_entry_mode(time_left)
        if late_mode:
            modes.append(late_mode)
        return modes

    def _get_volume_eval_threshold_levels(self) -> List[float]:
        mode_cfg = getattr(self.config.strategy, "volume_eval_mode", None)
        raw = getattr(mode_cfg, "entry_min_current_volume_diffs", None) if mode_cfg else None
        levels: List[float] = []
        if isinstance(raw, list):
            for item in raw:
                try:
                    val = float(item)
                except (TypeError, ValueError):
                    continue
                if val >= 0.0:
                    levels.append(val)
        if levels:
            return levels

        vac_cfg = getattr(self.config.strategy, "volume_acceleration_check", None)
        fallback = float(getattr(vac_cfg, "min_current_volume_diff", 5000.0) or 5000.0)
        return [max(0.0, fallback)]

    def _get_volume_eval_entry_trade_limits(self) -> List[int]:
        mode_cfg = getattr(self.config.strategy, "volume_eval_mode", None)
        raw = getattr(mode_cfg, "entry_trade_limits", None) if mode_cfg else None
        limits: List[int] = []
        if isinstance(raw, list):
            for item in raw:
                try:
                    val = int(float(item))
                except (TypeError, ValueError):
                    continue
                if val > 0:
                    limits.append(val)

        levels = self._get_volume_eval_threshold_levels()
        level_count = len(levels) if levels else 1
        if not limits:
            return [1 for _ in range(level_count)]

        if len(limits) < level_count:
            limits.extend([limits[-1]] * (level_count - len(limits)))
        elif len(limits) > level_count:
            limits = limits[:level_count]
        return limits

    def _get_volume_eval_next_min_current_diff(self, time_left: Optional[float] = None) -> Optional[float]:
        if time_left is None:
            time_left = max(0, self.state.end_time - time.time())

        if not self._get_volume_eval_mode(time_left):
            return None

        levels = self._get_volume_eval_threshold_levels()
        if not levels:
            return None

        used_count = self.stats.late_mode_trade_count("volume_eval_mode")
        limits = self._get_volume_eval_entry_trade_limits()
        remaining = max(used_count, 0)
        idx = len(levels) - 1
        for i, cap in enumerate(limits):
            if remaining < cap:
                idx = i
                break
            remaining -= cap
        idx = min(max(idx, 0), len(levels) - 1)
        return float(levels[idx])

    def _is_volume_check_enabled_for_active_mode(self, active_mode: Optional[Dict[str, float]]) -> bool:
        """Late-entry modes do not use volume gate; standalone volume_eval_mode can use it."""
        if not active_mode:
            return True
        if str(active_mode.get("name", "")) == "volume_eval_mode":
            return bool(active_mode.get("volume_check_enabled", True))
        return False

    def _mode_btc_gate_status(
        self,
        mode: Optional[Dict[str, float]],
        btc_buffer: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        mode_name = str((mode or {}).get("name", ""))

        current_usd = float((btc_buffer or {}).get("current_abs_usd", 0.0) or 0.0)
        threshold_usd = float((btc_buffer or {}).get("buffer_abs_usd", 0.0) or 0.0)
        ok_default = bool(btc_buffer and btc_buffer.get("ok"))

        if mode and btc_buffer:
            by_name = btc_buffer.get("mode_thresholds_by_name")
            if isinstance(by_name, dict):
                row = by_name.get(mode_name)
                if isinstance(row, dict):
                    try:
                        threshold_usd = float(row.get("threshold_usd", threshold_usd) or threshold_usd)
                    except (TypeError, ValueError):
                        pass
                    return {
                        "ok": bool(row.get("ok", ok_default)),
                        "metric": "btc_buffer",
                        "current_usd": current_usd,
                        "threshold_usd": threshold_usd,
                    }

        return {
            "ok": bool(ok_default),
            "metric": "btc_buffer",
            "current_usd": current_usd,
            "threshold_usd": threshold_usd,
        }

    def _get_btc_buffer_status(self) -> Optional[Dict[str, float]]:
        buffer_stats = self._get_recent_btc_buffer()
        if not buffer_stats:
            return None

        if self.state.btc_current_price <= 0 or self.state.btc_anchor_price <= 0:
            return None

        current_abs_usd = abs(self.state.btc_current_price - self.state.btc_anchor_price)
        current_abs_pct = abs((self.state.btc_current_price - self.state.btc_anchor_price) / self.state.btc_anchor_price * 100)

        stats_abs_usd = buffer_stats["avg_abs_usd"]
        stats_abs_pct = buffer_stats["avg_abs_pct"]

        # Base threshold from static buffer and adaptive window stats.
        base_buffer_abs_usd = max(self.config.buffer, stats_abs_usd)

        buffer_abs_usd = base_buffer_abs_usd
        time_left = max(0, self.state.end_time - time.time())
        active_modes = self._get_active_entry_modes(time_left)
        late_mode = None
        mode_threshold_rows: List[Dict[str, Any]] = []
        mode_thresholds_by_name: Dict[str, Dict[str, Any]] = {}
        if active_modes:
            mode_thresholds: List[Tuple[float, Dict[str, float]]] = []
            for mode in active_modes:
                mode_name = str(mode.get("name", "")).strip() or "mode"
                mode_floor_usd = float(mode.get("min_buffer_threshold_usd", self.config.buffer))
                threshold_usd = max(mode_floor_usd, stats_abs_usd * float(mode.get("buffer_avg_multiplier", 1.0)))
                mode_ok = current_abs_usd >= threshold_usd
                mode_row = {
                    "name": mode_name,
                    "window_sec": float(mode.get("window_sec", 0.0) or 0.0),
                    "threshold_usd": float(threshold_usd),
                    "ok": bool(mode_ok),
                }
                mode_threshold_rows.append(mode_row)
                mode_thresholds_by_name[mode_name] = mode_row
                mode_thresholds.append((threshold_usd, mode))
            if mode_thresholds:
                mode_thresholds.sort(key=lambda item: item[0])
                late_mode = mode_thresholds[0][1]
        elif self.config.strategy.dangerous and time_left <= 20:
            # Legacy fallback when structured late-entry modes are disabled.
            buffer_abs_usd = max(20.0, stats_abs_usd * 0.5)
        
        return {
            "current_abs_usd": current_abs_usd,
            "current_abs_pct": current_abs_pct,
            "buffer_abs_usd": buffer_abs_usd,
            "base_buffer_abs_usd": base_buffer_abs_usd,
            "buffer_abs_pct": stats_abs_pct,
            "buffer_metric": "average",
            "ok": current_abs_usd >= buffer_abs_usd,
            "mode_thresholds": mode_threshold_rows,
            "mode_thresholds_by_name": mode_thresholds_by_name,
            "late_mode_name": late_mode["name"] if late_mode else "",
            "late_mode_window_sec": late_mode["window_sec"] if late_mode else 0.0,
            "late_mode_min_contracts": float(late_mode.get("min_contracts", self.config.entry.min_contracts)) if late_mode else 0.0,
            "late_mode_buffer_multiplier": late_mode["buffer_avg_multiplier"] if late_mode else 0.0,
            "late_mode_min_buffer_threshold_usd": late_mode.get("min_buffer_threshold_usd", 0.0) if late_mode else 0.0,
        }
    
    def _fmt_price(self, price: float) -> str:
        if price >= 0.6:
            return f"[green]{price:.3f}[/green]"
        elif price <= 0.4:
            return f"[red]{price:.3f}[/red]"
        return f"[yellow]{price:.3f}[/yellow]"
    
    def _fmt_dev(self, dev: float) -> str:
        if dev > 5:
            return f"[bold green]+{dev:.1f}%[/bold green]"
        elif dev > 0:
            return f"[green]+{dev:.1f}%[/green]"
        elif dev < -5:
            return f"[bold red]{dev:.1f}%[/bold red]"
        elif dev < 0:
            return f"[red]{dev:.1f}%[/red]"
        return f"{dev:+.1f}%"
    
    def _fmt_zscore(self, z: float) -> str:
        if z > 2:
            return f"[bold magenta]+{z:.2f}[/bold magenta] ⚡"
        elif z > 1:
            return f"[magenta]+{z:.2f}[/magenta]"
        elif z < -2:
            return f"[bold cyan]{z:.2f}[/bold cyan] ⚡"
        elif z < -1:
            return f"[cyan]{z:.2f}[/cyan]"
        return f"{z:+.2f}"
    
    def create_header(self) -> Panel:
        now = time.time()
        time_left = max(0, self.state.end_time - now)
        minutes = int(time_left // 60)
        seconds = int(time_left % 60)
        
        if time_left < 60:
            timer = f"[bold red]⏱️ {seconds}s[/bold red]"
        elif time_left < 180:
            timer = f"[yellow]⏱️ {minutes}:{seconds:02d}[/yellow]"
        else:
            timer = f"[green]⏱️ {minutes}:{seconds:02d}[/green]"
        
        status = "[green]● LIVE[/green]" if self.state.connected else "[red]○ DISCONNECTED[/red]"
        if getattr(self.config, "simulation", None) and self.config.simulation.enabled:
            mode = "[bold yellow]SIMULATION (no real orders)[/bold yellow]"
        else:
            mode = "[bold cyan]REAL TRADING[/bold cyan]"
        
        header = f"{timer}  |  {self.state.slug}  |  {status}  |  {mode}"
        im = self.config.market.interval_minutes
        return Panel(header, title=f"[bold]BTC {im}-Min Live Bot[/bold]")
    
    def create_token_panel(self, token: TokenData, label: str) -> Panel:
        if not token:
            return Panel("No data", title=label)
        
        lines = []
        if token.best_ask > 0:
            lines.append(f"[red]ASK  {token.best_ask:.3f}[/red] | {token.best_ask_size:.0f}")
        else:
            lines.append(f"[red]ASK  ---[/red]")
        
        lines.append("─" * 20)
        lines.append(f"[bold white]LAST {token.last_price:.3f}[/bold white]")
        
        if token.best_ask > 0 and token.best_bid > 0:
            spread = token.best_ask - token.best_bid
            lines.append(f"[dim]Spread: {spread:.3f}[/dim]")
        
        lines.append("─" * 20)
        
        if token.best_bid > 0:
            lines.append(f"[green]BID  {token.best_bid:.3f}[/green] | {token.best_bid_size:.0f}")
        else:
            lines.append(f"[green]BID  ---[/green]")
        
        return Panel(
            "\n".join(lines),
            title=f"[bold]{label}[/bold] - {self._fmt_price(token.last_price)}",
            border_style="green" if "Up" in label else "red"
        )
    
    def _fmt_momentum(self, m: Optional[float]) -> str:
        if m is None:
            return "[dim]N/A[/dim]"
        if m > 0:
            return f"[green]+{m:.2f}%[/green]"
        elif m < 0:
            return f"[red]{m:.2f}%[/red]"
        return f"[cyan]0.00%[/cyan]"

    def _sum_volume_in_window(self, trades: deque, start_ts: float, end_ts: float, basis: str = "total") -> float:
        if not trades:
            return 0.0
        normalized_basis = str(basis or "total").strip().lower()
        if normalized_basis == "buy":
            return float(
                sum(
                    float(t.size)
                    for t in trades
                    if start_ts <= float(t.timestamp) < end_ts and str(getattr(t, "side", "")).upper() == "BUY"
                )
            )
        return float(sum(float(t.size) for t in trades if start_ts <= float(t.timestamp) < end_ts))

    def _get_favorite_volume_speed_check(
        self,
        fav_name: str,
        up: TokenData,
        down: TokenData,
        window_sec: Optional[float] = None,
        min_current_volume_diff_override: Optional[float] = None,
        volume_basis_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compare volume acceleration between favored and opposite side."""
        vac_cfg = getattr(self.config.strategy, "volume_acceleration_check", None)
        enabled = bool(getattr(vac_cfg, "enabled", True))
        if window_sec is None:
            window_sec = float(getattr(vac_cfg, "window_sec", 10.0) or 10.0)
        require_curr_lead = bool(getattr(vac_cfg, "require_current_volume_lead", True))
        min_accel_diff = float(getattr(vac_cfg, "min_accel_diff", 0.0) or 0.0)
        threshold = float(getattr(vac_cfg, "threshold", 5000.0) or 5000.0)
        if volume_basis_override is None:
            volume_basis = str(getattr(vac_cfg, "volume_basis", "total") or "total").strip().lower()
        else:
            volume_basis = str(volume_basis_override or "total").strip().lower()
        if volume_basis not in {"total", "buy"}:
            volume_basis = "total"
        if min_current_volume_diff_override is None:
            min_curr_diff = float(getattr(vac_cfg, "min_current_volume_diff", threshold) or threshold)
        else:
            min_curr_diff = float(min_current_volume_diff_override)

        now = time.time()
        curr_start = now - float(window_sec)
        prev_start = now - (2.0 * float(window_sec))

        fav_token = up if fav_name == "UP" else down
        other_token = down if fav_name == "UP" else up

        fav_curr = self._sum_volume_in_window(fav_token.trades, curr_start, now, basis=volume_basis)
        fav_prev = self._sum_volume_in_window(fav_token.trades, prev_start, curr_start, basis=volume_basis)
        other_curr = self._sum_volume_in_window(other_token.trades, curr_start, now, basis=volume_basis)
        other_prev = self._sum_volume_in_window(other_token.trades, prev_start, curr_start, basis=volume_basis)

        fav_accel = fav_curr - fav_prev
        other_accel = other_curr - other_prev
        has_data = (fav_curr + fav_prev + other_curr + other_prev) > 0.0

        curr_lead_ok = (fav_curr - other_curr) >= min_curr_diff
        # Acceleration is informational only; do not gate entries on accel lead.
        accel_ok = True
        if require_curr_lead:
            rule_ok = curr_lead_ok and accel_ok
        else:
            rule_ok = accel_ok

        ok = True if not enabled else bool(has_data and rule_ok)

        return {
            "window_sec": float(window_sec),
            "enabled": enabled,
            "volume_basis": volume_basis,
            "fav_name": fav_name,
            "fav_curr": fav_curr,
            "fav_prev": fav_prev,
            "other_curr": other_curr,
            "other_prev": other_prev,
            "fav_accel": fav_accel,
            "other_accel": other_accel,
            "has_data": has_data,
            "require_current_volume_lead": require_curr_lead,
            "min_accel_diff": min_accel_diff,
            "threshold": threshold,
            "min_current_volume_diff": min_curr_diff,
            "ok": ok,
        }
    
    def create_indicators_panel(self, token: TokenData, label: str) -> Panel:
        if not token or not token.trades:
            return Panel("Waiting for data...", title=f"{label} Indicators")
        
        mom_window = self.config.strategy.momentum_window_sec
        
        vwap_window = self.config.strategy.vwap_window_sec
        trades_in_window = self.calc.get_trades_in_window(token.trades, vwap_window)
        vwap = self.calc.calc_vwap(trades_in_window)
        deviation = self.calc.calc_deviation(token.last_price, vwap)
        zscore = self.calc.calc_zscore(token.trades, token.last_price, window=5)
        momentum = self.calc.calc_momentum(token.trades, token.last_price, window=mom_window)
        ema9 = self.calc.calc_ema_9(token.trades, window=vwap_window)
        ema21 = self.calc.calc_ema_21(token.trades, window=vwap_window)
        
        def fmt_vol(v):
            if v >= 1_000_000:
                return f"{v/1_000_000:.1f}M"
            elif v >= 1_000:
                return f"{v/1_000:.1f}K"
            return f"{v:.0f}"
        
        def fmt_ema(e):
            if e is None:
                return "[dim]N/A[/dim]"
            return f"{e:.4f}"
        
        # BTC-volume indicators roll from previous closed window when current one is sparse.
        vol_ratio = self._get_btc_rolling_indicators(label, vwap_window)
        
        lines = [
            f"PM VWAP {vwap_window}s: {vwap:.4f}  [dim](poly vol)[/dim]",
        ]
        
        ratio_str = f"{vol_ratio:+.1f}%" if vol_ratio is not None else "N/A"
        ratio_color = "green" if (vol_ratio or 0) > 0 else "red"
        lines += [
            f"Deviation:   {self._fmt_dev(deviation)}",
            f"Z-Score 5s:  {self._fmt_zscore(zscore)}",
            f"Mom {mom_window}s:   {self._fmt_momentum(momentum)}",
            f"EMA9:        {fmt_ema(ema9)}  EMA21: {fmt_ema(ema21)}",
            f"BTC Vol Bias: [{ratio_color}]{ratio_str}[/{ratio_color}]",
            "",
            f"Trades:      {token.trade_count}",
            f"Volume:      {fmt_vol(token.volume_total)}",
            f"  Buy:  [green]{fmt_vol(token.volume_buy)}[/green]",
            f"  Sell: [red]{fmt_vol(token.volume_sell)}[/red]",
        ]
        
        return Panel("\n".join(lines), title=f"{label} Indicators", border_style="blue")
    
    def create_strategy_panel(self) -> Panel:
        if not self.state.up_token or not self.state.down_token:
            return Panel("Waiting for data...", title="Strategy Signal")
        
        up = self.state.up_token
        down = self.state.down_token
        
        vwap_window = self.config.strategy.vwap_window_sec
        up_vwap = self.calc.calc_vwap(self.calc.get_trades_in_window(up.trades, vwap_window))
        down_vwap = self.calc.calc_vwap(self.calc.get_trades_in_window(down.trades, vwap_window))
        
        up_dev = self.calc.calc_deviation(up.last_price, up_vwap)
        down_dev = self.calc.calc_deviation(down.last_price, down_vwap)
        
        mom_window = self.config.strategy.momentum_window_sec
        up_mom = self.calc.calc_momentum(up.trades, up.last_price, window=mom_window)
        down_mom = self.calc.calc_momentum(down.trades, down.last_price, window=mom_window)
        
        time_left = max(0, self.state.end_time - time.time())
        time_minutes = time_left / 60
        span = self.config.market.interval_minutes
        time_bin = int((span - 1) - time_minutes)
        time_bin = max(0, min(time_bin, span - 1))
        
        if up.last_price > down.last_price:
            fav_name = "UP"
            fav_price = up.last_price
            fav_dev = up_dev
            fav_mom = up_mom
            fav_zscore = self.calc.calc_zscore(up.trades, up.last_price, window=5)
        else:
            fav_name = "DOWN"
            fav_price = down.last_price
            fav_dev = down_dev
            fav_mom = down_mom
            fav_zscore = self.calc.calc_zscore(down.trades, down.last_price, window=5)
        
        base_wr = self.winrate_table.get_winrate(fav_price, time_bin, span)
        wr_str = f"{base_wr:.1f}%" if base_wr else "N/A"
        
        min_elapsed = self.config.strategy.min_elapsed_sec
        min_dev = self.config.strategy.min_deviation_pct
        max_dev = self.config.strategy.max_deviation_pct
        
        no_entry_cutoff = self.config.strategy.no_entry_before_end_sec
        
        elapsed_sec = self.config.market.duration_sec - time_left
        btc_buffer = self._get_btc_buffer_status()
        
        volume_eval_mode = self._get_volume_eval_mode(time_left)
        late_entry_mode = self._get_late_entry_mode(time_left)
        active_modes = [m for m in [volume_eval_mode, late_entry_mode] if m is not None]
        late_mode = active_modes[0] if active_modes else None
        if late_mode:
            min_price = late_mode.get("min_price", 0.0)
            max_price = late_mode.get("max_price", 1.0)
        else:
            min_price = self.config.strategy.min_price
            max_price = self.config.strategy.max_price
        
        price_ok = min_price <= fav_price <= max_price
        time_ok = elapsed_sec >= min_elapsed
        dev_raw_ok = fav_dev > min_dev and fav_dev < max_dev
        mom_min_pct = float(getattr(self.config.strategy, "momentum_min_pct", 0.0) or 0.0)
        mom_raw_ok = fav_mom is not None and fav_mom > mom_min_pct
        z_raw_ok = fav_zscore is not None and abs(fav_zscore) >= self._zscore_abs_min
        controls = self.get_indicator_controls()
        dev_ok = dev_raw_ok if controls["vwap_deviation"] else True
        mom_ok = mom_raw_ok if controls["momentum"] else True
        zscore_ok = z_raw_ok if controls["zscore"] else True
        time_cutoff_ok = time_left > no_entry_cutoff
        btc_buffer_ok = btc_buffer is not None and btc_buffer["ok"]
        window_checks = self._favorite_window_checks()
        
        # Check price trend over last 10 seconds
        up_trend = self.calc.calc_price_trend(up.trades, window=10.0) if up and up.trades else None
        down_trend = self.calc.calc_price_trend(down.trades, window=10.0) if down and down.trades else None
        
        # UP is OK if trending up (or no data), DOWN is OK if trending down (or no data)
        up_trend_ok = up_trend is None or up_trend >= 0
        down_trend_ok = down_trend is None or down_trend <= 0
        fav_trend_ok = (fav_name == "UP" and up_trend_ok) or (fav_name == "DOWN" and down_trend_ok)
        next_min_curr_diff = self._get_volume_eval_next_min_current_diff(time_left)
        vol_speed = self._get_favorite_volume_speed_check(
            fav_name,
            up,
            down,
            min_current_volume_diff_override=next_min_curr_diff,
        )
        vol_speed_ok = bool(vol_speed["ok"])
        if active_modes:
            vol_check_enabled = any(self._is_volume_check_enabled_for_active_mode(mode) for mode in active_modes)
        else:
            vol_check_enabled = True
        vol_gate_ok = vol_speed_ok if vol_check_enabled else True
        
        signal = "⏳ WAIT"
        signal_color = "yellow"

        ready_modes: List[Dict[str, float]] = []
        for mode in active_modes:
            mode_price_ok = mode.get("min_price", 0.0) <= fav_price <= mode.get("max_price", 1.0)
            mode_vol_enabled = self._is_volume_check_enabled_for_active_mode(mode)
            mode_vol_ok = vol_speed_ok if mode_vol_enabled else True
            mode_btc_ok = bool(self._mode_btc_gate_status(mode, btc_buffer).get("ok", False))
            if mode_price_ok and mode_btc_ok and mode_vol_ok:
                ready_modes.append(mode)

        mode_status_lines: List[str] = []

        def _build_mode_status_line(mode_label: str, mode: Optional[Dict[str, float]]) -> str:
            if not mode:
                return f"{mode_label}: inactive"
            mode_price_ok = mode.get("min_price", 0.0) <= fav_price <= mode.get("max_price", 1.0)
            mode_vol_enabled = self._is_volume_check_enabled_for_active_mode(mode)
            mode_vol_ok = vol_speed_ok if mode_vol_enabled else True
            mode_btc = self._mode_btc_gate_status(mode, btc_buffer)
            mode_btc_ok = bool(mode_btc.get("ok", False))
            mode_btc_threshold = mode_btc.get("threshold_usd")
            mode_btc_metric = str(mode_btc.get("metric", "btc_buffer"))
            mode_ready = mode_price_ok and mode_btc_ok and mode_vol_ok
            vol_label = ("OK" if mode_vol_ok else "WAIT") if mode_vol_enabled else "OFF"
            btc_label = (
                f"B {'OK' if mode_btc_ok else 'WAIT'}"
                + (f"(≥${float(mode_btc_threshold):,.2f})" if mode_btc_threshold is not None else "")
            )
            return (
                f"{mode_label}: active {int(mode.get('window_sec', 0))}s | "
                f"P {'OK' if mode_price_ok else 'WAIT'} | "
                f"{btc_label} | "
                f"Vol {vol_label} | "
                f"T {'OK' if time_ok else 'WAIT'} | "
                f"D {'OK' if dev_ok else 'WAIT'} | "
                f"M {'OK' if mom_ok else 'WAIT'} | "
                f"Z {'OK' if zscore_ok else 'WAIT'} | "
                f"R {'OK' if fav_trend_ok else 'WAIT'} | "
                f"Cut {'OK' if time_cutoff_ok else 'WAIT'} => {'READY' if mode_ready else 'WAIT'}"
            )

        mode_status_lines.append(_build_mode_status_line("Volume mode", volume_eval_mode))
        mode_status_lines.append(_build_mode_status_line("Late mode", late_entry_mode))

        late_mode = ready_modes[0] if ready_modes else (active_modes[0] if active_modes else None)
        late_mode_btc = self._mode_btc_gate_status(late_mode, btc_buffer)
        late_mode_btc_ok = bool(late_mode_btc.get("ok", False))
        late_mode_btc_threshold = late_mode_btc.get("threshold_usd")
        if late_mode:
            min_price = late_mode.get("min_price", 0.0)
            max_price = late_mode.get("max_price", 1.0)

        late_window_price_only = len(active_modes) > 0
        last_20s_price_only = self.config.strategy.dangerous and time_left <= 20
        price_only_gate = late_window_price_only or last_20s_price_only
        
        if price_only_gate:
            if ready_modes:
                chosen_mode = ready_modes[0]
                chosen_vol_enabled = self._is_volume_check_enabled_for_active_mode(chosen_mode)
                if chosen_mode:
                    mcvd_hint = ""
                    if str(chosen_mode.get("name", "")) == "volume_eval_mode":
                        mcvd_hint = (
                            f", minCurrDiff={vol_speed.get('min_current_volume_diff', 0):.0f}"
                            f", minC={int(chosen_mode.get('min_contracts', self.config.entry.min_contracts))}"
                        )
                    chosen_mode_name = str(chosen_mode.get("name", "")).strip() or "mode"
                    mode_metric = self._mode_btc_gate_status(chosen_mode, btc_buffer)
                    metric_hint = f", x{float(chosen_mode.get('buffer_avg_multiplier', 1.0)):.2f} buffer"
                    signal = (
                        f"✅ BUY {fav_name} ({chosen_mode_name}: "
                        f"last {int(chosen_mode['window_sec'])}s, "
                        f"{metric_hint.lstrip(', ')}, "
                        f"vol {'ON' if chosen_vol_enabled else 'OFF'}{mcvd_hint})"
                    )
                else:
                    signal = f"✅ BUY {fav_name} (last 20s: price + BTC buffer)"
                signal_color = "bold green"
                self.last_signal = f"BUY_{fav_name}"
            elif not late_mode_btc_ok and btc_buffer:
                label = f"last {int(late_mode['window_sec'])}s" if late_mode else "last 20s"
                threshold = late_mode_btc_threshold if late_mode_btc_threshold is not None else btc_buffer['buffer_abs_usd']
                signal = f"⏳ WAIT ({label}: BTC buffer < ${threshold:.2f})"
                self.last_signal = ""
            elif vol_check_enabled and not vol_speed_ok:
                label = f"last {int(late_mode['window_sec'])}s" if late_mode else "last 20s"
                signal = f"⏳ WAIT ({label}: {fav_name} current volume lead too small)"
                self.last_signal = ""
            else:
                label = f"last {int(late_mode['window_sec'])}s" if late_mode else "last 20s"
                signal = f"⏳ WAIT ({label}: P not in range)"
                self.last_signal = ""
        elif not time_cutoff_ok:
            signal = f"🚫 NO ENTRY (< {no_entry_cutoff}s left)"
            signal_color = "red"
            self.last_signal = ""
        elif price_ok and time_ok and dev_ok and mom_ok and zscore_ok and btc_buffer_ok and fav_trend_ok and vol_gate_ok:
            signal = f"✅ BUY {fav_name}"
            signal_color = "bold green"
            self.last_signal = f"BUY_{fav_name}"
        elif fav_price >= 0.70 and time_ok:
            if not fav_trend_ok:
                signal = f"🟡 ALMOST (need {fav_name} trending {'up' if fav_name == 'UP' else 'down'})"
            elif not mom_ok:
                signal = "🟡 ALMOST (need Mom>0%)"
            elif not zscore_ok:
                signal = f"🟡 ALMOST (need |Z|>={self._zscore_abs_min:.2f})"
            elif vol_check_enabled and not vol_speed_ok:
                signal = f"🟡 ALMOST (need {fav_name} current volume lead)"
            elif not btc_buffer_ok:
                if btc_buffer:
                    signal = f"🟡 ALMOST (need BTC buffer >= ${btc_buffer['buffer_abs_usd']:.2f})"
                else:
                    signal = "🟡 ALMOST (need BTC buffer data)"
            elif fav_dev >= max_dev:
                signal = f"🟡 ALMOST (Dev≥{max_dev}%)"
            else:
                signal = "🟡 ALMOST (need dev)"
            self.last_signal = ""
        else:
            self.last_signal = ""
            if not time_ok:
                signal = (
                    f"⏳ WAIT (elapsed<{min_elapsed}s; mode checks below)"
                    if mode_status_lines else f"⏳ WAIT (elapsed<{min_elapsed}s)"
                )
            elif not price_ok:
                signal = f"⏳ WAIT (P not in range)"
            elif not dev_ok:
                if fav_dev >= max_dev:
                    signal = f"⏳ WAIT (Dev≥{max_dev}%)"
                else:
                    signal = f"⏳ WAIT (Dev<{min_dev}%)"
            elif not mom_ok:
                signal = f"⏳ WAIT (Mom≤0%)"
            elif not zscore_ok:
                signal = f"⏳ WAIT (|Z|<{self._zscore_abs_min:.2f})"
            elif vol_check_enabled and not vol_speed_ok:
                signal = f"⏳ WAIT ({fav_name} current volume lead too small)"
            elif not fav_trend_ok:
                signal = f"⏳ WAIT ({fav_name} not trending {'up' if fav_name == 'UP' else 'down'})"
            elif not btc_buffer_ok and btc_buffer:
                signal = f"⏳ WAIT (BTC buffer < ${btc_buffer['buffer_abs_usd']:.2f})"
        
        lines = [
            f"Favorite:    [{signal_color}]{fav_name} ({fav_price:.3f})[/{signal_color}] — WR: [cyan]{wr_str}[/cyan]",
            f"Signal:      [{signal_color}][bold]{signal}[/bold][/{signal_color}]",
            "",
            f"Price:       {self._fmt_price(fav_price)} (range: {min_price}-{max_price})",
            f"Deviation:   {self._fmt_dev(fav_dev)} (need {min_dev}%–{max_dev}%)",
            f"Momentum:    {self._fmt_momentum(fav_mom)}",
            f"Z-Score 5s:  {self._fmt_zscore(fav_zscore)} (need |Z|≥{self._zscore_abs_min:.2f})",
            f"Indicator gates: M={'ON' if controls['momentum'] else 'OFF'} D={'ON' if controls['vwap_deviation'] else 'OFF'} Z={'ON' if controls['zscore'] else 'OFF'}",
            f"5s checks:   M {'OK' if window_checks['s5']['momentum_ok'] else 'WAIT'} | D {'OK' if window_checks['s5']['vwap_deviation_ok'] else 'WAIT'} | Z {'OK' if window_checks['s5']['zscore_ok'] else 'WAIT'} | ALL {'OK' if window_checks['s5']['all_ok'] else 'WAIT'}",
            f"15s checks:  M {'OK' if window_checks['s15']['momentum_ok'] else 'WAIT'} | D {'OK' if window_checks['s15']['vwap_deviation_ok'] else 'WAIT'} | Z {'OK' if window_checks['s15']['zscore_ok'] else 'WAIT'} | ALL {'OK' if window_checks['s15']['all_ok'] else 'WAIT'}",
            f"Vol Speed:   {'OFF' if (not vol_speed.get('enabled', True) or not vol_check_enabled) else ('OK' if vol_speed_ok else 'WAIT')} "
            f"({fav_name} dV{int(vol_speed['window_sec'])}={vol_speed['fav_accel']:+.0f}, other={vol_speed['other_accel']:+.0f})",
            f"Elapsed:     {int(elapsed_sec)}s (need ≥{min_elapsed}s)  [bin {time_bin}]",
        ]
        if btc_buffer:
            lines.insert(
                7,
                f"BTC Buffer:  ${btc_buffer['current_abs_usd']:,.2f} vs ${btc_buffer['buffer_abs_usd']:,.2f} "
                f"({'OK' if btc_buffer_ok else 'WAIT'})"
            )
            mode_rows = btc_buffer.get("mode_thresholds")
            if isinstance(mode_rows, list):
                for row in reversed(mode_rows):
                    if not isinstance(row, dict):
                        continue
                    try:
                        threshold = float(row.get("threshold_usd", 0.0) or 0.0)
                    except (TypeError, ValueError):
                        threshold = 0.0
                    mode_name = str(row.get("name", "mode") or "mode")
                    mode_ok = bool(row.get("ok", False))
                    lines.insert(
                        8,
                        f"BTC {mode_name}: need >= ${threshold:,.2f} ({'OK' if mode_ok else 'WAIT'})"
                    )
        if mode_status_lines:
            for mode_line in reversed(mode_status_lines):
                lines.insert(7, mode_line)
        if late_mode:
            mode_name_for_line = str(late_mode.get("name", ""))
            if mode_name_for_line == "volume_eval_mode":
                chosen_mode_line = (
                    f"Chosen mode: volume_eval_mode ({int(late_mode['window_sec'])}s) | "
                    f"minCurrDiff={vol_speed.get('min_current_volume_diff', 0):.0f} | "
                    f"minC={int(late_mode.get('min_contracts', self.config.entry.min_contracts))} | "
                    f"buffer x{late_mode['buffer_avg_multiplier']:.2f}"
                )
            else:
                chosen_mode_line = (
                    f"Chosen mode: {mode_name_for_line} ({int(late_mode['window_sec'])}s) | "
                    f"minC={int(late_mode['min_contracts'])} | "
                    f"buffer x{late_mode['buffer_avg_multiplier']:.2f}"
                )
            lines.insert(7, chosen_mode_line)
        elif last_20s_price_only:
            lines.insert(7, "Last 20s:   DANGEROUS BTC buffer = max($20.00, 50% of avg buffer)")
        
        title = f"[bold]Strategy: P {min_price}-{max_price}, T≥{min_elapsed}s, Dev {min_dev}%-{max_dev}%[/bold]"
        border = "green" if signal_color == "bold green" else "magenta"
        return Panel("\n".join(lines), title=title, border_style=border)
    
    def create_trading_panel(self) -> Panel:
        s = self.stats
        bet = self.config.entry.bet_amount_usd
        
        wr_str = f"{s.win_rate:.1f}%" if s.trade_count > 0 else "N/A"
        stats_line = f"📊 Markets: {s.markets_seen} | Trades: {s.trade_count} | WR: {wr_str}"
        
        pnl_color = "green" if s.total_pnl >= 0 else "red"
        pnl_line = f"💰 PnL: [{pnl_color}]${s.total_pnl:+.2f}[/{pnl_color}]"
        
        if s.position:
            pos = s.position
            if pos.token_name == "UP" and self.state.up_token:
                current_price = self.state.up_token.best_bid or self.state.up_token.last_price
            elif pos.token_name == "DOWN" and self.state.down_token:
                current_price = self.state.down_token.best_bid or self.state.down_token.last_price
            else:
                current_price = pos.entry_price
            
            unrealized = (pos.contracts * current_price) - (pos.contracts * pos.entry_price)
            ur_color = "green" if unrealized >= 0 else "red"
            
            hedge_str = " [cyan]🛡️ HEDGED[/cyan]" if pos.hedged else ""
            flash = "🔔 " if self.entry_flash else ""
            self.entry_flash = False
            
            pos_line = f"{flash}🟢 LONG {pos.token_name} @ {pos.entry_price:.3f} ({pos.contracts} contracts){hedge_str}"
            ur_line = f"   Unrealized: [{ur_color}]${unrealized:+.2f}[/{ur_color}] (price: {current_price:.3f})"
            
            # Live drawdown
            dd_price = max(0, pos.entry_price - pos.min_price_seen)
            dd_pct = (dd_price / pos.entry_price * 100) if pos.entry_price > 0 else 0
            dd_usd = dd_price * pos.contracts
            if dd_price > 0:
                ur_line += f"\n   Max DD: [red]-${dd_usd:.2f} (-{dd_pct:.1f}%)[/red] (low: {pos.min_price_seen:.3f})"
        else:
            pos_line = "⏳ No position (waiting for signal)"
            ur_line = ""
        
        last_trades_lines = []
        for trade in s.trades[-3:][::-1]:
            icon = "✅" if trade.won else "❌"
            trade_time = datetime.fromtimestamp(trade.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            last_trades_lines.append(f"  {icon} {trade_time} | {trade.token_name} @ {trade.entry_price:.2f} -> ${trade.pnl:+.2f}")
        
        lines = [stats_line, pnl_line, "", pos_line]
        if ur_line:
            lines.append(ur_line)
        if last_trades_lines:
            lines.append("")
            lines.append("Last trades:")
            lines.extend(last_trades_lines)
        
        border = "bold yellow" if self.entry_flash or self.hedge_flash else "cyan"
        self.hedge_flash = False
        return Panel("\n".join(lines), title=f"[bold]💰 REAL Trading (${bet:.0f}/trade)[/bold]", border_style=border)
    
    def create_btc_price_panel(self) -> Panel:
        """Panel showing BTC price and deviation from market start."""
        s = self.state
        feed_label = "Chainlink" if (s.btc_feed_source or "").lower() == "chainlink" else "Binance"
        pair_label = "BTC/USD" if feed_label == "Chainlink" else "BTC/USDT"
        buffer_stats = self._get_recent_btc_buffer()
        buffer_label = self._format_recent_btc_buffer()
        
        if s.btc_current_price <= 0:
            status = "[green]● LIVE[/green]" if s.btc_connected else "[red]○ OFF[/red]"
            buffer_line = ""
            if buffer_label:
                buffer_line = f"\n{buffer_label}"
            return Panel(
                f"{feed_label} {status}\nWaiting for price...{buffer_line}",
                title=f"[bold]₿ {pair_label} ({feed_label})[/bold]",
                border_style="dim"
            )
        
        # Connection status
        status = "[green]●[/green]" if s.btc_connected else "[red]○[/red]"
        
        # Freshness indicator
        age = time.time() - s.btc_last_update if s.btc_last_update > 0 else 999
        if age < 5:
            fresh = "[green]LIVE[/green]"
        elif age < 30:
            fresh = f"[yellow]{int(age)}s ago[/yellow]"
        else:
            fresh = f"[red]{int(age)}s ago[/red]"
        
        lines = [
            f"Price:       [bold white]${s.btc_current_price:,.2f}[/bold white]  {status} {fresh}",
        ]
        
        if s.btc_anchor_price > 0:
            dev_abs = s.btc_current_price - s.btc_anchor_price
            dev_pct = (dev_abs / s.btc_anchor_price) * 100 if s.btc_anchor_price else 0
            
            # Color based on direction
            if dev_abs > 0:
                dev_abs_str = f"[green]+${dev_abs:,.2f}[/green]"
                dev_pct_str = f"[green]+{dev_pct:.3f}%[/green]"
            elif dev_abs < 0:
                dev_abs_str = f"[red]-${abs(dev_abs):,.2f}[/red]"
                dev_pct_str = f"[red]{dev_pct:.3f}%[/red]"
            else:
                dev_abs_str = "$0.00"
                dev_pct_str = "0.000%"
            
            lines.append(f"Anchor:      [dim]${s.btc_anchor_price:,.2f}[/dim]")
            lines.append(f"Deviation:   {dev_abs_str}  ({dev_pct_str})")
        else:
            lines.append("[dim]Anchor: waiting for market start...[/dim]")

        if buffer_label:
            lines.append(f"[cyan]{buffer_label}[/cyan]")
        
        return Panel(
            "\n".join(lines),
            title=f"[bold]₿ {pair_label} ({feed_label})[/bold]",
            border_style="yellow"
        )
    
    def render(self) -> Layout:
        layout = Layout()
        
        layout.split_column(
            Layout(name="body"),
            Layout(name="footer", size=16),
            Layout(self.create_btc_price_panel(), name="btc_price", size=6)
        )
        
        layout["body"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )
        
        layout["left"].split_column(
            Layout(self.create_token_panel(self.state.up_token, "⬆️ UP"), name="up_book"),
            Layout(self.create_indicators_panel(self.state.up_token, "UP"), name="up_ind")
        )
        
        layout["right"].split_column(
            Layout(self.create_token_panel(self.state.down_token, "⬇️ DOWN"), name="down_book"),
            Layout(self.create_indicators_panel(self.state.down_token, "DOWN"), name="down_ind")
        )
        
        layout["footer"].split_row(
            Layout(name="strategy"),
            Layout(name="trading")
        )
        layout["strategy"].update(self.create_strategy_panel())
        layout["trading"].update(self.create_trading_panel())
        
        return layout

    def build_web_snapshot(self) -> dict:
        """Plain dict for the HTTP dashboard (same numbers as terminal panels; no Rich markup)."""
        now = time.time()
        time_left = max(0.0, self.state.end_time - now)
        sim = bool(getattr(self.config, "simulation", None) and self.config.simulation.enabled)
        header = {
            "slug": self.state.slug or "—",
            "time_left_sec": time_left,
            "elapsed_sec": max(0.0, self.config.market.duration_sec - time_left),
            "ws_connected": bool(self.state.connected),
            "simulation": sim,
            "interval_minutes": self.config.market.interval_minutes,
        }

        def token_block(token: Optional[TokenData]) -> Optional[dict]:
            if not token:
                return None
            book = {
                "best_bid": token.best_bid,
                "best_bid_size": token.best_bid_size,
                "best_ask": token.best_ask,
                "best_ask_size": token.best_ask_size,
                "last_price": token.last_price,
                "trade_count": token.trade_count,
                "volume_total": token.volume_total,
                "volume_buy": token.volume_buy,
                "volume_sell": token.volume_sell,
            }
            ind = None
            if token.trades:
                vw = self.config.strategy.vwap_window_sec
                mw = self.config.strategy.momentum_window_sec
                trades_in_window = self.calc.get_trades_in_window(token.trades, vw)
                vwap = self.calc.calc_vwap(trades_in_window)
                ema9 = self.calc.calc_ema_9(token.trades, window=vw)
                ema21 = self.calc.calc_ema_21(token.trades, window=vw)
                btc_vol_ratio = self._get_btc_rolling_indicators(token.name, vw)
                ind = {
                    "vwap_window_sec": vw,
                    "pm_vwap": vwap,
                    "deviation_pct": self.calc.calc_deviation(token.last_price, vwap),
                    "zscore": self.calc.calc_zscore(token.trades, token.last_price, window=5),
                    "momentum_window_sec": mw,
                    "momentum_pct": self.calc.calc_momentum(token.trades, token.last_price, window=mw),
                    "ema9": ema9,
                    "ema21": ema21,
                    "btc_vol_ratio": btc_vol_ratio,
                }
            return {"book": book, "indicators": ind}

        strategy: dict = {
            "signal_text": "Waiting for data...",
            "favorite": None,
            "win_rate_str": None,
            "checks": {},
            "up_line": "",
            "down_line": "",
            "indicator_controls": self.get_indicator_controls(),
            "window_checks": {
                "s5": {
                    "momentum_ok": False,
                    "vwap_deviation_ok": False,
                    "zscore_ok": False,
                    "all_ok": False,
                },
                "s15": {
                    "momentum_ok": False,
                    "vwap_deviation_ok": False,
                    "zscore_ok": False,
                    "all_ok": False,
                },
            },
        }

        if self.state.up_token and self.state.down_token:
            up = self.state.up_token
            down = self.state.down_token
            vwap_window = self.config.strategy.vwap_window_sec
            up_vwap = self.calc.calc_vwap(self.calc.get_trades_in_window(up.trades, vwap_window))
            down_vwap = self.calc.calc_vwap(self.calc.get_trades_in_window(down.trades, vwap_window))
            up_dev = self.calc.calc_deviation(up.last_price, up_vwap)
            down_dev = self.calc.calc_deviation(down.last_price, down_vwap)
            mom_window = self.config.strategy.momentum_window_sec
            up_mom = self.calc.calc_momentum(up.trades, up.last_price, window=mom_window)
            down_mom = self.calc.calc_momentum(down.trades, down.last_price, window=mom_window)

            time_minutes = time_left / 60.0
            span = self.config.market.interval_minutes
            time_bin = int((span - 1) - time_minutes)
            time_bin = max(0, min(time_bin, span - 1))

            if up.last_price > down.last_price:
                fav_name = "UP"
                fav_price = up.last_price
                fav_dev = up_dev
                fav_mom = up_mom
                fav_zscore = self.calc.calc_zscore(up.trades, up.last_price, window=5)
            else:
                fav_name = "DOWN"
                fav_price = down.last_price
                fav_dev = down_dev
                fav_mom = down_mom
                fav_zscore = self.calc.calc_zscore(down.trades, down.last_price, window=5)

            base_wr = self.winrate_table.get_winrate(fav_price, time_bin, span)
            wr_str = f"{base_wr:.1f}%" if base_wr else None

            min_elapsed = self.config.strategy.min_elapsed_sec
            min_dev = self.config.strategy.min_deviation_pct
            max_dev = self.config.strategy.max_deviation_pct
            no_entry_cutoff = self.config.strategy.no_entry_before_end_sec
            elapsed_sec = self.config.market.duration_sec - time_left
            btc_buffer = self._get_btc_buffer_status()

            volume_eval_mode = self._get_volume_eval_mode(time_left)
            late_entry_mode = self._get_late_entry_mode(time_left)
            active_mode = volume_eval_mode or late_entry_mode
            if active_mode:
                min_price = active_mode.get("min_price", 0.0)
                max_price = active_mode.get("max_price", 1.0)
            else:
                min_price = self.config.strategy.min_price
                max_price = self.config.strategy.max_price

            price_ok = min_price <= fav_price <= max_price
            time_ok = elapsed_sec >= min_elapsed
            dev_raw_ok = fav_dev > min_dev and fav_dev < max_dev
            mom_min_pct = float(getattr(self.config.strategy, "momentum_min_pct", 0.0) or 0.0)
            mom_raw_ok = fav_mom is not None and fav_mom > mom_min_pct
            z_raw_ok = fav_zscore is not None and abs(fav_zscore) >= self._zscore_abs_min
            controls = self.get_indicator_controls()
            dev_ok = dev_raw_ok if controls["vwap_deviation"] else True
            mom_ok = mom_raw_ok if controls["momentum"] else True
            zscore_ok = z_raw_ok if controls["zscore"] else True
            time_cutoff_ok = time_left > no_entry_cutoff
            btc_buffer_ok = btc_buffer is not None and btc_buffer["ok"]
            window_checks = self._favorite_window_checks()

            # Check price trend over last 10 seconds
            up_trend = self.calc.calc_price_trend(up.trades, window=10.0) if up and up.trades else None
            down_trend = self.calc.calc_price_trend(down.trades, window=10.0) if down and down.trades else None
            
            # UP is OK if trending up (or no data), DOWN is OK if trending down (or no data)
            up_trend_ok = up_trend is None or up_trend >= 0
            down_trend_ok = down_trend is None or down_trend <= 0
            fav_trend_ok = (fav_name == "UP" and up_trend_ok) or (fav_name == "DOWN" and down_trend_ok)
            next_min_curr_diff = self._get_volume_eval_next_min_current_diff(time_left)
            vol_speed = self._get_favorite_volume_speed_check(
                fav_name,
                up,
                down,
                min_current_volume_diff_override=next_min_curr_diff,
            )
            vol_speed_buy = self._get_favorite_volume_speed_check(
                fav_name,
                up,
                down,
                min_current_volume_diff_override=next_min_curr_diff,
                volume_basis_override="buy",
            )
            vol_speed_total = self._get_favorite_volume_speed_check(
                fav_name,
                up,
                down,
                min_current_volume_diff_override=next_min_curr_diff,
                volume_basis_override="total",
            )
            vol_speed_ok = bool(vol_speed["ok"])
            vol_check_enabled = self._is_volume_check_enabled_for_active_mode(active_mode)
            vol_gate_ok = vol_speed_ok if vol_check_enabled else True
            active_mode_btc = self._mode_btc_gate_status(active_mode, btc_buffer)
            active_mode_btc_ok = bool(active_mode_btc.get("ok", False))

            volume_entry_index: Optional[int] = None
            volume_entry_label: Optional[str] = None
            volume_entry_min_curr_diff: Optional[float] = None
            if volume_eval_mode:
                levels = self._get_volume_eval_threshold_levels()
                if levels:
                    limits = self._get_volume_eval_entry_trade_limits()
                    used_count = self.stats.late_mode_trade_count("volume_eval_mode")
                    remaining = max(used_count, 0)
                    idx = len(levels) - 1
                    for i, cap in enumerate(limits):
                        if remaining < cap:
                            idx = i
                            break
                        remaining -= cap
                    idx = min(max(idx, 0), len(levels) - 1)
                    volume_entry_index = idx + 1
                    volume_entry_label = f"Entry {volume_entry_index} MinVol"
                    volume_entry_min_curr_diff = float(levels[idx])

            late_window_price_only = active_mode is not None
            last_20s_price_only = self.config.strategy.dangerous and time_left <= 20
            price_only_gate = late_window_price_only or last_20s_price_only

            def _mode_strategy_block(mode: Optional[Dict[str, float]], mode_kind: str) -> Dict[str, Any]:
                if not mode:
                    block = {
                        "configured": bool(mode_kind == "volume_eval_mode" and getattr(self.config.strategy, "volume_eval_mode", None) and getattr(self.config.strategy.volume_eval_mode, "enabled", False))
                                      or bool(mode_kind == "late_entry_mode" and getattr(self.config.strategy, "late_entry_modes", None) and getattr(self.config.strategy.late_entry_modes, "enabled", False)),
                        "active": False,
                        "name": mode_kind,
                    }
                    if mode_kind == "volume_eval_mode":
                        block["entry_type"] = volume_entry_label
                        block["entry_min_current_volume_diff"] = (
                            float(volume_entry_min_curr_diff)
                            if volume_entry_min_curr_diff is not None
                            else float(vol_speed.get("min_current_volume_diff", 0.0))
                        )
                    return block

                mode_vol_enabled = self._is_volume_check_enabled_for_active_mode(mode)
                mode_price_ok = mode.get("min_price", 0.0) <= fav_price <= mode.get("max_price", 1.0)
                mode_vol_ok = vol_speed_ok if mode_vol_enabled else True
                mode_btc = self._mode_btc_gate_status(mode, btc_buffer)
                mode_btc_ok = bool(mode_btc.get("ok", False))
                mode_btc_threshold = mode_btc.get("threshold_usd")
                block = {
                    "configured": True,
                    "active": True,
                    "name": str(mode.get("name", mode_kind)),
                    "window_sec": float(mode.get("window_sec", 0.0) or 0.0),
                    "min_price": float(mode.get("min_price", 0.0) or 0.0),
                    "max_price": float(mode.get("max_price", 1.0) or 1.0),
                    "min_contracts": int(float(mode.get("min_contracts", self.config.entry.min_contracts) or self.config.entry.min_contracts)),
                    "buffer_avg_multiplier": float(mode.get("buffer_avg_multiplier", 1.0) or 1.0),
                    "volume_gate_enabled": bool(mode_vol_enabled),
                    "checks": {
                        "price": bool(mode_price_ok),
                        "btc_buffer": bool(mode_btc_ok),
                        "volume": bool(mode_vol_ok),
                    },
                    "btc_buffer_threshold_usd": float(mode_btc_threshold) if mode_btc_threshold is not None else None,
                    "ready": bool(mode_price_ok and mode_btc_ok and mode_vol_ok),
                }
                if mode_kind == "volume_eval_mode":
                    block["entry_type"] = volume_entry_label
                    block["entry_min_current_volume_diff"] = (
                        float(volume_entry_min_curr_diff)
                        if volume_entry_min_curr_diff is not None
                        else float(vol_speed.get("min_current_volume_diff", 0.0))
                    )
                return block

            if price_only_gate:
                if price_ok and active_mode_btc_ok and vol_gate_ok:
                    if volume_eval_mode and active_mode:
                        signal = f"✅ BUY {fav_name} (volume eval, last {int(active_mode['window_sec'])}s)"
                    elif late_entry_mode and active_mode:
                        signal = f"✅ BUY {fav_name} (late entry, last {int(active_mode['window_sec'])}s)"
                    else:
                        signal = f"✅ BUY {fav_name} (last 20s: price + BTC buffer)"
                elif not active_mode_btc_ok and btc_buffer:
                    gate_threshold = active_mode_btc.get("threshold_usd")
                    if volume_eval_mode:
                        label = f"volume eval (last {int(active_mode['window_sec'])}s)"
                    elif late_entry_mode:
                        label = f"late entry (last {int(active_mode['window_sec'])}s)"
                    else:
                        label = "last 20s"
                    signal = f"⏳ WAIT ({label}: BTC buffer < ${float(gate_threshold or btc_buffer['buffer_abs_usd']):.2f})"
                elif vol_check_enabled and not vol_speed_ok:
                    if volume_eval_mode:
                        label = f"volume eval (last {int(active_mode['window_sec'])}s)"
                    elif late_entry_mode:
                        label = f"late entry (last {int(active_mode['window_sec'])}s)"
                    else:
                        label = "last 20s"
                    signal = f"⏳ WAIT ({label}: {fav_name} current volume lead too small)"
                else:
                    if volume_eval_mode:
                        label = f"volume eval (last {int(active_mode['window_sec'])}s)"
                    elif late_entry_mode:
                        label = f"late entry (last {int(active_mode['window_sec'])}s)"
                    else:
                        label = "last 20s"
                    signal = f"⏳ WAIT ({label}: P not in range)"
            elif not time_cutoff_ok:
                signal = f"🚫 NO ENTRY (< {no_entry_cutoff}s left)"
            elif price_ok and time_ok and dev_ok and mom_ok and zscore_ok and btc_buffer_ok and fav_trend_ok and vol_gate_ok:
                signal = f"✅ BUY {fav_name}"
            elif fav_price >= 0.70 and time_ok:
                if not fav_trend_ok:
                    signal = f"🟡 ALMOST (need {fav_name} trending {'up' if fav_name == 'UP' else 'down'})"
                elif not mom_ok:
                    signal = "🟡 ALMOST (need Mom>0%)"
                elif not zscore_ok:
                    signal = f"🟡 ALMOST (need |Z|>={self._zscore_abs_min:.2f})"
                elif vol_check_enabled and not vol_speed_ok:
                    signal = f"🟡 ALMOST (need {fav_name} current volume lead)"
                elif not btc_buffer_ok and btc_buffer:
                    signal = f"🟡 ALMOST (need BTC buffer >= ${btc_buffer['buffer_abs_usd']:.2f})"
                elif fav_dev >= max_dev:
                    signal = f"🟡 ALMOST (Dev≥{max_dev}%)"
                else:
                    signal = "🟡 ALMOST (need dev)"
            elif not time_ok:
                signal = f"⏳ WAIT (elapsed<{min_elapsed}s)"
            elif not price_ok:
                signal = "⏳ WAIT (P not in range)"
            elif not dev_ok:
                signal = (
                    f"⏳ WAIT (Dev≥{max_dev}%)"
                    if fav_dev >= max_dev
                    else f"⏳ WAIT (Dev<{min_dev}%)"
                )
            elif not mom_ok:
                signal = "⏳ WAIT (Mom≤0%)"
            elif not zscore_ok:
                signal = f"⏳ WAIT (|Z|<{self._zscore_abs_min:.2f})"
            elif vol_check_enabled and not vol_speed_ok:
                signal = f"⏳ WAIT ({fav_name} current volume lead too small)"
            elif not fav_trend_ok:
                signal = f"⏳ WAIT ({fav_name} not trending {'up' if fav_name == 'UP' else 'down'})"
            elif not btc_buffer_ok and btc_buffer:
                signal = f"⏳ WAIT (BTC buffer < ${btc_buffer['buffer_abs_usd']:.2f})"
            else:
                signal = "⏳ WAIT"

            strategy = {
                "signal_text": signal,
                "favorite": f"{fav_name} ({fav_price:.3f})",
                "win_rate_str": wr_str,
                "time_bin": time_bin,
                "checks": {
                    "price": price_ok,
                    "time": time_ok,
                    "dev": dev_ok,
                    "mom": mom_ok,
                    "zscore": zscore_ok,
                    "volume": vol_gate_ok,
                    "volume_enabled": vol_check_enabled,
                    "trend": fav_trend_ok,
                    "vol_speed": vol_gate_ok,
                    "btc_buffer": btc_buffer_ok,
                    "time_cutoff": time_cutoff_ok,
                    "last_20s_price_only": price_only_gate,
                },
                "btc_buffer_line": (
                    f"${btc_buffer['current_abs_usd']:,.2f} vs ${btc_buffer['buffer_abs_usd']:,.2f}"
                    if btc_buffer else None
                ),
                "btc_buffer_mode_lines": (
                    [
                        f"{str(row.get('name', 'mode'))}: >= ${float(row.get('threshold_usd', 0.0) or 0.0):,.2f} ({'OK' if bool(row.get('ok', False)) else 'WAIT'})"
                        for row in (btc_buffer.get("mode_thresholds") if isinstance(btc_buffer, dict) else [])
                        if isinstance(row, dict)
                    ]
                    if btc_buffer else []
                ),
                "trend": {
                    "window_sec": 10.0,
                    "up_delta": up_trend,
                    "down_delta": down_trend,
                    "up_ok": up_trend_ok,
                    "down_ok": down_trend_ok,
                    "favorite_ok": fav_trend_ok,
                },
                "volume_speed": {
                    "window_sec": vol_speed["window_sec"],
                    "enabled": bool(vol_speed.get("enabled", True)) and vol_check_enabled,
                    "rule_enabled": vol_check_enabled,
                    "volume_basis": str(vol_speed.get("volume_basis", "total") or "total"),
                    "favorite": fav_name,
                    "fav_curr": round(float(vol_speed["fav_curr"]), 6),
                    "fav_prev": round(float(vol_speed["fav_prev"]), 6),
                    "other_curr": round(float(vol_speed["other_curr"]), 6),
                    "other_prev": round(float(vol_speed["other_prev"]), 6),
                    "curr_diff": round(float(vol_speed["fav_curr"] - vol_speed["other_curr"]), 6),
                    "curr_diff_buy": round(float(vol_speed_buy["fav_curr"] - vol_speed_buy["other_curr"]), 6),
                    "curr_diff_total": round(float(vol_speed_total["fav_curr"] - vol_speed_total["other_curr"]), 6),
                    "fav_accel": round(float(vol_speed["fav_accel"]), 6),
                    "other_accel": round(float(vol_speed["other_accel"]), 6),
                    "min_current_volume_diff": round(float(vol_speed.get("min_current_volume_diff", 0.0)), 6),
                    "ok": vol_speed_ok,
                },
                "active_mode_kind": (
                    "volume_eval_mode" if volume_eval_mode else (
                        "late_entry_mode" if late_entry_mode else "none"
                    )
                ),
                "mode_strategies": {
                    "volume_eval_mode": _mode_strategy_block(volume_eval_mode, "volume_eval_mode"),
                    "late_entry_mode": _mode_strategy_block(late_entry_mode, "late_entry_mode"),
                },
                "up_line": f"{up.last_price:.3f} | Dev {up_dev:+.1f}% | Mom {up_mom if up_mom is not None else 0:.2f}% | Trend {up_trend if up_trend is not None else 0:+.4f}",
                "down_line": f"{down.last_price:.3f} | Dev {down_dev:+.1f}% | Mom {down_mom if down_mom is not None else 0:.2f}% | Trend {down_trend if down_trend is not None else 0:+.4f}",
                "indicator_controls": controls,
                "window_checks": window_checks,
                "favorite_zscore_5s": fav_zscore,
                "zscore_abs_min": self._zscore_abs_min,
            }

        s = self.state
        btc_age = time.time() - s.btc_last_update if s.btc_last_update > 0 else None
        
        btc_block: dict = {
            "btc_current_price": s.btc_current_price,
            "btc_anchor_price": s.btc_anchor_price,
            "btc_market_anchor_price": s.btc_market_anchor_price,
            "btc_market_anchor_source": s.btc_market_anchor_source,
            "btc_anchor_history": [],
            "btc_connected": s.btc_connected,
            "btc_feed_source": s.btc_feed_source,
            "fresh_sec": btc_age,
            "binance_current_price": s.binance_current_price,
            "binance_connected": s.binance_connected,
            "binance_fresh_sec": time.time() - s.binance_last_update if s.binance_last_update > 0 else None,
            "deviation_line": "",
            "buffer_avg_abs_usd": None,
            "buffer_avg_abs_pct": None,
            "current_window_min_usd": s.btc_current_window_min_usd,
            "current_window_max_usd": s.btc_current_window_max_usd,
        }
        if s.btc_current_price > 0 and s.btc_anchor_price > 0:
            dev_abs = s.btc_current_price - s.btc_anchor_price
            dev_pct = (dev_abs / s.btc_anchor_price) * 100 if s.btc_anchor_price else 0.0
            btc_block["deviation_line"] = f"${dev_abs:+,.2f} ({dev_pct:+.3f}%)"
        buffer_stats = self._get_recent_btc_buffer()
        if buffer_stats:
            btc_block["buffer_avg_abs_usd"] = round(buffer_stats["avg_abs_usd"], 6)
            btc_block["buffer_avg_abs_pct"] = round(buffer_stats["avg_abs_pct"], 6)

        # Last 5 window moves for display
        recent_windows = list(self.state.btc_window_moves)[-5:]
        btc_block["buffer_windows"] = [
            {
                "window_ts": int(r.get("window", 0)),
                "signed_usd": round(float(r.get("signed_usd", r.get("abs_usd", 0.0))), 2),
                "signed_pct": round(float(r.get("signed_pct", r.get("abs_pct", 0.0))), 4),
                "abs_usd": round(float(r.get("abs_usd", 0.0)), 2),
                "abs_pct": round(float(r.get("abs_pct", 0.0)), 4),
                "min_usd": round(float(r.get("min_usd", min(0.0, float(r.get("signed_usd", 0.0) or 0.0)))), 2),
                "max_usd": round(float(r.get("max_usd", max(0.0, float(r.get("signed_usd", 0.0) or 0.0)))), 2),
            }
            for r in recent_windows
        ]
        btc_block["btc_anchor_history"] = [
            {
                "ts": int(r.get("ts", 0)),
                "window_ts": int(r.get("window_ts", 0)),
                "btc_price": round(float(r.get("btc_price", 0.0)), 2),
                "anchor_price": round(float(r.get("anchor_price", 0.0)), 2),
                "market_anchor_price": round(float(r.get("market_anchor_price", 0.0)), 2),
            }
            for r in list(self.state.btc_anchor_history)
        ]

        st = self.stats
        bet = self.config.entry.bet_amount_usd
        wr_str = f"{st.win_rate:.1f}%" if st.trade_count > 0 else None
        trading: dict = {
            "bet_usd": bet,
            "markets_seen": st.markets_seen,
            "trade_count": st.trade_count,
            "win_rate_str": wr_str,
            "win_rate_by_mode": {},
            "total_pnl": st.total_pnl,
            "position": None,
            "recent_trades": [],
        }
        summary = st.summary_dict()
        mode_stats = summary.get("win_rate_by_mode") if isinstance(summary, dict) else None
        if isinstance(mode_stats, dict):
            trading["win_rate_by_mode"] = mode_stats
        if st.position:
            pos = st.position
            if pos.token_name == "UP" and self.state.up_token:
                current_price = self.state.up_token.best_bid or self.state.up_token.last_price
            elif pos.token_name == "DOWN" and self.state.down_token:
                current_price = self.state.down_token.best_bid or self.state.down_token.last_price
            else:
                current_price = pos.entry_price
            unrealized = (pos.contracts * current_price) - (pos.contracts * pos.entry_price)
            dd_price = max(0.0, pos.entry_price - pos.min_price_seen)
            dd_pct = (dd_price / pos.entry_price * 100) if pos.entry_price > 0 else 0.0
            dd_usd = dd_price * pos.contracts
            trading["position"] = {
                "token_name": pos.token_name,
                "entry_price": pos.entry_price,
                "contracts": pos.contracts,
                "hedged": pos.hedged,
                "current_price": current_price,
                "unrealized_pnl": unrealized,
                "max_dd_usd": dd_usd,
                "max_dd_pct": dd_pct,
                "min_price_seen": pos.min_price_seen,
            }
        for trade in st.trades[-5:][::-1]:
            icon = "✅" if trade.won else "❌"
            trade_time = datetime.fromtimestamp(trade.timestamp).strftime('%Y-%m-%d %H:%M:%S')
            display_mode = str(getattr(trade, "entry_mode", "unknown") or "unknown")
            if display_mode in {"mixed", "mixed_active_modes"}:
                breakdown = getattr(trade, "mode_breakdown", None)
                if isinstance(breakdown, dict) and breakdown:
                    def _mode_score(item: Tuple[str, Dict[str, Any]]) -> Tuple[float, float, str]:
                        mk, md = item
                        try:
                            triggers = float(md.get("trigger_count", 0.0) or 0.0)
                        except (TypeError, ValueError):
                            triggers = 0.0
                        try:
                            pnl_usd = float(md.get("total_pnl_usd", 0.0) or 0.0)
                        except (TypeError, ValueError):
                            pnl_usd = 0.0
                        return (triggers, abs(pnl_usd), str(mk))

                    display_mode = sorted(breakdown.items(), key=_mode_score, reverse=True)[0][0]
            trading["recent_trades"].append({
                "line": f"{icon} {trade_time} | {trade.token_name} @ {trade.entry_price:.2f} -> ${trade.pnl:+.2f}",
                "entry_mode": display_mode,
            })

        return {
            "ts": now,
            "header": header,
            "strategy": strategy,
            "up": token_block(self.state.up_token),
            "down": token_block(self.state.down_token),
            "btc": btc_block,
            "trading": trading,
            "last_signal": self.last_signal,
            "manual_buy_live_status": self.manual_buy_live_status,
        }

    def build_microstructure_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        state = self.build_web_snapshot()
        up = state.get("up") or {}
        down = state.get("down") or {}

        def with_spread(token_block: Dict[str, Any]) -> Dict[str, Any]:
            book = token_block.get("book") or {}
            best_bid = float(book.get("best_bid") or 0.0)
            best_ask = float(book.get("best_ask") or 0.0)
            spread = (best_ask - best_bid) if best_bid > 0 and best_ask > 0 else None
            mid = ((best_ask + best_bid) / 2.0) if best_bid > 0 and best_ask > 0 else None
            out = dict(token_block)
            out["micro"] = {
                "spread": spread,
                "mid": mid,
            }
            return out

        btc = state.get("btc") or {}
        btc_current = float(btc.get("btc_current_price") or 0.0)
        btc_anchor = float(btc.get("btc_anchor_price") or 0.0)
        btc_dev_usd = btc_current - btc_anchor if btc_current > 0 and btc_anchor > 0 else None
        btc_dev_pct = ((btc_dev_usd / btc_anchor) * 100.0) if (btc_dev_usd is not None and btc_anchor > 0) else None

        return {
            "ts": now,
            "iso": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "market_slug": (state.get("header") or {}).get("slug"),
            "interval_minutes": (state.get("header") or {}).get("interval_minutes"),
            "time_left_sec": (state.get("header") or {}).get("time_left_sec"),
            "strategy": {
                "signal": (state.get("strategy") or {}).get("signal_text"),
                "favorite": (state.get("strategy") or {}).get("favorite"),
                "indicator_controls": (state.get("strategy") or {}).get("indicator_controls"),
                "window_checks": (state.get("strategy") or {}).get("window_checks"),
            },
            "up": with_spread(up),
            "down": with_spread(down),
            "btc": {
                "price": btc_current,
                "anchor": btc_anchor,
                "deviation_usd": btc_dev_usd,
                "deviation_pct": btc_dev_pct,
                "connected": bool(btc.get("btc_connected")),
                "fresh_sec": btc.get("fresh_sec"),
            },
        }


# =============================================================================
# MAIN BOT
# =============================================================================

class LiveTradingBot:
    def __init__(self):
        self.config = None
        self.state = MarketState()
        self.stats = TradingStats()
        self.dashboard: Dashboard = None
        
        # Trading components
        self.executor: OrderExecutor = None
        self.hedge_mgr: HedgeManager = None
        self.redeemer: Optional[AsyncAutoRedeemer] = None
        self.telegram: TelegramNotifier = None
        self.timer_telegram: Optional[TelegramNotifier] = None
        self.user_ws = None
        self._user_ws_task: Optional[asyncio.Task] = None

        # WebSocket
        self.ws_client: WebSocketClient = None
        
        # Chainlink BTC price
        self.chainlink_client: ChainlinkPriceClient = None
        self._chainlink_task: Optional[asyncio.Task] = None
        
        # BTC volume feed (Binance)
        self.btc_volume_feed: BTCVolumeFeed = None
        self._btc_volume_task: Optional[asyncio.Task] = None
        
        # BTC price movement logger (tracks price after each buy)
        self.btc_price_movement_logger: Optional[BTCPriceMovementLogger] = None
        self.market_microstructure_logger: Optional[MarketMicrostructureLogger] = None
        
        # Control
        self.running = False
        self.tasks = []
        self._sim_history: Optional[SimulationHistoryLogger] = None
        self._web_snapshot_holder: Optional[WebSnapshotHolder] = None
        self._config_lock = threading.Lock()
        self._timer_alert_last_sent_slug: str = ""

    def _web_get_indicator_controls(self) -> Dict[str, Any]:
        if not self.dashboard:
            return {"momentum": True, "vwap_deviation": True, "zscore": True}
        return self.dashboard.get_indicator_controls()

    def _web_update_indicator_controls(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.dashboard:
            return {"momentum": True, "vwap_deviation": True, "zscore": True}
        return self.dashboard.update_indicator_controls(payload if isinstance(payload, dict) else {})

    def _web_get_late_modes(self) -> Dict[str, Any]:
        with self._config_lock:
            modes = getattr(self.config.strategy, "late_entry_modes", None)
            if not modes:
                return {"enabled": False, "total_max_trades": 0, "modes": []}

            mode_keys = []
            if self.dashboard is not None:
                mode_keys = self.dashboard._late_mode_keys()
            if not mode_keys:
                mode_keys = ["mode_70s", "mode_60s", "mode_40s", "mode_30s", "mode_20s"]

            out_modes = []
            for key in mode_keys:
                m = getattr(modes, key, None)
                if not m:
                    continue
                out_modes.append({
                    "key": key,
                    "enabled": bool(getattr(m, "enabled", False)),
                    "time_left_sec": int(getattr(m, "time_left_sec", 0)),
                    "min_contracts": int(getattr(m, "min_contracts", 1)),
                    "max_trades": int(getattr(m, "max_trades", 1)),
                    "buffer_avg_multiplier": float(getattr(m, "buffer_avg_multiplier", 1.0)),
                    "min_buffer_threshold_usd": float(getattr(m, "min_buffer_threshold_usd", self.config.buffer)),
                    "min_price": float(getattr(m, "min_price", 0.0)),
                    "max_price": float(getattr(m, "max_price", 1.0)),
                })

            return {
                "enabled": bool(getattr(modes, "enabled", False)),
                "total_max_trades": int(getattr(modes, "total_max_trades", 1)),
                "modes": out_modes,
            }

    def _web_update_late_modes(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return self._web_get_late_modes()

        with self._config_lock:
            modes = getattr(self.config.strategy, "late_entry_modes", None)
            if not modes:
                return {"enabled": False, "total_max_trades": 0, "modes": []}

            modes.enabled = bool(payload.get("enabled", modes.enabled))
            try:
                modes.total_max_trades = max(1, int(payload.get("total_max_trades", modes.total_max_trades)))
            except (TypeError, ValueError):
                pass

            incoming = payload.get("modes", [])
            if isinstance(incoming, list):
                for row in incoming:
                    if not isinstance(row, dict):
                        continue
                    key = str(row.get("key", "")).strip()
                    mode_cfg = getattr(modes, key, None)
                    if not mode_cfg:
                        continue
                    try:
                        mode_cfg.enabled = bool(row.get("enabled", mode_cfg.enabled))
                        mode_cfg.time_left_sec = max(0, int(row.get("time_left_sec", mode_cfg.time_left_sec)))
                        mode_cfg.min_contracts = max(1, int(row.get("min_contracts", mode_cfg.min_contracts)))
                        mode_cfg.max_trades = max(1, int(row.get("max_trades", mode_cfg.max_trades)))
                        mode_cfg.buffer_avg_multiplier = max(0.0, float(row.get("buffer_avg_multiplier", mode_cfg.buffer_avg_multiplier)))
                        mode_cfg.min_buffer_threshold_usd = max(0.0, float(row.get("min_buffer_threshold_usd", getattr(mode_cfg, "min_buffer_threshold_usd", self.config.buffer))))
                        mode_cfg.min_price = float(row.get("min_price", mode_cfg.min_price))
                        mode_cfg.max_price = float(row.get("max_price", mode_cfg.max_price))
                    except (TypeError, ValueError):
                        continue

        return self._web_get_late_modes()

    def _web_get_volume_accel_check(self) -> Dict[str, Any]:
        with self._config_lock:
            vac = getattr(self.config.strategy, "volume_acceleration_check", None)
            if not vac:
                return {"min_current_volume_diff": 0.0, "min_accel_diff": 0.0}
            return {
                "min_current_volume_diff": float(getattr(vac, "min_current_volume_diff", 0.0) or 0.0),
                "min_accel_diff": float(getattr(vac, "min_accel_diff", 0.0) or 0.0),
                "volume_basis": str(getattr(vac, "volume_basis", "total") or "total").strip().lower(),
            }

    def _web_update_volume_accel_check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return self._web_get_volume_accel_check()

        with self._config_lock:
            vac = getattr(self.config.strategy, "volume_acceleration_check", None)
            if not vac:
                return {"min_current_volume_diff": 0.0, "min_accel_diff": 0.0}

            try:
                vac.min_current_volume_diff = max(0.0, float(payload.get("min_current_volume_diff", vac.min_current_volume_diff)))
                vac.min_accel_diff = max(0.0, float(payload.get("min_accel_diff", vac.min_accel_diff)))
                if "volume_basis" in payload:
                    basis = str(payload.get("volume_basis", vac.volume_basis)).strip().lower()
                    if basis in {"total", "buy"}:
                        vac.volume_basis = basis
            except (TypeError, ValueError):
                pass

        return self._web_get_volume_accel_check()

    def _web_get_volume_eval_mode(self) -> Dict[str, Any]:
        with self._config_lock:
            mode = getattr(self.config.strategy, "volume_eval_mode", None)
            if not mode:
                return {"enabled": False}
            return {
                "enabled": bool(getattr(mode, "enabled", False)),
                "time_left_sec": int(getattr(mode, "time_left_sec", 0)),
                "min_contracts": int(getattr(mode, "min_contracts", self.config.entry.min_contracts)),
                "max_trades": int(getattr(mode, "max_trades", 1)),
                "buffer_avg_multiplier": float(getattr(mode, "buffer_avg_multiplier", 1.0)),
                "min_buffer_threshold_usd": float(getattr(mode, "min_buffer_threshold_usd", self.config.buffer)),
                "volume_check_enabled": bool(getattr(mode, "volume_check_enabled", True)),
                "entry_min_current_volume_diffs": list(getattr(mode, "entry_min_current_volume_diffs", [1000.0, 2000.0, 3000.0])),
                "entry_trade_limits": list(getattr(mode, "entry_trade_limits", [1, 1, 1])),
                "min_price": float(getattr(mode, "min_price", self.config.strategy.min_price)),
                "max_price": float(getattr(mode, "max_price", self.config.strategy.max_price)),
            }

    def _web_update_volume_eval_mode(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return self._web_get_volume_eval_mode()

        with self._config_lock:
            mode = getattr(self.config.strategy, "volume_eval_mode", None)
            if not mode:
                return {"enabled": False}

            try:
                mode.enabled = bool(payload.get("enabled", mode.enabled))
                mode.time_left_sec = max(0, int(payload.get("time_left_sec", mode.time_left_sec)))
                mode.min_contracts = max(1, int(payload.get("min_contracts", mode.min_contracts)))
                mode.max_trades = max(1, int(payload.get("max_trades", mode.max_trades)))
                mode.buffer_avg_multiplier = max(0.0, float(payload.get("buffer_avg_multiplier", mode.buffer_avg_multiplier)))
                mode.min_buffer_threshold_usd = max(0.0, float(payload.get("min_buffer_threshold_usd", mode.min_buffer_threshold_usd)))
                mode.volume_check_enabled = bool(payload.get("volume_check_enabled", mode.volume_check_enabled))
                raw_levels = payload.get("entry_min_current_volume_diffs", getattr(mode, "entry_min_current_volume_diffs", []))
                levels = []
                if isinstance(raw_levels, list):
                    for raw in raw_levels:
                        try:
                            v = float(raw)
                        except (TypeError, ValueError):
                            continue
                        if v >= 0.0:
                            levels.append(v)
                if levels:
                    mode.entry_min_current_volume_diffs = levels
                raw_limits = payload.get("entry_trade_limits", getattr(mode, "entry_trade_limits", []))
                limits = []
                if isinstance(raw_limits, list):
                    for raw in raw_limits:
                        try:
                            v = int(float(raw))
                        except (TypeError, ValueError):
                            continue
                        if v > 0:
                            limits.append(v)
                if limits:
                    mode.entry_trade_limits = limits
                mode.min_price = float(payload.get("min_price", mode.min_price))
                mode.max_price = float(payload.get("max_price", mode.max_price))
            except (TypeError, ValueError):
                pass

        return self._web_get_volume_eval_mode()

    def _web_get_timer_alert(self) -> Dict[str, Any]:
        with self._config_lock:
            timer_alert = getattr(self.config.telegram, "timer_alert", None)
            if not timer_alert:
                return {
                    "enabled": False,
                    "time_left_sec": 100,
                    "min_price": 0.75,
                    "max_price": 0.95,
                    "btc_buffer_multiplier": 0.0,
                    "timer_bot_ready": False,
                }
            return {
                "enabled": bool(getattr(timer_alert, "enabled", False)),
                "time_left_sec": int(getattr(timer_alert, "time_left_sec", 100)),
                "min_price": float(getattr(timer_alert, "min_price", 0.75)),
                "max_price": float(getattr(timer_alert, "max_price", 0.95)),
                "btc_buffer_multiplier": float(getattr(timer_alert, "btc_buffer_multiplier", 0.0)),
                "timer_bot_ready": bool(
                    getattr(self.config.telegram, "timer_bot_token", "")
                    and getattr(self.config.telegram, "timer_chat_id", "")
                ),
            }

    def _web_update_timer_alert(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return self._web_get_timer_alert()

        with self._config_lock:
            timer_alert = getattr(self.config.telegram, "timer_alert", None)
            if not timer_alert:
                return self._web_get_timer_alert()

            # Parse fields independently so one bad value does not discard other updates.
            if "enabled" in payload:
                timer_alert.enabled = bool(payload.get("enabled"))

            if "time_left_sec" in payload:
                try:
                    timer_alert.time_left_sec = max(0, int(payload.get("time_left_sec")))
                except (TypeError, ValueError):
                    pass

            if "min_price" in payload:
                try:
                    timer_alert.min_price = min(1.0, max(0.0, float(payload.get("min_price"))))
                except (TypeError, ValueError):
                    pass

            if "max_price" in payload:
                try:
                    timer_alert.max_price = min(1.0, max(0.0, float(payload.get("max_price"))))
                except (TypeError, ValueError):
                    pass

            # Accept both the current key and the older alias for compatibility.
            mult_raw = None
            if "btc_buffer_multiplier" in payload:
                mult_raw = payload.get("btc_buffer_multiplier")
            elif "min_btc_buffer_threshold_usd" in payload:
                mult_raw = payload.get("min_btc_buffer_threshold_usd")
            if mult_raw is not None:
                try:
                    timer_alert.btc_buffer_multiplier = max(0.0, float(mult_raw))
                except (TypeError, ValueError):
                    pass

            if timer_alert.min_price > timer_alert.max_price:
                timer_alert.min_price, timer_alert.max_price = timer_alert.max_price, timer_alert.min_price

        return self._web_get_timer_alert()

    def _get_favorite_price_snapshot(self) -> Optional[Dict[str, Any]]:
        up = self.state.up_token
        down = self.state.down_token
        if not up or not down:
            return None

        up_price = float(up.last_price or 0.0)
        down_price = float(down.last_price or 0.0)
        if up_price <= 0.0 and down_price <= 0.0:
            return None

        if up_price >= down_price:
            return {"favorite": "UP", "price": up_price}
        return {"favorite": "DOWN", "price": down_price}

    async def _check_timer_alert(self) -> None:
        timer_alert = getattr(self.config.telegram, "timer_alert", None)
        if not timer_alert or not bool(getattr(timer_alert, "enabled", False)):
            return

        if not self.timer_telegram or not self.timer_telegram.enabled:
            return

        market_slug = str(self.state.slug or "").strip()
        if not market_slug or market_slug == self._timer_alert_last_sent_slug:
            return

        time_left = max(0.0, self.state.end_time - time.time())
        threshold = float(getattr(timer_alert, "time_left_sec", 100))
        if time_left > threshold:
            return

        fav = self._get_favorite_price_snapshot()
        if not fav:
            return

        fav_price = float(fav["price"])
        min_price = float(getattr(timer_alert, "min_price", 0.75))
        max_price = float(getattr(timer_alert, "max_price", 0.95))
        if not (min_price <= fav_price <= max_price):
            return

        btc_buffer_multiplier = float(getattr(timer_alert, "btc_buffer_multiplier", 0.0) or 0.0)
        current_btc_buffer_usd = None
        btc_now = float(getattr(self.state, "btc_current_price", 0.0) or 0.0)
        btc_anchor = float(getattr(self.state, "btc_anchor_price", 0.0) or 0.0)
        if btc_now > 0.0 and btc_anchor > 0.0:
            current_btc_buffer_usd = abs(btc_now - btc_anchor)

        required_btc_buffer_usd = None
        if btc_buffer_multiplier > 0.0:
            recent_moves = list(self.state.btc_window_moves)[-5:]
            if recent_moves:
                abs_values: List[float] = []
                for row in recent_moves:
                    try:
                        abs_values.append(abs(float(row.get("abs_usd", 0.0) or 0.0)))
                    except (TypeError, ValueError):
                        continue
                if abs_values:
                    required_btc_buffer_usd = (sum(abs_values) / len(abs_values)) * btc_buffer_multiplier

        if btc_buffer_multiplier > 0.0:
            if current_btc_buffer_usd is None:
                return
            if required_btc_buffer_usd is None:
                return
            if current_btc_buffer_usd < required_btc_buffer_usd:
                return

        self._timer_alert_last_sent_slug = market_slug
        btc_buffer_line = ""
        if (
            btc_buffer_multiplier > 0.0
            and current_btc_buffer_usd is not None
            and required_btc_buffer_usd is not None
        ):
            btc_buffer_line = (
                f"\nBTC Buffer: ${current_btc_buffer_usd:.2f} "
                f"(need >= ${required_btc_buffer_usd:.2f}, mult {btc_buffer_multiplier:.2f}x)"
            )
        sent = await self.timer_telegram.send_message(
            (
                "⏰ <b>Timer Alert</b>\n"
                f"Market: {market_slug}\n"
                f"Time left: {int(time_left)}s (threshold {int(threshold)}s)\n"
                f"Favorite: {fav['favorite']}\n"
                f"Price: {fav_price:.3f}\n"
                f"Range: [{min_price:.3f}, {max_price:.3f}]"
                f"{btc_buffer_line}"
            )
        )
        if sent:
            logger.info(
                "Timer alert sent | "
                f"market={market_slug} | left={time_left:.1f}s | fav={fav['favorite']} | price={fav_price:.4f}"
            )
        else:
            logger.warning(
                "Timer alert send failed | "
                f"market={market_slug} | left={time_left:.1f}s | fav={fav['favorite']} | price={fav_price:.4f}"
            )

    def _web_trigger_manual_buy(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.running:
            self.dashboard.manual_buy_live_status = "blocked: bot not running"
            return {"ok": False, "error": "Bot is not running"}

        if not self.dashboard:
            return {"ok": False, "error": "Dashboard not ready"}

        if self.dashboard.last_signal or self.dashboard.manual_signal_pending:
            self.dashboard.manual_buy_live_status = "queued: waiting for previous signal"
            return {"ok": False, "error": "A signal is already queued"}

        up = self.state.up_token
        down = self.state.down_token
        if not up or not down:
            self.dashboard.manual_buy_live_status = "blocked: market tokens not ready"
            return {"ok": False, "error": "Market tokens not ready"}

        up_last = float(up.last_price or 0.0)
        down_last = float(down.last_price or 0.0)
        if up_last <= 0.0 and down_last <= 0.0:
            self.dashboard.manual_buy_live_status = "blocked: no live prices"
            return {"ok": False, "error": "No live token prices yet"}

        amount_usd = self.config.entry.bet_amount_usd
        if isinstance(payload, dict) and ("amount_usd" in payload):
            try:
                amount_usd = float(payload.get("amount_usd"))
            except (TypeError, ValueError):
                self.dashboard.manual_buy_live_status = "blocked: invalid amount"
                return {"ok": False, "error": "Invalid amount"}
        if not (amount_usd > 0):
            self.dashboard.manual_buy_live_status = "blocked: amount must be > 0"
            return {"ok": False, "error": "Amount must be > 0"}

        # Get direction from payload, or auto-determine if not specified
        direction = None
        if isinstance(payload, dict) and ("direction" in payload):
            direction = str(payload.get("direction", "")).upper()
            if direction not in ("UP", "DOWN"):
                self.dashboard.manual_buy_live_status = "blocked: invalid direction"
                return {"ok": False, "error": "Direction must be UP or DOWN"}

        # If direction not specified, auto-determine based on current position or prices
        if not direction:
            if self.stats.position is not None:
                direction = "UP" if self.stats.position.token_name == "UP" else "DOWN"
            else:
                direction = "UP" if up_last >= down_last else "DOWN"

        signal = f"BUY_{direction}"
        self.dashboard.manual_signal_pending = f"{signal}|amount={amount_usd:.8f}"
        self.dashboard.manual_buy_live_status = f"queued: {signal} ${amount_usd:.2f}"
        logger.info(
            f"Manual web buy queued: {signal} amount=${amount_usd:.2f} "
            f"(up={up_last:.4f}, down={down_last:.4f}, market={self.state.slug})"
        )
        return {
            "ok": True,
            "signal": signal,
            "amount_usd": amount_usd,
            "message": f"Queued {signal} (${amount_usd:.2f})",
        }

    def _web_trigger_manual_sell(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        _ = payload
        if not self.running:
            self.dashboard.manual_buy_live_status = "blocked: bot not running"
            return {"ok": False, "error": "Bot is not running"}

        if not self.dashboard:
            return {"ok": False, "error": "Dashboard not ready"}

        if self.dashboard.manual_sell_pending:
            self.dashboard.manual_buy_live_status = "queued: waiting for previous sell"
            return {"ok": False, "error": "A sell request is already queued"}

        if self.stats.position is None:
            self.dashboard.manual_buy_live_status = "blocked: no open position"
            return {"ok": False, "error": "No open position to sell"}

        self.dashboard.manual_sell_pending = "SELL"
        self.dashboard.manual_buy_live_status = "queued: SELL position"
        logger.info(f"Manual web sell queued for market={self.state.slug}")
        return {
            "ok": True,
            "action": "SELL",
            "message": "Queued SELL position",
        }
    
    async def initialize(self) -> bool:
        # Load config
        self.config = load_config()
        errors = validate_config(self.config)
        if errors:
            for err in errors:
                console.print(f"[red]Config error: {err}[/red]")
            return False

        im = self.config.market.interval_minutes
        ##console.print(f"[bold cyan]🚀 BTC {im}-Min Live Trading Bot[/bold cyan]")
        if self.config.simulation.enabled:
            console.print("[bold yellow]   SIMULATION MODE — no CLOB orders, no redeemer[/bold yellow]\n")
        else:
            console.print("[bold cyan]   Real Trading + Dashboard[/bold cyan]\n")

        console.print(f"[green]✓ Market: BTC up/down {im}m (slug btc-updown-{im}m-*)[/green]")
        console.print(f"[green]✓ Config: P {self.config.strategy.min_price}-{self.config.strategy.max_price}, "
                      f"T≥{self.config.strategy.min_elapsed_sec}s, "
                      f"Dev {self.config.strategy.min_deviation_pct}%-{self.config.strategy.max_deviation_pct}%[/green]")
        console.print(f"[green]✓ Bet: ${self.config.entry.bet_amount_usd}, "
                      f"Hedge: {'ON' if self.config.hedge.enabled else 'OFF'}[/green]")
        if self.config.simulation.enabled:
            if self.config.simulation.separate_trading_log:
                self.stats = TradingStats(self.config.simulation.trading_log_path)
                console.print(
                    f"[yellow]✓ Simulation stats: {self.config.simulation.trading_log_path}[/yellow]"
                )
            else:
                console.print("[yellow]✓ Simulation stats: same file as live (trading_log.json)[/yellow]")

        # Initialize trading components
        console.print("[yellow]Initializing trading components...[/yellow]")
        
        # Telegram
        self.telegram = TelegramNotifier(
            bot_token=self.config.telegram.bot_token,
            chat_id=self.config.telegram.chat_id,
            enabled=self.config.telegram.enabled
        )
        self.timer_telegram = TelegramNotifier(
            bot_token=getattr(self.config.telegram, "timer_bot_token", ""),
            chat_id=getattr(self.config.telegram, "timer_chat_id", ""),
            enabled=True,
        )
        if getattr(self.config.telegram, "timer_alert", None) and self.config.telegram.timer_alert.enabled and not self.timer_telegram.enabled:
            logger.warning("Timer alert is enabled but TELEGRAM_TIMER_BOT_TOKEN / TELEGRAM_TIMER_CHAT_ID are missing")

        sim = self.config.simulation.enabled

        if sim:
            self.user_ws = None
            self._user_ws_task = None
            # Dummy credentials — CLOB is never initialized in simulation
            pk = self.config.polymarket.private_key or "0x0000000000000000000000000000000000000000000000000000000000000001"
            ak = self.config.polymarket.api_key or "sim"
            sec = self.config.polymarket.api_secret or "sim"
            ph = self.config.polymarket.api_passphrase or "sim"
            self.executor = OrderExecutor(
                private_key=pk,
                api_key=ak,
                api_secret=sec,
                api_passphrase=ph,
                clob_host=self.config.polymarket.clob_host,
                chain_id=self.config.polymarket.chain_id,
                signature_type=self.config.polymarket.signature_type,
                funder_address=self.config.polymarket.funder_address or None,
                user_ws=None,
                simulation_mode=True,
            )
            console.print("[green]✓ Order executor: simulation (no CLOB)[/green]")
        else:
            # User WebSocket for order tracking (CRITICAL for fill confirmation!)
            self.user_ws = UserWebSocket(
                api_key=os.getenv("POLY_API_KEY"),
                api_secret=os.getenv("POLY_API_SECRET"),
                api_passphrase=os.getenv("POLY_API_PASSPHRASE")
            )
            self._user_ws_task = None

            self.executor = OrderExecutor(
                private_key=os.getenv("PRIVATE_KEY"),
                api_key=os.getenv("POLY_API_KEY"),
                api_secret=os.getenv("POLY_API_SECRET"),
                api_passphrase=os.getenv("POLY_API_PASSPHRASE"),
                clob_host=os.getenv("CLOB_HOST"),
                chain_id=os.getenv("CHAIN_ID"),
                signature_type=os.getenv("SIGNATURE_TYPE"),
                funder_address=os.getenv("FUNDER_ADDRESS") or None,
                user_ws=self.user_ws,
                simulation_mode=False,
            )

            if not await self.executor.initialize():
                console.print("[red]Failed to initialize order executor[/red]")
                return False

            console.print("[yellow]Starting User WebSocket for order tracking...[/yellow]")
            self._user_ws_task = asyncio.create_task(self.user_ws.run_loop())
            await asyncio.sleep(1)
            if self.user_ws.is_connected:
                console.print("[green]User WebSocket connected - order tracking active[/green]")
                logger.info("User WebSocket connected for order fill tracking")
            else:
                console.print("[yellow]User WebSocket connecting... (will retry)[/yellow]")
                logger.warning("User WebSocket not yet connected")
        
        # Hedge manager
        hedge_config = HedgeManagerConfig(
            enabled=self.config.hedge.enabled,
            hedge_price=self.config.hedge.hedge_price,
            order_type=self.config.hedge.order_type,
            max_retries=self.config.hedge.max_retries,
            retry_delay_ms=self.config.hedge.retry_delay_ms,
            simulation_mode=sim,
        )
        self.hedge_mgr = HedgeManager(self.executor, hedge_config)
        
        # Auto redeemer (live only)
        if sim:
            self.redeemer = None
            console.print("[yellow]✓ Auto-redeemer: disabled in simulation[/yellow]")
        else:
            self.redeemer = AsyncAutoRedeemer(
                private_key=os.getenv("PRIVATE_KEY"),
                rpc_url=os.getenv("RPC_URL"),
                funder_address=os.getenv("FUNDER_ADDRESS") or None,
                signature_type=os.getenv("SIGNATURE_TYPE"),
                interval_seconds=self.config.redeem.interval_seconds,
                telegram_notifier=self.telegram
            )

        if sim:
            jl = (self.config.simulation.history_jsonl_path or "").strip()
            self._sim_history = SimulationHistoryLogger(
                csv_path=self.config.simulation.history_csv_path,
                jsonl_path=jl if jl else None,
                summary_path=self.config.simulation.history_summary_path,
            )
            if self.stats.trades:
                self._sim_history.write_summary(
                    [t.__dict__ for t in self.stats.trades],
                    self.stats.summary_dict(),
                )
            csv_p = self.config.simulation.history_csv_path or "(disabled)"
            sum_p = self.config.simulation.history_summary_path or "(disabled)"
            jl_p = jl or "(disabled)"
            console.print(
                f"[green]✓ Simulation analytics: CSV={csv_p} | JSONL={jl_p} | summary={sum_p}[/green]"
            )
        else:
            self._sim_history = None
        
        # BTC price client (chainlink or binance)
        feed_cfg = getattr(self.config, "btc_price_feed", None)
        feed_source = getattr(feed_cfg, "provider", "chainlink") if feed_cfg else "chainlink"
        feed_url_cfg = getattr(feed_cfg, "ws_url", "") if feed_cfg else ""
        feed_url_override = os.getenv("BTC_PRICE_WSS_URL") or feed_url_cfg
        self.state.btc_feed_source = str(feed_source or "chainlink").strip().lower()
        self.chainlink_client = ChainlinkPriceClient(
            self.state,
            self.config.market.duration_sec,
            feed_source=feed_source,
            feed_url=(feed_url_override or None),
        )
        self._chainlink_task = asyncio.create_task(self.chainlink_client.connect())
        source_name = "Chainlink" if self.state.btc_feed_source == "chainlink" else "Binance"
        console.print(f"[green]✓ {source_name} BTC price feed starting...[/green]")
        
        # BTC volume feed (Binance)
        self.btc_volume_feed = BTCVolumeFeed()
        self._btc_volume_task = asyncio.create_task(self.btc_volume_feed.start())
        console.print("[green]✓ Binance BTC volume feed starting...[/green]")
        
        # BTC price movement logger
        self.btc_price_movement_logger = BTCPriceMovementLogger()
        console.print("[green]✓ BTC price movement logger initialized[/green]")

        self.market_microstructure_logger = MarketMicrostructureLogger(
            log_file="data/market_microstructure.jsonl",
            interval_sec=5.0,
        )
        console.print("[green]✓ Market microstructure logger: data/market_microstructure.jsonl[/green]")
        
        # Dashboard
        self.dashboard = Dashboard(self.state, self.stats, self.config)
        self.dashboard.btc_volume_feed = self.btc_volume_feed

        wd = self.config.web_dashboard
        if wd.enabled:
            env_port = os.getenv("PORT")
            env_host = os.getenv("HOST")
            if env_port:
                try:
                    wd.port = int(env_port)
                except ValueError:
                    logger.warning(f"Invalid PORT env value: {env_port}")
            if env_host:
                wd.host = env_host

            # Railway and similar platforms require binding on 0.0.0.0
            if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID"):
                wd.host = "0.0.0.0"

            self._web_snapshot_holder = WebSnapshotHolder()
            ok = start_web_dashboard(
                wd.host,
                wd.port,
                self._web_snapshot_holder,
                get_indicator_controls=self._web_get_indicator_controls,
                update_indicator_controls=self._web_update_indicator_controls,
                get_late_modes=self._web_get_late_modes,
                update_late_modes=self._web_update_late_modes,
                get_volume_accel_check=self._web_get_volume_accel_check,
                update_volume_accel_check=self._web_update_volume_accel_check,
                get_volume_eval_mode=self._web_get_volume_eval_mode,
                update_volume_eval_mode=self._web_update_volume_eval_mode,
                get_timer_alert=self._web_get_timer_alert,
                update_timer_alert=self._web_update_timer_alert,
                trigger_manual_buy=self._web_trigger_manual_buy,
                trigger_manual_sell=self._web_trigger_manual_sell,
            )
            # 0.0.0.0 is not a valid host in a browser URL; use loopback for display.
            railway_public = os.getenv("RAILWAY_PUBLIC_DOMAIN") or os.getenv("RAILWAY_STATIC_URL")
            if railway_public:
                public_host = railway_public.replace("https://", "").replace("http://", "")
                open_url = f"https://{public_host}/"
            elif wd.host in ("0.0.0.0", ""):
                open_url = f"http://127.0.0.1:{wd.port}/"
            elif wd.host in ("::", "[::]"):
                open_url = f"http://[::1]:{wd.port}/"
            else:
                open_url = f"http://{wd.host}:{wd.port}/"
            if ok:
                console.print(f"[green]✓ Web dashboard:[/green] [bold]{open_url}[/bold]")
                console.print(
                    "[dim]  Use http:// not https://. On Windows, if the page fails in your browser, "
                    "open this exact URL (avoid typing only “localhost”, which may use IPv6).[/dim]"
                )
            else:
                console.print(
                    f"[yellow]⚠ Web dashboard did not start on port {wd.port} "
                    f"(in use by another app, or bind failed). Check logs.[/yellow]"
                )
        
        console.print("[green]✓ All components initialized[/green]\n")
        return True
    
    async def find_market(self) -> bool:
        d = self.config.market.duration_sec
        sfx = self.config.market.slug_infix
        console.print(f"[yellow]Searching for active BTC {self.config.market.interval_minutes}-min market...[/yellow]")
        
        async with aiohttp.ClientSession() as session:
            now = int(time.time())
            current_window = (now // d) * d
            
            for offset in [0, d, -d, 2 * d]:
                target_ts = current_window + offset
                expected_slug = f"btc-updown-{sfx}-{target_ts}"
                
                try:
                    async with session.get(f"{GAMMA_API}/markets?slug={expected_slug}") as resp:
                        if resp.status == 200:
                            markets = await resp.json()
                            if markets:
                                market = markets[0]
                                returned_slug = market.get("slug", "")
                                
                                # CRITICAL: Verify API returned the market we asked for
                                if returned_slug != expected_slug:
                                    logger.warning(f"API slug mismatch! Asked for {expected_slug}, got {returned_slug}")
                                    continue
                                
                                if not market.get("closed", True):
                                    return await self._setup_market(market)
                except Exception as e:
                    logger.debug(f"Error finding market {expected_slug}: {e}")
                    continue
        
        return False
    
    async def _setup_market(self, market: dict) -> bool:
        console.print(f"[green]Found: {market.get('slug')}[/green]")
        
        outcomes = market.get("outcomes", [])
        tokens = market.get("clobTokenIds", [])
        
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        
        up_token_id = None
        down_token_id = None
        
        # Use exact index lookup like reference implementation
        try:
            up_index = outcomes.index("Up") if "Up" in outcomes else None
            down_index = outcomes.index("Down") if "Down" in outcomes else None
            
            if up_index is not None and up_index < len(tokens):
                up_token_id = tokens[up_index]
            if down_index is not None and down_index < len(tokens):
                down_token_id = tokens[down_index]
        except (ValueError, IndexError):
            pass
        
        # Fallback to contains-based matching
        if not up_token_id or not down_token_id:
            for i, outcome in enumerate(outcomes):
                if i < len(tokens):
                    outcome_lower = str(outcome).lower()
                    if not up_token_id and "up" in outcome_lower:
                        up_token_id = tokens[i]
                    elif not down_token_id and "down" in outcome_lower:
                        down_token_id = tokens[i]
        
        # Last resort fallback
        if not up_token_id and len(tokens) >= 1:
            up_token_id = tokens[0]
        if not down_token_id and len(tokens) >= 2:
            down_token_id = tokens[1]
        
        if not up_token_id or not down_token_id:
            return False
        
        end_str = market.get("end_date_iso") or market.get("endDate", "")
        try:
            end_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            end_timestamp = end_time.timestamp()
        except:
            end_timestamp = time.time() + self.config.market.duration_sec
        
        slug = market.get("slug", "")
        
        self.state.market_id = market.get("id", "")
        self.state.condition_id = market.get("conditionId", "")
        self.state.slug = slug
        self.state.end_time = end_timestamp
        self.state.up_token = TokenData(token_id=up_token_id, name="Up")
        self.state.down_token = TokenData(token_id=down_token_id, name="Down")
        self.state.connected = False
        # Reset fixed settlement anchor for the new market.
        self.state.btc_market_anchor_price = 0.0
        self.state.btc_market_anchor_source = "none"
        if self.state.btc_current_price > 0:
            self.state.btc_market_anchor_price = self.state.btc_current_price
            self.state.btc_market_anchor_source = "fallback_tick"
        
        # Log token assignments for debugging
        logger.info(f"Market tokens assigned:")
        logger.info(f"  Slug: {slug}")
        logger.info(f"  End Time: {end_str} (timestamp: {end_timestamp})")
        logger.info(f"  UP token: {up_token_id[:40]}...")
        logger.info(f"  DOWN token: {down_token_id[:40]}...")
        
        self.stats.new_market(self.state.slug)
        self.hedge_mgr.clear()  # Reset hedge state for new market
        if self.user_ws:
            self.user_ws.clear_token_fills()  # Reset WS fill buffer for new market
        
        # BTC anchor is now auto-managed by ChainlinkPriceClient
        # It detects interval boundaries from Chainlink timestamps independently
        
        return True

    def _simulation_log_entry(
        self,
        token_name: str,
        avg_price: float,
        contracts: int,
        total_cost: float,
    ) -> None:
        if not self._sim_history or not self.config.simulation.enabled:
            return
        pos = self.stats.position
        hedged = bool(pos and pos.hedged)
        self._sim_history.log_open(
            market_slug=self.state.slug,
            token_name=token_name,
            contracts=contracts,
            avg_price=avg_price,
            total_cost=total_cost,
            cumulative_realized_pnl=self.stats.total_pnl,
            hedged=hedged,
            trade_number=len(self.stats.trades) + 1,
        )
        signal_logger.info(
            f"  [SIM] History OPEN logged | realized PnL before exit: ${self.stats.total_pnl:+.4f}"
        )

    def _simulation_log_close(self, record: TradeRecord, hedged_was: bool) -> None:
        if not self._sim_history or not self.config.simulation.enabled:
            return
        n = len(self.stats.trades)
        self._sim_history.log_close(
            record,
            cumulative_pnl=self.stats.total_pnl,
            total_closed=n,
            win_rate_pct=self.stats.win_rate,
            hedged=hedged_was,
        )
        self._sim_history.write_summary(
            [t.__dict__ for t in self.stats.trades],
            self.stats.summary_dict(),
        )
        s = self.stats.summary_dict()
        signal_logger.info(
            f"  [SIM] History CLOSE logged | trade PnL ${record.pnl:+.4f} | "
            f"cumulative ${s['total_pnl_usd']:+.4f} | WR {s['win_rate_pct']:.2f}% ({n} closed)"
        )
    
    async def execute_entry(
        self,
        side: str,
        bet_amount_override_usd: Optional[float] = None,
        manual_override: bool = False,
    ):
        """Execute entry order (live CLOB or simulation)."""
        time_left = max(0, self.state.end_time - time.time())
        active_modes = self.dashboard._get_active_entry_modes(time_left)
        late_modes_cfg = getattr(self.config.strategy, "late_entry_modes", None)
        total_late_mode_max_trades = int(getattr(late_modes_cfg, "total_max_trades", 3))
        total_late_mode_trade_count = sum(
            self.stats.late_mode_trade_count(mode_key)
            for mode_key in self.dashboard._late_mode_keys()
        )
        total_trade_cap_ok = total_late_mode_trade_count < total_late_mode_max_trades
        volume_threshold_levels = self.dashboard._get_volume_eval_threshold_levels()

        mode_states: List[Dict[str, Any]] = []
        for mode in active_modes:
            mode_name_i = str(mode.get("name", "")).strip()
            is_volume_i = mode_name_i == "volume_eval_mode"
            mode_max_trades_i = int(mode.get("max_trades", 1))
            if is_volume_i and volume_threshold_levels:
                entry_trade_limits = self.dashboard._get_volume_eval_entry_trade_limits()
                if entry_trade_limits:
                    mode_max_trades_i = sum(entry_trade_limits)
                else:
                    mode_max_trades_i = len(volume_threshold_levels)
            mode_trade_count_i = self.stats.late_mode_trade_count(mode_name_i)
            mode_trade_cap_ok_i = self.stats.can_enter_late_mode(mode_name_i, mode_max_trades_i)
            total_cap_ok_i = True if is_volume_i else total_trade_cap_ok
            mode_states.append(
                {
                    "mode": mode,
                    "mode_name": mode_name_i,
                    "is_volume": is_volume_i,
                    "mode_max_trades": mode_max_trades_i,
                    "mode_trade_count": mode_trade_count_i,
                    "mode_trade_cap_ok": mode_trade_cap_ok_i,
                    "total_cap_ok": total_cap_ok_i,
                }
            )

        eligible_mode_states = [
            st for st in mode_states if st["mode_trade_cap_ok"] and st["total_cap_ok"]
        ]

        triggered_mode_names: List[str] = []
        selected_min_contracts = int(self.config.entry.min_contracts)
        mode_name = ""
        late_mode = None
        is_volume_eval_mode = False
        mode_max_trades = 0
        mode_trade_count = 0
        mode_trade_cap_ok = True

        gate_ready_states: List[Dict[str, Any]] = []
        parallel_mode_requests: List[Dict[str, Any]] = []

        if not manual_override:
            if active_modes and not eligible_mode_states:
                first_state = mode_states[0] if mode_states else None
                if first_state and not bool(first_state["mode_trade_cap_ok"]):
                    signal_logger.info(
                        f"SIGNAL IGNORED: {side} - late mode '{first_state['mode_name']}' trade cap reached "
                        f"({first_state['mode_trade_count']}/{first_state['mode_max_trades']})"
                    )
                elif first_state and not bool(first_state["total_cap_ok"]):
                    signal_logger.info(
                        f"SIGNAL IGNORED: {side} - total late-mode trade cap reached "
                        f"({total_late_mode_trade_count}/{total_late_mode_max_trades})"
                    )
                else:
                    signal_logger.info(f"SIGNAL IGNORED: {side} - no eligible active mode")
                return

        if self.stats.position is not None:
            if not manual_override and not eligible_mode_states:
                signal_logger.info(
                    f"SIGNAL IGNORED: {side} - already in position and no eligible active mode for scale-in"
                )
                return
            expected_token_name = "UP" if side == "BUY_UP" else "DOWN"
            if self.stats.position.token_name != expected_token_name:
                signal_logger.info(
                    f"SIGNAL IGNORED: {side} - opposite position already open "
                    f"({self.stats.position.token_name})"
                )
                return
        elif not manual_override and not self.stats.can_enter():
            signal_logger.info(f"SIGNAL IGNORED: {side} - cannot enter (entry blocked for this market)")
            return

        if side == "BUY_UP":
            token = self.state.up_token
            token_name = "UP"
            opposite_token = self.state.down_token
        else:
            token = self.state.down_token
            token_name = "DOWN"
            opposite_token = self.state.up_token

        if not token or not opposite_token:
            signal_logger.warning(f"SIGNAL IGNORED: {side} - token data missing")
            return

        if late_mode:
            min_price = float(late_mode.get("min_price", self.config.strategy.min_price))
            max_price = float(late_mode.get("max_price", self.config.strategy.max_price))
        else:
            min_price = self.config.strategy.min_price
            max_price = self.config.strategy.max_price
        price_ok = min_price <= token.last_price <= max_price

        # Check price trend over last 10 seconds (UP should be trending up, DOWN should be trending down)
        token_trend = self.dashboard.calc.calc_price_trend(token.trades, window=10.0) if token and token.trades else None
        trend_ok = token_trend is None or (
            (side == "BUY_UP" and token_trend >= 0) or
            (side == "BUY_DOWN" and token_trend <= 0)
        )
        fav_name = "UP" if side == "BUY_UP" else "DOWN"
        next_min_curr_diff = self.dashboard._get_volume_eval_next_min_current_diff(time_left)
        vol_speed = self.dashboard._get_favorite_volume_speed_check(
            fav_name,
            self.state.up_token,
            self.state.down_token,
            min_current_volume_diff_override=next_min_curr_diff,
        )
        vol_speed_ok = bool(vol_speed["ok"])
        vol_check_enabled = self.dashboard._is_volume_check_enabled_for_active_mode(late_mode)
        vol_gate_ok = vol_speed_ok if vol_check_enabled else True

        # Defensive time cutoff / buffer / trend gating applies only to auto entries.
        no_entry_cutoff = self.config.strategy.no_entry_before_end_sec
        last_20s_price_only = self.config.strategy.dangerous and time_left <= 20
        btc_buffer = self.dashboard._get_btc_buffer_status()
        btc_buffer_ok = bool(btc_buffer and btc_buffer.get("ok"))

        if eligible_mode_states:
            for st in eligible_mode_states:
                mode_i = st["mode"]
                mode_price_ok = float(mode_i.get("min_price", self.config.strategy.min_price)) <= token.last_price <= float(mode_i.get("max_price", self.config.strategy.max_price))
                mode_vol_enabled = self.dashboard._is_volume_check_enabled_for_active_mode(mode_i)
                mode_vol_ok = vol_speed_ok if mode_vol_enabled else True
                mode_btc_ok = bool(self.dashboard._mode_btc_gate_status(mode_i, btc_buffer).get("ok", False))
                if mode_price_ok and mode_btc_ok and mode_vol_ok:
                    gate_ready_states.append(st)

            if gate_ready_states:
                # Pick one deterministic primary mode for logs while executing all ready modes.
                ranked_states = sorted(
                    gate_ready_states,
                    key=lambda st: (
                        0 if (str(st.get("mode_name", "")).startswith("mode_") and str(st.get("mode_name", "")).endswith("s")) else (1 if bool(st.get("is_volume")) else 2),
                        float(st.get("mode", {}).get("window_sec", 10**9) or 10**9),
                        str(st.get("mode_name", "")),
                    ),
                )

                selected_min_contracts = 0
                triggered_mode_names = []
                seen_mode_names: set[str] = set()
                parallel_mode_requests = []
                for st in ranked_states:
                    mode_name_i = str(st.get("mode_name", "")).strip()
                    if mode_name_i and mode_name_i not in seen_mode_names:
                        seen_mode_names.add(mode_name_i)
                        triggered_mode_names.append(mode_name_i)

                    mode_contracts = int(float(st["mode"].get("min_contracts", self.config.entry.min_contracts) or self.config.entry.min_contracts))
                    mode_contracts = max(1, mode_contracts)
                    selected_min_contracts += mode_contracts
                    parallel_mode_requests.append({
                        "mode_name": mode_name_i or "mode",
                        "min_contracts": mode_contracts,
                    })

                selected_mode_state = ranked_states[0]
                late_mode = selected_mode_state["mode"]
                mode_name = selected_mode_state["mode_name"]
                is_volume_eval_mode = bool(selected_mode_state["is_volume"])
                mode_max_trades = int(selected_mode_state["mode_max_trades"])
                mode_trade_count = int(selected_mode_state["mode_trade_count"])
                mode_trade_cap_ok = bool(selected_mode_state["mode_trade_cap_ok"])

                if is_volume_eval_mode and volume_threshold_levels:
                    chosen_min_curr_diff = float(vol_speed.get("min_current_volume_diff", 0.0) or 0.0)
                    selected_entry_idx = 0
                    best_gap = float("inf")
                    for i, threshold_i in enumerate(volume_threshold_levels):
                        gap = abs(float(threshold_i) - chosen_min_curr_diff)
                        if gap < best_gap:
                            best_gap = gap
                            selected_entry_idx = i
                    mode_name = f"volume_eval_entry_{selected_entry_idx + 1}"

        selected_mode_btc = self.dashboard._mode_btc_gate_status(late_mode, btc_buffer) if late_mode else {
            "ok": btc_buffer_ok,
            "metric": "btc_buffer",
            "current_usd": float((btc_buffer or {}).get("current_abs_usd", 0.0) or 0.0),
            "threshold_usd": float((btc_buffer or {}).get("buffer_abs_usd", 0.0) or 0.0),
        }

        if gate_ready_states:
            price_ok = True
            if late_mode and len(gate_ready_states) == 1:
                min_price = float(late_mode.get("min_price", self.config.strategy.min_price))
                max_price = float(late_mode.get("max_price", self.config.strategy.max_price))
                vol_check_enabled = self.dashboard._is_volume_check_enabled_for_active_mode(late_mode)
                vol_gate_ok = vol_speed_ok if vol_check_enabled else True
                selected_mode_btc = self.dashboard._mode_btc_gate_status(late_mode, btc_buffer)
            else:
                min_price = min(
                    float(st["mode"].get("min_price", self.config.strategy.min_price))
                    for st in gate_ready_states
                )
                max_price = max(
                    float(st["mode"].get("max_price", self.config.strategy.max_price))
                    for st in gate_ready_states
                )
                vol_check_enabled = all(
                    self.dashboard._is_volume_check_enabled_for_active_mode(st["mode"])
                    for st in gate_ready_states
                )
                vol_gate_ok = True
                selected_mode_btc = {
                    "ok": True,
                    "metric": "parallel_modes",
                    "current_usd": float((btc_buffer or {}).get("current_abs_usd", 0.0) or 0.0),
                    "threshold_usd": None,
                }

        price_only_gate = bool(active_modes) or last_20s_price_only
        if not manual_override:
            if active_modes and not gate_ready_states:
                signal_logger.info(
                    f"SIGNAL BLOCKED: {side} - active modes present but none are ready "
                    f"(price/buffer/volume gate not satisfied)"
                )
                return
            if price_only_gate:
                if not price_ok:
                    signal_logger.info(
                        f"SIGNAL BLOCKED: {side} - late-window price-only gate failed "
                        f"({token.last_price:.4f} not in [{min_price}, {max_price}])"
                    )
                    return
                if not trend_ok:
                    signal_logger.info(
                        f"SIGNAL BLOCKED: {side} - not trending in desired direction "
                        f"(trend={'up' if side == 'BUY_UP' else 'down'} required)"
                    )
                    return
            elif time_left < no_entry_cutoff:
                signal_logger.info(
                    f"SIGNAL BLOCKED: {side} - too close to market end "
                    f"({time_left:.0f}s left < {no_entry_cutoff}s cutoff)"
                )
                logger.warning(f"Entry blocked: {time_left:.0f}s left < {no_entry_cutoff}s cutoff")
                return

            if not trend_ok:
                signal_logger.info(
                    f"SIGNAL BLOCKED: {side} - not trending in desired direction "
                    f"(trend={'up' if side == 'BUY_UP' else 'down'} required)"
                )
                return

            if vol_check_enabled and not vol_speed_ok:
                signal_logger.info(
                    f"SIGNAL BLOCKED: {side} - volume speed check failed "
                    f"({fav_name} dV{int(vol_speed['window_sec'])}={vol_speed['fav_accel']:+.2f}, "
                    f"other dV{int(vol_speed['window_sec'])}={vol_speed['other_accel']:+.2f}; "
                    f"curr={vol_speed['fav_curr']:.2f} vs {vol_speed['other_curr']:.2f})"
                )
                return

            if not bool(selected_mode_btc.get("ok", False)):
                current_usd = float(selected_mode_btc.get("current_usd", 0.0) or 0.0)
                threshold_usd = selected_mode_btc.get("threshold_usd")
                if threshold_usd is not None:
                    signal_logger.info(
                        f"SIGNAL BLOCKED: {side} - BTC absolute move ${current_usd:,.2f} "
                        f"< buffer ${float(threshold_usd):,.2f}"
                    )
                    logger.warning(
                        f"Entry blocked by BTC buffer: ${current_usd:,.2f} "
                        f"< ${float(threshold_usd):,.2f}"
                    )
                else:
                    signal_logger.info(f"SIGNAL BLOCKED: {side} - BTC buffer data unavailable")
                return
        
        # Log full signal snapshot
        signal_logger.info("=" * 60)
        signal_logger.info(
            "TRADE SIGNAL TRIGGERED (SIMULATION)" if self.config.simulation.enabled else "TRADE SIGNAL TRIGGERED"
        )
        signal_logger.info(f"  Time: {datetime.now().isoformat()}")
        signal_logger.info(f"  Market: {self.state.slug}")
        signal_logger.info(f"  Signal: {side}")
        signal_logger.info(f"  Token: {token_name}")
        
        time_left = max(0, self.state.end_time - time.time())
        dur = self.config.market.duration_sec
        span = self.config.market.interval_minutes
        elapsed_sec = dur - time_left
        time_bin = int((span - 1) - time_left / 60)
        time_bin = max(0, min(time_bin, span - 1))
        signal_logger.info(f"  Elapsed: {elapsed_sec:.0f}s | Remaining: {time_left:.0f}s | Bin: {time_bin}")
        
        # Calculate all indicators for both tokens
        calc = self.dashboard.calc
        vwap_window = self.config.strategy.vwap_window_sec
        mom_window = self.config.strategy.momentum_window_sec
        
        for label, tk in [("UP", self.state.up_token), ("DOWN", self.state.down_token)]:
            if not tk:
                signal_logger.info(f"  {label}: no data")
                continue
            
            vwap = calc.calc_vwap(calc.get_trades_in_window(tk.trades, vwap_window))
            dev = calc.calc_deviation(tk.last_price, vwap)
            zscore = calc.calc_zscore(tk.trades, tk.last_price, window=5)
            mom = calc.calc_momentum(tk.trades, tk.last_price, window=mom_window)
            mom_str = f"{mom:+.2f}%" if mom is not None else "N/A"
            
            signal_logger.info(f"  --- {label} ---")
            signal_logger.info(f"    Price:   LAST={tk.last_price:.4f}  BID={tk.best_bid:.4f}  ASK={tk.best_ask:.4f}")
            signal_logger.info(f"    VWAP {vwap_window}s: {vwap:.4f}  |  Deviation: {dev:+.2f}%")
            signal_logger.info(f"    Z-Score 5s: {zscore:+.2f}  |  Momentum {mom_window}s: {mom_str}")
            signal_logger.info(f"    Trades: {tk.trade_count}  |  Volume: {tk.volume_total:.0f}")
            signal_logger.info(f"    Buy Vol: {tk.volume_buy:.0f}  |  Sell Vol: {tk.volume_sell:.0f}")
        
        # Win rate
        up = self.state.up_token
        down = self.state.down_token
        if up and down:
            fav_price = up.last_price if up.last_price > down.last_price else down.last_price
            wr = self.dashboard.winrate_table.get_winrate(
                fav_price, time_bin, self.config.market.interval_minutes
            )
            signal_logger.info(f"  Win Rate: {wr:.1f}%" if wr else "  Win Rate: N/A")
        
        # Strategy conditions snapshot
        signal_logger.info(f"  Config: min_price={self.config.strategy.min_price}, "
                          f"max_price={self.config.strategy.max_price}, "
                          f"dangerous={self.config.strategy.dangerous}, "
                          f"min_elapsed={self.config.strategy.min_elapsed_sec}s, "
                          f"dev_range={self.config.strategy.min_deviation_pct}%-{self.config.strategy.max_deviation_pct}%, "
                          f"no_entry_cutoff={self.config.strategy.no_entry_before_end_sec}s")
        
        # Chainlink BTC/USD
        s = self.state
        if s.btc_current_price > 0 and s.btc_anchor_price > 0:
            btc_dev_abs = s.btc_current_price - s.btc_anchor_price
            btc_dev_pct = (btc_dev_abs / s.btc_anchor_price) * 100
            signal_logger.info(f"  BTC Chainlink: ${s.btc_current_price:,.2f} (anchor: ${s.btc_anchor_price:,.2f})")
            signal_logger.info(f"  BTC Deviation: ${btc_dev_abs:+,.2f} ({btc_dev_pct:+.4f}%)")
        buffer_label = self.dashboard._format_recent_btc_buffer()
        if buffer_label:
            signal_logger.info(f"  {buffer_label}")
        else:
            signal_logger.info(f"  BTC Chainlink: N/A")
        
        signal_logger.info("=" * 60)
        
        logger.info(f"Executing entry: {token_name}")

        if late_mode and len(gate_ready_states) <= 1 and ("min_contracts" in late_mode):
            selected_min_contracts = int(late_mode["min_contracts"])
        if late_mode:
            mode_hint = ""
            if str(late_mode.get("name", "")) == "volume_eval_mode":
                mode_hint = f"minCurrDiff={vol_speed.get('min_current_volume_diff', 0):.0f} | "
            if gate_ready_states and len(gate_ready_states) > 1:
                mode_contracts = ", ".join(
                    f"{req['mode_name']}:{int(req['min_contracts'])}"
                    for req in parallel_mode_requests
                )
                signal_logger.info(
                    f"  Parallel modes active: {mode_contracts} | "
                    f"total_min_contracts={selected_min_contracts} | "
                    f"late_total={total_late_mode_trade_count + 1}/{total_late_mode_max_trades}"
                )
            else:
                buffer_mult = late_mode.get("buffer_avg_multiplier")
                buffer_part = f" | buffer_mult={float(buffer_mult):.2f}" if buffer_mult is not None else ""
                signal_logger.info(
                    f"  Late mode active: last {int(late_mode['window_sec'])}s | "
                    f"mode={mode_name} | "
                    f"trades={mode_trade_count + 1}/{mode_max_trades} | "
                    f"late_total={total_late_mode_trade_count + 1}/{total_late_mode_max_trades} | "
                    f"{mode_hint}min_contracts={selected_min_contracts}"
                    f"{buffer_part}"
                )
        
        bet_amount_usd = self.config.entry.bet_amount_usd
        if bet_amount_override_usd is not None and bet_amount_override_usd > 0:
            bet_amount_usd = float(bet_amount_override_usd)

        exec_config = ExecutionConfig(
            bet_amount_usd=bet_amount_usd,
            price_offset=self.config.entry.price_offset,
            max_retries=self.config.entry.max_retries,
            retry_delay_ms=self.config.entry.retry_delay_ms,
            fill_timeout_ms=self.config.entry.fill_timeout_ms,
            min_contracts=selected_min_contracts,
            min_order_usd=self.config.entry.min_order_usd,
            max_entry_price=self.config.entry.max_entry_price
        )

        intended_limit_price = (token.best_ask or 0.0) + float(self.config.entry.price_offset)
        signal_logger.info(
            f"  Intended limit price: {intended_limit_price:.4f} "
            f"(ASK {token.best_ask:.4f} + offset {self.config.entry.price_offset:.4f})"
        )
        
        # Snapshot BTC prices at the moment of order submission
        btc_price_at_entry = self.state.btc_current_price
        # Use rolling window anchor for all trading logic.
        btc_anchor_at_entry = self.state.btc_anchor_price
        if manual_override:
            entry_mode = "manual"
        elif triggered_mode_names and len(triggered_mode_names) > 1:
            entry_mode = "parallel_modes"
        elif mode_name:
            entry_mode = mode_name
        else:
            entry_mode = "normal"

        def _allocate_parallel_mode_legs(total_contracts: int, fill_price: float) -> List[Dict[str, Any]]:
            if manual_override or total_contracts <= 0 or not parallel_mode_requests:
                return []

            total_required = int(sum(int(req.get("min_contracts", 0) or 0) for req in parallel_mode_requests))
            if total_required <= 0:
                return []

            raw_alloc: List[Dict[str, Any]] = []
            allocated = 0
            for req in parallel_mode_requests:
                req_contracts = int(req.get("min_contracts", 0) or 0)
                exact = (float(total_contracts) * float(req_contracts)) / float(total_required)
                base = int(math.floor(exact))
                base = max(0, base)
                allocated += base
                raw_alloc.append({
                    "mode": str(req.get("mode_name", "") or "mode"),
                    "contracts": base,
                    "frac": exact - float(base),
                })

            remaining = int(total_contracts - allocated)
            if remaining > 0 and raw_alloc:
                ranked = sorted(raw_alloc, key=lambda item: item["frac"], reverse=True)
                for i in range(remaining):
                    ranked[i % len(ranked)]["contracts"] += 1

            legs: List[Dict[str, Any]] = []
            for item in raw_alloc:
                c = int(item.get("contracts", 0) or 0)
                if c <= 0:
                    continue
                legs.append({
                    "mode": str(item.get("mode", "") or "mode"),
                    "contracts": c,
                    "entry_price": float(fill_price),
                })
            return legs
        
        result = await self.executor.execute_entry(
            token_id=token.token_id,
            config=exec_config,
            websocket_price=token.best_ask  # Для ПОКУПКИ нужен ASK! Мы платим продавцам.
        )
        
        if result.success:
            mode_legs_alloc = _allocate_parallel_mode_legs(result.contracts_filled, result.avg_price)
            opened_new_position = self.stats.position is None

            if opened_new_position:
                self.stats.record_entry(
                    token_name=token_name,
                    token_id=token.token_id,
                    opposite_token_id=opposite_token.token_id,
                    price=result.avg_price,
                    contracts=result.contracts_filled,
                    market_slug=self.state.slug,
                    btc_price_at_entry=btc_price_at_entry,
                    btc_anchor_at_entry=btc_anchor_at_entry,
                    entry_mode=entry_mode,
                )
                if mode_legs_alloc and self.stats.position:
                    self.stats.position.mode_legs = mode_legs_alloc
                    if len(mode_legs_alloc) > 1:
                        self.stats.position.entry_mode = "parallel_modes"
                    else:
                        self.stats.position.entry_mode = str(mode_legs_alloc[0].get("mode", entry_mode) or entry_mode)
            else:
                if mode_legs_alloc:
                    for leg in mode_legs_alloc:
                        self.stats.add_to_position(
                            price=result.avg_price,
                            contracts=int(leg["contracts"]),
                            btc_price_at_entry=btc_price_at_entry,
                            entry_mode=str(leg["mode"]),
                        )
                else:
                    self.stats.add_to_position(
                        price=result.avg_price,
                        contracts=result.contracts_filled,
                        btc_price_at_entry=btc_price_at_entry,
                        entry_mode=entry_mode,
                    )

            if triggered_mode_names and not manual_override:
                for triggered_mode_name in triggered_mode_names:
                    self.stats.record_late_mode_entry(triggered_mode_name)
            
            # Log BTC price movement for this buy
            if self.btc_price_movement_logger:
                self.btc_price_movement_logger.record_buy(
                    market_slug=self.state.slug,
                    token_name=token_name,
                    entry_price=result.avg_price,
                    contracts=result.contracts_filled,
                    btc_price=btc_price_at_entry,
                    timestamp=time.time(),
                )
            
            self._simulation_log_entry(
                token_name, result.avg_price, result.contracts_filled, result.total_cost
            )
            
            self.dashboard.entry_flash = True
            
            # Log successful entry
            signal_logger.info(
                "ENTRY EXECUTED SUCCESSFULLY (SIMULATED)"
                if self.config.simulation.enabled
                else "ENTRY EXECUTED SUCCESSFULLY"
            )
            signal_logger.info(f"  Token: {token_name}")
            signal_logger.info(f"  Contracts: {result.contracts_filled}")
            signal_logger.info(f"  Avg Price: {result.avg_price:.4f}")
            signal_logger.info(
                f"  Slippage vs intended: {result.avg_price - intended_limit_price:+.4f} "
                f"(actual {result.avg_price:.4f} vs intended {intended_limit_price:.4f})"
            )
            signal_logger.info(f"  Total Cost: ${result.total_cost:.2f}")
            signal_logger.info(f"  Attempts: {result.attempts}")
            signal_logger.info("-" * 40)
            
            await self.telegram.notify_entry(
                side=token_name,
                price=result.avg_price,
                contracts=result.contracts_filled,
                cost=result.total_cost,
                retries=result.attempts,
                interval_minutes=self.config.market.interval_minutes,
                simulation=self.config.simulation.enabled,
            )
            
            logger.info(f"Entry complete: {result.contracts_filled} @ {result.avg_price:.3f}")
            
            # === PLACE GTD HEDGE ORDER ===
            if self.config.hedge.enabled:
                self.hedge_mgr.set_position(
                    opposite_token_id=opposite_token.token_id,
                    contracts=result.contracts_filled
                )
                
                hedge_result = await self.hedge_mgr.place_gtd_hedge()
                
                if hedge_result.success:
                    self.dashboard.hedge_flash = True
                    hedge_cost = hedge_result.contracts * hedge_result.price
                
                    
                    # Register WebSocket handler for hedge fills
                    self._register_hedge_ws_handler()
                    
                    logger.info(f"GTD hedge placed: {hedge_result.contracts} @ ${hedge_result.price}")
                else:
                    logger.error(f"Hedge failed: {hedge_result.error}")
        else:
            signal_logger.error(f"ENTRY FAILED: {result.error}")
            signal_logger.info(f"  Attempts: {result.attempts}")
            signal_logger.info("-" * 40)
            logger.error(f"Entry failed: {result.error}")
            
            # ============================================================
            # КРИТИЧНО: Если был таймаут - НЕ делаем retry (двойная покупка!)
            # Вместо этого проверяем через WebSocket - может ордер исполнился
            # ============================================================
            if result.was_timeout:
                signal_logger.error("🛑 TIMEOUT: Checking WebSocket for fills...")
                logger.warning("Timeout detected — starting WS recovery")
                
                recovered = False
                
                if self.user_ws and self.user_ws.is_connected:
                    recovery_timeout = self.config.entry.ws_recovery_timeout_sec
                    
                    signal_logger.info(f"  Checking WS for fills on {token.token_id[:30]}...")
                    signal_logger.info(f"  Recovery timeout: {recovery_timeout}s")
                    
                    fill_data = await self.user_ws.wait_for_fills_on_token(
                        token_id=token.token_id,
                        timeout=recovery_timeout
                    )
                    
                    if fill_data and fill_data["contracts"] > 0:
                        # ==============================
                        # RECOVERY: Order DID execute!
                        # ==============================
                        recovered = True
                        rec_contracts = fill_data["contracts"]
                        rec_price = fill_data["avg_price"]
                        rec_cost = fill_data["total_cost"]
                        
                        signal_logger.info("=" * 60)
                        signal_logger.info("✅ TIMEOUT RECOVERY: Position found via WebSocket!")
                        signal_logger.info(f"  Contracts: {rec_contracts}")
                        signal_logger.info(f"  Avg Price: {rec_price:.4f}")
                        signal_logger.info(f"  Total Cost: ${rec_cost:.2f}")
                        signal_logger.info(f"  Fills: {len(fill_data['fills'])}")
                        signal_logger.info("=" * 60)
                        
                        logger.info(f"Timeout recovery: {rec_contracts} @ {rec_price:.4f}")
                        
                        mode_legs_alloc = _allocate_parallel_mode_legs(rec_contracts, rec_price)
                        opened_new_position = self.stats.position is None

                        # Record recovered fill with the same scale-in behavior as normal success path.
                        if opened_new_position:
                            self.stats.record_entry(
                                token_name=token_name,
                                token_id=token.token_id,
                                opposite_token_id=opposite_token.token_id,
                                price=rec_price,
                                contracts=rec_contracts,
                                market_slug=self.state.slug,
                                btc_price_at_entry=btc_price_at_entry,
                                btc_anchor_at_entry=btc_anchor_at_entry,
                                entry_mode=entry_mode,
                            )
                            if mode_legs_alloc and self.stats.position:
                                self.stats.position.mode_legs = mode_legs_alloc
                                if len(mode_legs_alloc) > 1:
                                    self.stats.position.entry_mode = "parallel_modes"
                                else:
                                    self.stats.position.entry_mode = str(mode_legs_alloc[0].get("mode", entry_mode) or entry_mode)
                        else:
                            if mode_legs_alloc:
                                for leg in mode_legs_alloc:
                                    self.stats.add_to_position(
                                        price=rec_price,
                                        contracts=int(leg["contracts"]),
                                        btc_price_at_entry=btc_price_at_entry,
                                        entry_mode=str(leg["mode"]),
                                    )
                            else:
                                self.stats.add_to_position(
                                    price=rec_price,
                                    contracts=rec_contracts,
                                    btc_price_at_entry=btc_price_at_entry,
                                    entry_mode=entry_mode,
                                )

                        if triggered_mode_names and not manual_override:
                            for triggered_mode_name in triggered_mode_names:
                                self.stats.record_late_mode_entry(triggered_mode_name)

                        self._simulation_log_entry(
                            token_name, rec_price, rec_contracts, rec_cost
                        )
                        
                        self.dashboard.entry_flash = True
                        
                        await self.telegram.send_message(
                            f"🔄 <b>Timeout Recovery!</b>\n"
                            f"Order filled despite HTTP timeout.\n"
                            f"📊 {token_name} {rec_contracts} @ ${rec_price:.4f}\n"
                            f"💰 Cost: ${rec_cost:.2f}\n"
                            f"Market: {self.state.slug}"
                        )
                        
                        await self.telegram.notify_entry(
                            side=token_name,
                            price=rec_price,
                            contracts=rec_contracts,
                            cost=rec_cost,
                            retries=result.attempts,
                            interval_minutes=self.config.market.interval_minutes,
                            simulation=self.config.simulation.enabled,
                        )
                        
                        # Place hedge (normal flow)
                        if self.config.hedge.enabled:
                            self.hedge_mgr.set_position(
                                opposite_token_id=opposite_token.token_id,
                                contracts=rec_contracts
                            )
                            
                            hedge_result = await self.hedge_mgr.place_gtd_hedge()
                            
                            if hedge_result.success:
                                self.dashboard.hedge_flash = True
                                hedge_cost = hedge_result.contracts * hedge_result.price
                                hsim2 = "🎮 <b>[SIMULATION]</b>\n" if self.config.simulation.enabled else ""
                                await self.telegram.send_message(
                                    f"{hsim2}"
                                    f"🛡️ <b>Hedge Order Placed (GTD)</b>\n"
                                    f"📦 {hedge_result.contracts} contracts @ ${hedge_result.price}\n"
                                    f"💰 Cost: ${hedge_cost:.2f}\n"
                                    f"🔖 Order ID: {hedge_result.order_id[:20]}...\n"
                                    f"📋 Status: LIVE (passive)\n"
                                    f"🔄 Attempts: {hedge_result.attempts}"
                                )
                                
                                self._register_hedge_ws_handler()
                                logger.info(f"GTD hedge placed after recovery: {hedge_result.contracts} @ ${hedge_result.price}")
                            else:
                                await self.telegram.send_message(
                                    f"⚠️ <b>Hedge Failed (after recovery)</b>\n"
                                    f"❌ {hedge_result.error}"
                                )
                    else:
                        signal_logger.info("  WS recovery: no fills found")
                else:
                    signal_logger.warning("  WS not connected — cannot recover")
                
                if not recovered:
                    # No fill found — block entry (original behavior)
                    self.stats.block_entry("Network timeout - no fill detected via WS. Blocking re-entry.")
                    signal_logger.error("🛑 ENTRY BLOCKED: Timeout + no WS fill detected")
                    await self.telegram.send_message(
                        f"⚠️ <b>TIMEOUT — No Fill Detected</b>\n"
                        f"Order status unknown after timeout.\n"
                        f"WebSocket recovery found nothing.\n"
                        f"Re-entry blocked.\n"
                        f"Market: {self.state.slug}"
                    )
    
    def _register_hedge_ws_handler(self):
        """Register WebSocket handler to track hedge order fills."""
        if not self.user_ws:
            logger.warning("User WebSocket not available for hedge tracking")
            return
        
        hedge_order_id = self.hedge_mgr.hedge_order_id
        if not hedge_order_id:
            return
        
        original_on_trade = self.user_ws._on_trade
        
        async def _hedge_trade_handler(data: dict):
            """Handle trade events and check for hedge fills."""
            # Call original handler first
            if original_on_trade:
                await original_on_trade(data)
            
            # Check if this trade is for our hedge order
            # GTD orders are maker orders, so check maker_order_id
            trade_order_id = data.get("maker_order_id", "") or data.get("taker_order_id", "")
            status = data.get("status", "")
            
            if trade_order_id == hedge_order_id and status == "MATCHED":
                size = int(float(data.get("size", 0)))
                price = float(data.get("price", 0))
                
                self.hedge_mgr.on_hedge_fill(size, price)
                
                pos = self._position if hasattr(self, '_position') else None
                filled = self.hedge_mgr._position.hedge_contracts_filled if self.hedge_mgr._position else 0
                total = self.hedge_mgr._position.contracts if self.hedge_mgr._position else 0
                
                if self.hedge_mgr.is_hedged:
                    # Fully filled
                    self.stats.record_hedge(filled, price)
                    self.dashboard.hedge_flash = True
                    
                    await self.telegram.send_message(
                        f"✅ <b>Hedge FULLY Filled!</b>\n"
                        f"📦 {filled} contracts @ ${price}\n"
                        f"🛡️ Position fully protected"
                    )
                    logger.info(f"Hedge fully filled: {filled} contracts")
                else:
                    # Partial fill
                    await self.telegram.send_message(
                        f"🛡️ <b>Hedge Partial Fill</b>\n"
                        f"📦 +{size} contracts @ ${price}\n"
                        f"📊 Progress: {filled}/{total}"
                    )
                    logger.info(f"Hedge partial fill: +{size}, total {filled}/{total}")
        
        self.user_ws._on_trade = _hedge_trade_handler
        logger.info(f"Registered hedge fill handler for order {hedge_order_id[:20]}...")
    
    async def check_market_end(self):
        """Close position at market end."""
        pos = self.stats.position
        if not pos:
            return
        
        time_left = self.state.end_time - time.time()
        if time_left <= 0:  # Close only at/after market expiry for clearer outcome
            hedged_was = pos.hedged
            # Use rolling window anchor for settlement decision in bot logic
            # (UP wins if current BTC > anchor BTC, else DOWN). Fallback to token price
            # only when BTC feed is unavailable.
            s = self.state
            settlement_anchor = s.btc_anchor_price
            if settlement_anchor > 0 and s.btc_current_price > 0:
                if s.btc_current_price > settlement_anchor:
                    winner = "UP"
                elif s.btc_current_price < settlement_anchor:
                    winner = "DOWN"
                else:
                    winner = "TIE"

                if winner == "TIE":
                    if pos.token_name == "UP" and self.state.up_token:
                        final_price = self.state.up_token.last_price
                    elif pos.token_name == "DOWN" and self.state.down_token:
                        final_price = self.state.down_token.last_price
                    else:
                        final_price = 0.5
                else:
                    final_price = 1.0 if pos.token_name == winner else 0.0
            elif pos.token_name == "UP" and self.state.up_token:
                final_price = self.state.up_token.last_price
            elif pos.token_name == "DOWN" and self.state.down_token:
                final_price = self.state.down_token.last_price
            else:
                final_price = 0.5
            
            # Log market end details
            signal_logger.info("=" * 60)
            signal_logger.info("MARKET END - POSITION CLOSING")
            signal_logger.info(f"  Time: {datetime.now().isoformat()}")
            signal_logger.info(f"  Market: {self.state.slug}")
            signal_logger.info(f"  Position: {pos.token_name}")
            signal_logger.info(f"  Entry Price: {pos.entry_price:.4f}")
            signal_logger.info(f"  Final Price: {final_price:.4f}")
            signal_logger.info(f"  Contracts: {pos.contracts}")
            signal_logger.info(f"  Hedged: {pos.hedged}")
            btc_e = pos.btc_price_at_entry
            btc_a = pos.btc_anchor_at_entry
            if btc_e > 0:
                btc_diff = btc_e - btc_a
                btc_diff_pct = (btc_diff / btc_a * 100) if btc_a > 0 else 0.0
                signal_logger.info(f"  BTC at Entry: ${btc_e:,.2f} (anchor: ${btc_a:,.2f}, diff: ${btc_diff:+,.2f} / {btc_diff_pct:+.3f}%)")
            buffer_label = self.dashboard._format_recent_btc_buffer()
            if buffer_label:
                signal_logger.info(f"  {buffer_label}")
            
            btc_close_for_log = s.btc_current_price if s.btc_current_price > 0 else 0.0
            record = self.stats.close_position(final_price, btc_price_at_close=btc_close_for_log)
            if record:
                self._simulation_log_close(record, hedged_was)
                status = "✅ WIN" if record.won else "❌ LOSS"
                winner = pos.token_name if record.won else ("DOWN" if pos.token_name == "UP" else "UP")

                await self.telegram.notify_market_end(
                    winner=winner,
                    pnl=record.pnl,
                    total_pnl=self.stats.total_pnl,
                    win_rate=self.stats.win_rate,
                    btc_close_price=btc_close_for_log,
                    btc_anchor_price=settlement_anchor,
                )
                
                signal_logger.info(f"  Result: {'WIN' if record.won else 'LOSS'}")
                signal_logger.info(f"  P&L: ${record.pnl:+.2f}")
                signal_logger.info(f"  Max Drawdown: -{record.max_drawdown_abs:.4f} (-{record.max_drawdown_pct:.2f}%)")
                dd_usd = record.max_drawdown_abs * record.contracts
                signal_logger.info(f"  Max DD ($): -${dd_usd:.2f} (min price: {record.entry_price - record.max_drawdown_abs:.4f})")
                signal_logger.info(f"  Total Trades: {len(self.stats.trades)}")
                signal_logger.info(f"  Session Stats: W={sum(1 for r in self.stats.trades if r.won)} / L={sum(1 for r in self.stats.trades if not r.won)}")
                signal_logger.info(f"  Total P&L: ${sum(r.pnl for r in self.stats.trades):+.2f}")
                signal_logger.info("=" * 60)
                
                logger.info(f"Position closed: {status}, PnL: ${record.pnl:+.2f}")
    
    async def run_session(self):
        """Run single market session with dashboard."""
        # Start WebSocket
        self.ws_client = WebSocketClient(self.state)
        ws_task = asyncio.create_task(self.ws_client.connect())
        
        await asyncio.sleep(1)
        
        # Track running order task (для non-blocking execution)
        order_task: Optional[asyncio.Task] = None
        last_heartbeat_log = 0.0
        
        try:
            with Live(self.dashboard.render(), refresh_per_second=4, console=console) as live:
                while self.running:
                    # Update dashboard (никогда не блокируется)
                    live.update(self.dashboard.render())
                    if self._web_snapshot_holder:
                        self._web_snapshot_holder.set(self.dashboard.build_web_snapshot())
                    if self.market_microstructure_logger:
                        self.market_microstructure_logger.maybe_log(
                            self.dashboard.build_microstructure_snapshot(),
                            now=time.time(),
                        )
                    
                    # Check for entry signal - запускаем в отдельном task
                    time_left_now = max(0.0, self.state.end_time - time.time())
                    late_mode_active = (
                        self.dashboard._get_volume_eval_mode(time_left_now) is not None
                        or self.dashboard._get_late_entry_mode(time_left_now) is not None
                    )
                    can_attempt_signal = self.stats.can_enter() or late_mode_active
                    queued_manual_sell = self.dashboard.manual_sell_pending
                    queued_manual_signal = self.dashboard.manual_signal_pending
                    queued_auto_signal = self.dashboard.last_signal
                    queued_signal = queued_manual_signal or queued_auto_signal
                    is_manual_signal = bool(queued_manual_signal)
                    should_dispatch = bool(queued_signal) and (can_attempt_signal or is_manual_signal)
                    if should_dispatch:
                        if order_task is None or order_task.done():
                            signal = queued_signal
                            if is_manual_signal:
                                self.dashboard.manual_buy_live_status = "running: sending order"
                                self.dashboard.manual_signal_pending = ""
                            else:
                                self.dashboard.last_signal = ""
                            order_task = asyncio.create_task(self._safe_execute_entry(signal))
                    elif queued_manual_sell:
                        if order_task is None or order_task.done():
                            self.dashboard.manual_buy_live_status = "running: selling position"
                            self.dashboard.manual_sell_pending = ""
                            order_task = asyncio.create_task(self._safe_execute_manual_sell())
                    
                    # Check if order completed
                    if order_task and order_task.done():
                        try:
                            order_task.result()  # Получаем исключения если были
                        except Exception as e:
                            logger.error(f"Order task error: {e}")
                        order_task = None
                    
                    # Track drawdown while in position
                    if self.stats.position:
                        pos = self.stats.position
                        if pos.token_name == "UP" and self.state.up_token:
                            self.stats.update_drawdown(self.state.up_token.last_price)
                        elif pos.token_name == "DOWN" and self.state.down_token:
                            self.stats.update_drawdown(self.state.down_token.last_price)
                    
                    # Update BTC price movements for all active buys
                    if self.btc_price_movement_logger and self.state.btc_current_price > 0:
                        self.btc_price_movement_logger.update_btc_prices(
                            self.state.btc_current_price,
                            time.time()
                        )
                    
                    # Check market end (быстрая операция - не выносим в task)
                    await self._check_timer_alert()
                    await self.check_market_end()

                    # Emit a lightweight heartbeat so platform logs show liveness continuously.
                    now = time.time()
                    if now - last_heartbeat_log >= 5.0:
                        up = self.state.up_token.last_price if self.state.up_token else 0.0
                        down = self.state.down_token.last_price if self.state.down_token else 0.0
                        pos = self.stats.position.token_name if self.stats.position else "NONE"
                        t_left = max(0.0, self.state.end_time - now)
                        logger.info(
                            f"Heartbeat | market={self.state.slug} | left={t_left:.1f}s | "
                            f"pos={pos} | up={up:.4f} | down={down:.4f}"
                        )
                        last_heartbeat_log = now
                    
                    # Market ended?
                    if time.time() > self.state.end_time:
                        console.print("\n[yellow]Market ended![/yellow]")
                        break
                    
                    await asyncio.sleep(0.25)
        finally:
            # Finalize BTC price movement logger at session end
            if self.btc_price_movement_logger:
                self.btc_price_movement_logger.finalize_session(time.time())
                summary = self.btc_price_movement_logger.get_summary_stats()
                if summary:
                    logger.info(f"BTC Price Movement Summary: {summary}")
            
            # Cancel any running order tasks
            for task in [order_task]:
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except:
                        pass
            
            # Graceful WebSocket shutdown
            await self.ws_client.stop_graceful()
            try:
                ws_task.cancel()
                await ws_task
            except:
                pass
            
            # Stop User WebSocket for order tracking
            if self.user_ws:
                await self.user_ws.disconnect()
            if self._user_ws_task:
                try:
                    self._user_ws_task.cancel()
                    await self._user_ws_task
                except:
                    pass

    async def execute_manual_sell(self):
        """Close current position immediately at current tradable price."""
        pos = self.stats.position
        if not pos:
            self.dashboard.manual_buy_live_status = "blocked: no open position"
            return

        if pos.token_name == "UP":
            tk = self.state.up_token
        else:
            tk = self.state.down_token

        if not tk:
            self.dashboard.manual_buy_live_status = "blocked: token data unavailable"
            return

        exit_price = float(tk.best_bid or tk.last_price or 0.0)
        if exit_price <= 0.0:
            self.dashboard.manual_buy_live_status = "blocked: no live sell price"
            return

        hedged_was = pos.hedged
        contracts = int(pos.contracts)
        token_name = str(pos.token_name)
        entry_price = float(pos.entry_price)
        btc_close_for_log = self.state.btc_current_price if self.state.btc_current_price > 0 else 0.0

        signal_logger.info("=" * 60)
        signal_logger.info("MANUAL SELL TRIGGERED")
        signal_logger.info(f"  Market: {self.state.slug}")
        signal_logger.info(f"  Token: {token_name}")
        signal_logger.info(f"  Contracts: {contracts}")
        signal_logger.info(f"  Entry Price: {entry_price:.4f}")
        signal_logger.info(f"  Exit Price: {exit_price:.4f}")

        record = self.stats.close_position_early(exit_price, btc_price_at_close=btc_close_for_log)
        if not record:
            self.dashboard.manual_buy_live_status = "error: sell close failed"
            return

        self._simulation_log_close(record, hedged_was)
        self.dashboard.entry_flash = True

        pnl_tag = "WIN" if record.pnl >= 0 else "LOSS"
        logger.info(
            f"Manual sell complete: {token_name} {contracts} @ {exit_price:.4f} | "
            f"PnL ${record.pnl:+.2f}"
        )
        signal_logger.info(f"  Manual sell result: {pnl_tag} | PnL ${record.pnl:+.2f}")
        signal_logger.info("=" * 60)

        await self.telegram.send_message(
            f"🧾 <b>Manual Sell Executed</b>\n"
            f"Market: {self.state.slug}\n"
            f"Token: {token_name}\n"
            f"Contracts: {contracts}\n"
            f"Entry: ${entry_price:.4f}\n"
            f"Exit: ${exit_price:.4f}\n"
            f"PnL: ${record.pnl:+.2f}"
        )

        self.dashboard.manual_buy_live_status = f"sent: SOLD {token_name} ${exit_price:.3f}"
    
    async def _safe_execute_entry(self, signal: str):
        """Execute entry in separate task with error handling."""
        try:
            side = signal
            bet_override: Optional[float] = None
            is_manual = "|amount=" in signal
            before_trade_count = self.stats.trade_count
            before_contracts = self.stats.position.contracts if self.stats.position else 0
            if "|amount=" in signal:
                raw_side, raw_amt = signal.split("|amount=", 1)
                side = raw_side.strip()
                try:
                    parsed_amt = float(raw_amt.strip())
                    if parsed_amt > 0:
                        bet_override = parsed_amt
                except (TypeError, ValueError):
                    bet_override = None

            await self.execute_entry(
                side,
                bet_amount_override_usd=bet_override,
                manual_override=is_manual,
            )
            if is_manual:
                after_contracts = self.stats.position.contracts if self.stats.position else 0
                if after_contracts > before_contracts or self.stats.trade_count > before_trade_count:
                    self.dashboard.manual_buy_live_status = "sent: executed"
                else:
                    self.dashboard.manual_buy_live_status = "processed: no fill"
        except Exception as e:
            if "|amount=" in signal:
                self.dashboard.manual_buy_live_status = "error: execution failed"
            logger.error(f"Entry execution error: {e}")
            signal_logger.error(f"ENTRY ERROR: {e}")

    async def _safe_execute_manual_sell(self):
        """Execute manual sell in separate task with error handling."""
        try:
            await self.execute_manual_sell()
        except Exception as e:
            self.dashboard.manual_buy_live_status = "error: sell failed"
            logger.error(f"Manual sell execution error: {e}")
            signal_logger.error(f"MANUAL SELL ERROR: {e}")
    
    # GTD hedge is placed immediately after entry (no polling needed)
    # Fills are tracked via WebSocket _register_hedge_ws_handler()
    
    async def run(self):
        """Main run loop."""
        if not await self.initialize():
            return
        
        self.running = True

        redeemer_task = None
        if self.redeemer is not None:
            redeemer_task = asyncio.create_task(self.redeemer.run_loop())

        sim_note = ""
        if self.config.simulation.enabled:
            sim_note = "🎮 <b>SIMULATION MODE</b> — no real orders\n"
        await self.telegram.send_message(
            f"{sim_note}"
            f"🤖 <b>Bot Started</b>\n"
            f"Strategy: ${self.config.entry.bet_amount_usd} per trade\n"
            f"Hedge: {'enabled' if self.config.hedge.enabled else 'disabled'}"
        )
        
        try:
            while self.running:
                # Find market
                if not await self.find_market():
                    console.print("[red]No market found. Waiting 30s...[/red]")
                    await asyncio.sleep(30)
                    continue
                
                console.print("\n[bold green]Starting session...[/bold green]\n")
                await self.run_session()
                
                console.print("[yellow]Waiting 5s for next market...[/yellow]")
                await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Stopping...[/yellow]")
        finally:
            self.running = False
            if self.redeemer is not None:
                self.redeemer.stop()
            if redeemer_task is not None:
                try:
                    redeemer_task.cancel()
                    await redeemer_task
                except Exception:
                    pass
            
            # Gracefully close Chainlink RTDS WebSocket
            if self.chainlink_client:
                await self.chainlink_client.disconnect()
            if self._chainlink_task:
                try:
                    self._chainlink_task.cancel()
                    await self._chainlink_task
                except:
                    pass
            
            # Stop BTC volume feed
            if self.btc_volume_feed:
                await self.btc_volume_feed.stop()
            if self._btc_volume_task:
                try:
                    self._btc_volume_task.cancel()
                    await self._btc_volume_task
                except:
                    pass
            
            await self.telegram.send_message("🛑 Bot stopped")
            await self.telegram.close()
            if self.timer_telegram:
                await self.timer_telegram.close()
            
            console.print("[green]Bot stopped.[/green]")


async def main():
    bot = LiveTradingBot()
    
    loop = asyncio.get_event_loop()
    
    def shutdown():
        bot.running = False
    
    if sys.platform != "win32":
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown)
    
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
