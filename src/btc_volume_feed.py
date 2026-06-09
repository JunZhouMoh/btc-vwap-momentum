#!/usr/bin/env python3
"""
BTC Volume Feed - Fetches BTC/USD volume data from Binance and calculates
volume-weighted indicators for Polymarket tokens.

This module provides:
- Real-time BTC/USD kline (candle) data from Binance
- VWAP calculation using BTC volume as weight instead of trade size
- Alternative volume-based indicators
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Optional, List, Dict
from collections import deque
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger("btc_live.btc_volume")

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"
BINANCE_REST_URL = "https://api.binance.com/api/v3"


@dataclass
class BinanceKline:
    """Single Binance 1-minute kline (candlestick)"""
    timestamp: float  # Unix timestamp (ms converted to seconds)
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float  # BTC volume
    quote_volume: float  # USD volume (BTC * price)
    trades_count: int
    taker_buy_volume: float  # BTC volume bought by takers


@dataclass
class BTCVolumeSnapshot:
    """Aggregated BTC volume stats for a time window"""
    timestamp: float
    avg_price: float
    total_volume: float  # Total BTC volume
    total_quote_volume: float  # Total USD volume
    vwap: float  # Volume-weighted average price
    high: float
    low: float
    trade_count: int


class BinanceKlineClient:
    """
    Connects to Binance WebSocket to stream BTC/USD 1-minute klines.
    Maintains a rolling buffer of recent klines.
    """
    
    def __init__(self, buffer_size: int = 1440):  # 24 hours of 1-min klines
        self.running = False
        self._ws = None
        self.klines: deque = deque(maxlen=buffer_size)
        self._last_kline_time: float = 0.0
        self.connected = False
    
    async def connect(self):
        """Connect to Binance WebSocket and stream klines."""
        self.running = True
        
        while self.running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(BINANCE_WS_URL) as ws:
                        self._ws = ws
                        self.connected = True
                        logger.info("Binance BTC/USD 1m klines connected")
                        
                        async for msg in ws:
                            if not self.running:
                                break
                            
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle_kline_message(msg.data)
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error(f"Binance WS error: {msg.data}")
                                break
                        
                        self._ws = None
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Binance WS error: {e}")
                self.connected = False
                if self.running:
                    await asyncio.sleep(2)
        
        self.connected = False
    
    def _handle_kline_message(self, message: str):
        """Parse and store incoming kline data."""
        try:
            data = json.loads(message)
            kline_data = data.get("k", {})
            
            if not kline_data.get("x"):  # Only process closed klines
                return
            
            timestamp_ms = kline_data.get("t", 0)
            timestamp = timestamp_ms / 1000.0
            
            kline = BinanceKline(
                timestamp=timestamp,
                open_price=float(kline_data.get("o", 0)),
                high_price=float(kline_data.get("h", 0)),
                low_price=float(kline_data.get("l", 0)),
                close_price=float(kline_data.get("c", 0)),
                volume=float(kline_data.get("v", 0)),  # BTC volume
                quote_volume=float(kline_data.get("q", 0)),  # USD volume
                trades_count=int(kline_data.get("n", 0)),
                taker_buy_volume=float(kline_data.get("V", 0)),
            )
            
            self.klines.append(kline)
            self._last_kline_time = timestamp
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.debug(f"Error parsing kline: {e}")
    
    async def disconnect(self):
        """Close WebSocket connection."""
        self.running = False
        if self._ws:
            await self._ws.close()
        self.connected = False
    
    def get_klines_in_window(self, window_seconds: float) -> List[BinanceKline]:
        """Get all klines within the last N seconds."""
        now_sec = datetime.now(timezone.utc).timestamp()
        cutoff = now_sec - window_seconds
        return [k for k in self.klines if k.timestamp >= cutoff]


class BTCVolumeIndicator:
    """
    Calculates VWAP and other volume-weighted indicators using BTC volume
    instead of Polymarket trade volume.
    """
    
    @staticmethod
    def calc_vwap_from_volume(klines: List[BinanceKline]) -> Optional[float]:
        """
        Calculate VWAP using Binance BTC/USD volume as weight.
        
        VWAP = sum(price * volume) / sum(volume)
        
        Args:
            klines: List of BinanceKline objects
        
        Returns:
            VWAP value or None if no data
        """
        if not klines:
            return None
        
        total_pv = sum(k.close_price * k.volume for k in klines)
        total_vol = sum(k.volume for k in klines)
        
        if total_vol <= 0:
            return None
        
        return total_pv / total_vol
    
    @staticmethod
    def calc_vwap_quote_volume(klines: List[BinanceKline]) -> Optional[float]:
        """
        Calculate VWAP using USD (quote) volume as weight.
        This is an alternative that emphasizes dollar volume over BTC volume.
        """
        if not klines:
            return None
        
        total_pv = sum(k.close_price * k.quote_volume for k in klines)
        total_vol = sum(k.quote_volume for k in klines)
        
        if total_vol <= 0:
            return None
        
        return total_pv / total_vol
    
    @staticmethod
    def get_volume_snapshot(klines: List[BinanceKline]) -> Optional[BTCVolumeSnapshot]:
        """
        Get aggregated volume and price stats for a time window.
        """
        if not klines:
            return None
        
        prices = [k.close_price for k in klines]
        volumes = [k.volume for k in klines]
        
        if not volumes or sum(volumes) == 0:
            return None
        
        vwap = sum(p * v for p, v in zip(prices, volumes)) / sum(volumes)
        
        return BTCVolumeSnapshot(
            timestamp=klines[-1].timestamp,
            avg_price=sum(prices) / len(prices),
            total_volume=sum(volumes),
            total_quote_volume=sum(k.quote_volume for k in klines),
            vwap=vwap,
            high=max(k.high_price for k in klines),
            low=min(k.low_price for k in klines),
            trade_count=sum(k.trades_count for k in klines),
        )
    
    @staticmethod
    def calc_volume_ratio(klines: List[BinanceKline]) -> Optional[float]:
        """
        Calculate buy/sell volume ratio.
        Positive = more buy volume, Negative = more sell volume.
        """
        if not klines:
            return None
        
        total_buy = sum(k.taker_buy_volume for k in klines)
        total_volume = sum(k.volume for k in klines)
        
        if total_volume == 0:
            return None
        
        buy_pct = (total_buy / total_volume) * 100
        return buy_pct - 50.0  # 0 = balanced, +50 = all buy, -50 = all sell
    
    @staticmethod
    def calc_volume_trend(klines: List[BinanceKline], recent_count: int = 5) -> Optional[float]:
        """
        Calculate volume trend: compare recent volume to earlier volume.
        Positive = volume increasing, Negative = volume decreasing.
        """
        if not klines or len(klines) < recent_count * 2:
            return None
        
        recent_vol = sum(k.volume for k in list(klines)[-recent_count:])
        earlier_vol = sum(k.volume for k in list(klines)[-(recent_count*2):-recent_count])
        
        if earlier_vol == 0:
            return None
        
        return ((recent_vol - earlier_vol) / earlier_vol) * 100


class BTCVolumeFeed:
    """
    Main interface for BTC volume data feed.
    Handles connection lifecycle and provides accessor methods.
    """
    
    def __init__(self, buffer_size: int = 1440):
        self.client = BinanceKlineClient(buffer_size=buffer_size)
        self.indicator = BTCVolumeIndicator()
    
    async def start(self):
        """Start the BTC volume feed."""
        logger.info("Starting BTC volume feed")
        await self.client.connect()
    
    async def stop(self):
        """Stop the BTC volume feed."""
        logger.info("Stopping BTC volume feed")
        await self.client.disconnect()
    
    def get_vwap(self, window_seconds: float) -> Optional[float]:
        """Get VWAP for a time window using BTC volume weighting."""
        klines = self.client.get_klines_in_window(window_seconds)
        return self.indicator.calc_vwap_from_volume(klines)
    
    def get_volume_snapshot(self, window_seconds: float) -> Optional[BTCVolumeSnapshot]:
        """Get volume stats snapshot for a time window."""
        klines = self.client.get_klines_in_window(window_seconds)
        return self.indicator.get_volume_snapshot(klines)
    
    def get_volume_ratio(self, window_seconds: float) -> Optional[float]:
        """Get buy/sell volume ratio for a time window."""
        klines = self.client.get_klines_in_window(window_seconds)
        return self.indicator.calc_volume_ratio(klines)
    
    def get_volume_trend(self, window_seconds: float, recent_count: int = 5) -> Optional[float]:
        """Get volume trend indicator."""
        klines = self.client.get_klines_in_window(window_seconds)
        return self.indicator.calc_volume_trend(klines, recent_count)
    
    @property
    def is_connected(self) -> bool:
        """Check if feed is connected."""
        return self.client.connected
    
    @property
    def kline_count(self) -> int:
        """Get number of buffered klines."""
        return len(self.client.klines)
