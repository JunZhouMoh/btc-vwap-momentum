#!/usr/bin/env python3
"""
Configuration Loader

Loads settings from config.json and .env file.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default

    value = raw.strip().strip('"').strip("'")
    if value.startswith("$"):
        value = value[1:]

    return int(value or str(default))


@dataclass
class MarketConfig:
    """Which Polymarket BTC up/down interval to trade (slug: btc-updown-{5|15}m-<epoch>)."""
    interval_minutes: int = 15

    @property
    def duration_sec(self) -> int:
        return self.interval_minutes * 60

    @property
    def slug_infix(self) -> str:
        """e.g. '5m' or '15m' for btc-updown-5m-..."""
        return f"{self.interval_minutes}m"


@dataclass
class LateEntryModeConfig:
    """Single late-entry mode settings."""
    name: str = ""
    enabled: bool = True
    time_left_sec: int = 60
    min_contracts: int = 5
    max_trades: int = 1
    buffer_avg_multiplier: float = 1.0
    min_buffer_threshold_usd: float = 25.0
    volume_check_enabled: bool = True
    min_price: float = 0.0
    max_price: float = 1.0


@dataclass
class LateEntryModesConfig:
    """Predefined late-entry windows; tightest active window wins."""
    enabled: bool = False
    total_max_trades: int = 3
    mode_60s: LateEntryModeConfig = field(default_factory=lambda: LateEntryModeConfig(name="mode_60s", enabled=True, time_left_sec=60, min_contracts=5, max_trades=1, buffer_avg_multiplier=1.0, min_buffer_threshold_usd=25.0, min_price=0.8, max_price=0.99))
    mode_40s: LateEntryModeConfig = field(default_factory=lambda: LateEntryModeConfig(name="mode_40s", enabled=True, time_left_sec=40, min_contracts=8, max_trades=1, buffer_avg_multiplier=0.8, min_buffer_threshold_usd=25.0, min_price=0.85, max_price=0.99))
    mode_30s: LateEntryModeConfig = field(default_factory=lambda: LateEntryModeConfig(name="mode_30s", enabled=True, time_left_sec=30, min_contracts=8, max_trades=1, buffer_avg_multiplier=0.8, min_buffer_threshold_usd=25.0, min_price=0.85, max_price=0.99))
    mode_20s: LateEntryModeConfig = field(default_factory=lambda: LateEntryModeConfig(name="mode_20s", enabled=True, time_left_sec=20, min_contracts=12, max_trades=1, buffer_avg_multiplier=0.5, min_buffer_threshold_usd=20.0, min_price=0.9, max_price=0.99))


@dataclass
class VolumeEvalModeConfig:
    """Standalone volume-first entry mode (separate from late-entry modes)."""
    enabled: bool = False
    time_left_sec: int = 50
    min_contracts: int = 1
    max_trades: int = 1
    buffer_avg_multiplier: float = 1.25
    min_buffer_threshold_usd: float = 30.0
    volume_check_enabled: bool = True
    entry_min_current_volume_diffs: list[float] = field(default_factory=lambda: [1000.0, 2000.0, 3000.0])
    entry_trade_limits: list[int] = field(default_factory=lambda: [1, 1, 1])
    min_price: float = 0.84
    max_price: float = 0.96


@dataclass
class VolumeAccelerationCheckConfig:
    """Favored-side volume acceleration gate settings."""
    enabled: bool = True
    window_sec: float = 10.0
    threshold: float = 5000.0
    require_current_volume_lead: bool = True
    min_accel_diff: float = 0.0
    min_current_volume_diff: float = 5000.0


@dataclass
class StrategyConfig:
    """Strategy parameters."""
    min_price: float = 0.65
    max_price: float = 0.91
    dangerous: bool = False
    min_elapsed_sec: int = 480
    min_deviation_pct: float = 5.0
    max_deviation_pct: float = 100.0
    no_entry_before_end_sec: int = 90
    momentum_window_sec: int = 120
    momentum_min_pct: float = 0.0
    vwap_window_sec: int = 30
    win_rate_csv: str = "data/win_rate.csv"
    volume_acceleration_check: VolumeAccelerationCheckConfig = field(default_factory=VolumeAccelerationCheckConfig)
    volume_eval_mode: VolumeEvalModeConfig = field(default_factory=VolumeEvalModeConfig)
    late_entry_modes: LateEntryModesConfig = field(default_factory=LateEntryModesConfig)


@dataclass
class EntryConfig:
    """Entry execution parameters."""
    bet_amount_usd: float = 10.0
    price_offset: float = 0.01
    order_type: str = "FAK"
    max_retries: int = 5
    retry_delay_ms: int = 300
    fill_timeout_ms: int = 2000
    min_contracts: int = 5
    min_order_usd: float = 1.0
    max_entry_price: float = 0.91
    ws_recovery_timeout_sec: int = 10


@dataclass
class HedgeConfig:
    """Hedge execution parameters."""
    enabled: bool = True
    hedge_price: float = 0.02
    order_type: str = "GTD"
    max_retries: int = 3
    retry_delay_ms: int = 1000


@dataclass
class RedeemConfig:
    """Auto-redeem parameters."""
    enabled: bool = True
    interval_seconds: int = 180
    auto_confirm: bool = True


@dataclass
class TelegramConfig:
    """Telegram notification parameters."""
    enabled: bool = True
    bot_token: str = ""
    chat_id: str = ""
    chart_every_n_trades: int = 10


@dataclass
class SimulationConfig:
    """
    Paper-trading mode: same WebSockets, signals, and dashboard; no real orders or redeemer.
    When enabled, API keys and private key are optional (not validated).
    """
    enabled: bool = False
    separate_trading_log: bool = True
    trading_log_path: str = "logs/trading_log_sim.json"
    # Analysis exports (OPEN/CLOSE rows, cumulative PnL). Set jsonl path to "" to disable JSONL.
    history_csv_path: str = "logs/simulation_trades.csv"
    history_jsonl_path: str = "logs/simulation_history.jsonl"
    history_summary_path: str = "logs/simulation_summary.json"


@dataclass
class WebDashboardConfig:
    """Optional local web UI (FastAPI). Bind to 127.0.0.1 unless you trust your network."""
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8765


@dataclass
class BTCPriceFeedConfig:
    """BTC price feed source configuration."""
    provider: str = "chainlink"  # chainlink | binance
    ws_url: str = ""


@dataclass
class PolymarketConfig:
    """Polymarket API credentials."""
    private_key: str = ""
    funder_address: str = ""
    signature_type: int = 0
    rpc_url: str = "https://polygon-rpc.com"
    chain_id: int = 137
    clob_host: str = "https://clob.polymarket.com"
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""


@dataclass
class Config:
    """Main configuration."""
    market: MarketConfig
    simulation: SimulationConfig
    strategy: StrategyConfig
    buffer: float
    entry: EntryConfig
    hedge: HedgeConfig
    redeem: RedeemConfig
    telegram: TelegramConfig
    web_dashboard: WebDashboardConfig
    btc_price_feed: BTCPriceFeedConfig
    polymarket: PolymarketConfig


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from JSON file and environment variables.
    
    Args:
        config_path: Path to config.json (default: PROJECT_ROOT/config.json)
    
    Returns:
        Config object with all settings
    """
    if config_path is None:
        config_path = PROJECT_ROOT / "config.json"
    
    # Load JSON config
    with open(config_path, "r") as f:
        data = json.load(f)
    
    # Market interval (5 or 15 minutes)
    market_data = data.get("market", {})
    market = MarketConfig(
        interval_minutes=int(market_data.get("interval_minutes", 15)),
    )

    sim_data = data.get("simulation", {})
    simulation = SimulationConfig(
        enabled=bool(sim_data.get("enabled", False)),
        separate_trading_log=bool(sim_data.get("separate_trading_log", True)),
        trading_log_path=str(sim_data.get("trading_log_path", "logs/trading_log_sim.json")),
        history_csv_path=str(sim_data.get("history_csv_path", "logs/simulation_trades.csv")),
        history_jsonl_path=str(sim_data.get("history_jsonl_path", "logs/simulation_history.jsonl")),
        history_summary_path=str(sim_data.get("history_summary_path", "logs/simulation_summary.json")),
    )

    # Strategy
    strategy_data = data.get("strategy", {})
    late_modes_data = strategy_data.get("late_entry_modes", {})

    def _to_int(raw: Any, default: int) -> int:
        if raw is None:
            return int(default)
        try:
            txt = str(raw).strip().lower()
            if txt in {"", "tbd", "todo", "na", "n/a", "null", "none", "-"}:
                return int(default)
            return int(float(txt))
        except (TypeError, ValueError):
            return int(default)

    def _to_float(raw: Any, default: float) -> float:
        if raw is None:
            return float(default)
        try:
            txt = str(raw).strip().lower()
            if txt in {"", "tbd", "todo", "na", "n/a", "null", "none", "-"}:
                return float(default)
            return float(txt)
        except (TypeError, ValueError):
            return float(default)

    def _load_late_mode(raw: Dict[str, Any], default_time_left: int, default_contracts: int, default_max_trades: int, default_multiplier: float, default_min_buffer_threshold_usd: float, default_min_price: float = 0.0, default_max_price: float = 1.0) -> LateEntryModeConfig:
        return LateEntryModeConfig(
            name=str(raw.get("name", "") or "").strip(),
            enabled=bool(raw.get("enabled", True)),
            time_left_sec=_to_int(raw.get("time_left_sec", default_time_left), default_time_left),
            min_contracts=_to_int(raw.get("min_contracts", default_contracts), default_contracts),
            max_trades=_to_int(raw.get("max_trades", default_max_trades), default_max_trades),
            buffer_avg_multiplier=_to_float(raw.get("buffer_avg_multiplier", default_multiplier), default_multiplier),
            min_buffer_threshold_usd=_to_float(raw.get("min_buffer_threshold_usd", default_min_buffer_threshold_usd), default_min_buffer_threshold_usd),
            volume_check_enabled=bool(raw.get("volume_check_enabled", True)),
            min_price=_to_float(raw.get("min_price", default_min_price), default_min_price),
            max_price=_to_float(raw.get("max_price", default_max_price), default_max_price),
        )

    late_entry_modes = LateEntryModesConfig(
        enabled=bool(late_modes_data.get("enabled", False)),
        total_max_trades=_to_int(late_modes_data.get("total_max_trades", 3), 3),
        mode_60s=_load_late_mode(late_modes_data.get("mode_60s", {}), 60, 5, 1, 1.0, 25.0, 0.8, 0.99),
        mode_40s=_load_late_mode(late_modes_data.get("mode_40s", {}), 40, 8, 1, 0.8, 25.0, 0.85, 0.99),
        mode_30s=_load_late_mode(late_modes_data.get("mode_30s", {}), 30, 8, 1, 0.8, 25.0, 0.85, 0.99),
        mode_20s=_load_late_mode(late_modes_data.get("mode_20s", {}), 20, 12, 1, 0.5, 20.0, 0.9, 0.99),
    )

    for fallback_name, mode_cfg in [
        ("mode_60s", late_entry_modes.mode_60s),
        ("mode_40s", late_entry_modes.mode_40s),
        ("mode_30s", late_entry_modes.mode_30s),
        ("mode_20s", late_entry_modes.mode_20s),
    ]:
        if not mode_cfg.name:
            mode_cfg.name = fallback_name

    volume_eval_data = strategy_data.get("volume_eval_mode", {})
    raw_entry_diffs = volume_eval_data.get("entry_min_current_volume_diffs", [1000.0, 2000.0, 3000.0])
    entry_min_current_volume_diffs: list[float] = []
    if isinstance(raw_entry_diffs, list):
        for raw in raw_entry_diffs:
            val = _to_float(raw, -1.0)
            if val >= 0.0:
                entry_min_current_volume_diffs.append(val)
    if not entry_min_current_volume_diffs:
        entry_min_current_volume_diffs = [1000.0, 2000.0, 3000.0]

    raw_entry_limits = volume_eval_data.get("entry_trade_limits", [1, 1, 1])
    entry_trade_limits: list[int] = []
    if isinstance(raw_entry_limits, list):
        for raw in raw_entry_limits:
            val = _to_int(raw, 0)
            if val > 0:
                entry_trade_limits.append(val)
    if not entry_trade_limits:
        entry_trade_limits = [1, 1, 1]

    volume_eval_mode = VolumeEvalModeConfig(
        enabled=bool(volume_eval_data.get("enabled", False)),
        time_left_sec=_to_int(volume_eval_data.get("time_left_sec", 50), 50),
        min_contracts=_to_int(volume_eval_data.get("min_contracts", 1), 1),
        max_trades=_to_int(volume_eval_data.get("max_trades", 1), 1),
        buffer_avg_multiplier=_to_float(volume_eval_data.get("buffer_avg_multiplier", 1.25), 1.25),
        min_buffer_threshold_usd=_to_float(volume_eval_data.get("min_buffer_threshold_usd", 30.0), 30.0),
        volume_check_enabled=bool(volume_eval_data.get("volume_check_enabled", True)),
        entry_min_current_volume_diffs=entry_min_current_volume_diffs,
        entry_trade_limits=entry_trade_limits,
        min_price=_to_float(volume_eval_data.get("min_price", 0.84), 0.84),
        max_price=_to_float(volume_eval_data.get("max_price", 0.96), 0.96),
    )

    vac_data = strategy_data.get("volume_acceleration_check", {})
    threshold = _to_float(
        vac_data.get("threshold", vac_data.get("min_current_volume_diff", 5000.0)),
        5000.0,
    )

    strategy = StrategyConfig(
        min_price=strategy_data.get("min_price", 0.65),
        max_price=strategy_data.get("max_price", 0.91),
        dangerous=bool(strategy_data.get("dangerous", False)),
        min_elapsed_sec=strategy_data.get("min_elapsed_sec", 480),
        min_deviation_pct=strategy_data.get("min_deviation_pct", 5.0),
        max_deviation_pct=strategy_data.get("max_deviation_pct", 100.0),
        no_entry_before_end_sec=strategy_data.get("no_entry_before_end_sec", 90),
        momentum_window_sec=strategy_data.get("momentum_window_sec", 120),
        momentum_min_pct=float(strategy_data.get("momentum_min_pct", 0.0)),
        vwap_window_sec=strategy_data.get("vwap_window_sec", 30),
        win_rate_csv=strategy_data.get("win_rate_csv", "data/win_rate.csv"),
        volume_acceleration_check=VolumeAccelerationCheckConfig(
            enabled=bool(vac_data.get("enabled", True)),
            window_sec=_to_float(vac_data.get("window_sec", 10.0), 10.0),
            threshold=threshold,
            require_current_volume_lead=bool(vac_data.get("require_current_volume_lead", True)),
            min_accel_diff=_to_float(vac_data.get("min_accel_diff", 0.0), 0.0),
            min_current_volume_diff=_to_float(vac_data.get("min_current_volume_diff", threshold), threshold),
        ),
        volume_eval_mode=volume_eval_mode,
        late_entry_modes=late_entry_modes,
    )

    # Entry
    entry_data = data.get("entry", {})
    entry = EntryConfig(
        bet_amount_usd=entry_data.get("bet_amount_usd", 10.0),
        price_offset=entry_data.get("price_offset", 0.01),
        order_type=entry_data.get("order_type", "FAK"),
        max_retries=entry_data.get("max_retries", 5),
        retry_delay_ms=entry_data.get("retry_delay_ms", 300),
        fill_timeout_ms=entry_data.get("fill_timeout_ms", 2000),
        min_contracts=entry_data.get("min_contracts", 5),
        min_order_usd=entry_data.get("min_order_usd", 1.0),
        max_entry_price=entry_data.get("max_entry_price", 0.91),
        ws_recovery_timeout_sec=entry_data.get("ws_recovery_timeout_sec", 10),
    )
    
    # Hedge
    hedge_data = data.get("hedge", {})
    hedge = HedgeConfig(
        enabled=hedge_data.get("enabled", True),
        hedge_price=hedge_data.get("hedge_price", 0.02),
        order_type=hedge_data.get("order_type", "GTD"),
        max_retries=hedge_data.get("max_retries", 3),
        retry_delay_ms=hedge_data.get("retry_delay_ms", 1000),
    )
    
    # Redeem
    redeem_data = data.get("redeem", {})
    redeem = RedeemConfig(
        enabled=redeem_data.get("enabled", True),
        interval_seconds=redeem_data.get("interval_seconds", 180),
        auto_confirm=redeem_data.get("auto_confirm", True),
    )
    
    # Telegram (merge JSON + env)
    telegram_data = data.get("telegram", {})
    telegram = TelegramConfig(
        enabled=telegram_data.get("enabled", True),
        bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
        chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
        chart_every_n_trades=telegram_data.get("chart_every_n_trades", 10),
    )

    web_data = data.get("web_dashboard", {})
    web_dashboard = WebDashboardConfig(
        enabled=bool(web_data.get("enabled", False)),
        host=str(web_data.get("host", "127.0.0.1")),
        port=int(web_data.get("port", 8765)),
    )

    btc_feed_data = data.get("btc_price_feed", {})
    btc_price_feed = BTCPriceFeedConfig(
        provider=str(btc_feed_data.get("provider", "chainlink")).strip().lower(),
        ws_url=str(btc_feed_data.get("ws_url", "")).strip(),
    )
    
    # Polymarket (from env only - secrets)
    polymarket = PolymarketConfig(
        private_key=os.getenv("PRIVATE_KEY", ""),
        funder_address=os.getenv("FUNDER_ADDRESS", ""),
        signature_type=_get_env_int("SIGNATURE_TYPE", 0),
        rpc_url=os.getenv("RPC_URL", "https://polygon-rpc.com"),
        chain_id=_get_env_int("CHAIN_ID", 137),
        clob_host=os.getenv("CLOB_HOST", "https://clob.polymarket.com"),
        api_key=os.getenv("POLY_API_KEY", ""),
        api_secret=os.getenv("POLY_API_SECRET", ""),
        api_passphrase=os.getenv("POLY_API_PASSPHRASE", ""),
    )
    
    return Config(
        market=market,
        simulation=simulation,
        strategy=strategy,
        buffer=float(data.get("buffer", 25.0)),
        entry=entry,
        hedge=hedge,
        redeem=redeem,
        telegram=telegram,
        web_dashboard=web_dashboard,
        btc_price_feed=btc_price_feed,
        polymarket=polymarket,
    )


def validate_config(config: Config) -> list:
    """
    Validate configuration.
    
    Returns:
        List of error messages (empty if valid)
    """
    errors = []

    if config.market.interval_minutes not in (5, 15):
        errors.append(
            'market.interval_minutes must be 5 or 15 (Polymarket BTC up/down markets)'
        )

    dur = config.market.duration_sec
    if config.strategy.min_elapsed_sec >= dur:
        errors.append(
            f"strategy.min_elapsed_sec ({config.strategy.min_elapsed_sec}s) must be less than "
            f"market duration ({dur}s for {config.market.interval_minutes}m)"
        )
    if config.strategy.no_entry_before_end_sec >= dur:
        errors.append(
            f"strategy.no_entry_before_end_sec ({config.strategy.no_entry_before_end_sec}s) "
            f"must be less than market duration ({dur}s)"
        )

    live_trading = not config.simulation.enabled

    if live_trading:
        # Required: private key
        if not config.polymarket.private_key:
            errors.append("PRIVATE_KEY not set in .env")
        elif not config.polymarket.private_key.startswith("0x"):
            errors.append("PRIVATE_KEY must start with 0x")

        # Proxy wallet check
        if config.polymarket.signature_type in [1, 2]:
            if not config.polymarket.funder_address:
                errors.append(f"SIGNATURE_TYPE={config.polymarket.signature_type} requires FUNDER_ADDRESS")

        # API credentials
        if not config.polymarket.api_key:
            errors.append("POLY_API_KEY not set")
        if not config.polymarket.api_secret:
            errors.append("POLY_API_SECRET not set")
        if not config.polymarket.api_passphrase:
            errors.append("POLY_API_PASSPHRASE not set")
    
    # Strategy bounds
    if config.strategy.min_price >= config.strategy.max_price:
        errors.append("min_price must be less than max_price")
    
    if config.entry.max_entry_price > config.strategy.max_price:
        errors.append("max_entry_price should not exceed strategy max_price")
    
    if config.strategy.max_deviation_pct <= config.strategy.min_deviation_pct:
        errors.append(
            f"max_deviation_pct ({config.strategy.max_deviation_pct}) "
            f"must be greater than min_deviation_pct ({config.strategy.min_deviation_pct})"
        )

    vac = config.strategy.volume_acceleration_check
    if vac.window_sec <= 0:
        errors.append("strategy.volume_acceleration_check.window_sec must be > 0")
    if vac.threshold < 0:
        errors.append("strategy.volume_acceleration_check.threshold must be >= 0")
    if vac.min_accel_diff < 0:
        errors.append("strategy.volume_acceleration_check.min_accel_diff must be >= 0")
    if vac.min_current_volume_diff < 0:
        errors.append("strategy.volume_acceleration_check.min_current_volume_diff must be >= 0")

    for mode_name, mode_cfg in [
        ("mode_60s", config.strategy.late_entry_modes.mode_60s),
        ("mode_40s", config.strategy.late_entry_modes.mode_40s),
        ("mode_30s", config.strategy.late_entry_modes.mode_30s),
        ("mode_20s", config.strategy.late_entry_modes.mode_20s),
    ]:
        if mode_cfg.min_contracts <= 0:
            errors.append(f"strategy.late_entry_modes.{mode_name}.min_contracts must be > 0")
        if mode_cfg.max_trades <= 0:
            errors.append(f"strategy.late_entry_modes.{mode_name}.max_trades must be > 0")
        if mode_cfg.buffer_avg_multiplier <= 0:
            errors.append(f"strategy.late_entry_modes.{mode_name}.buffer_avg_multiplier must be > 0")
        if mode_cfg.min_buffer_threshold_usd < 0:
            errors.append(f"strategy.late_entry_modes.{mode_name}.min_buffer_threshold_usd must be >= 0")

    volume_mode = config.strategy.volume_eval_mode
    if volume_mode.time_left_sec < 0:
        errors.append("strategy.volume_eval_mode.time_left_sec must be >= 0")
    if volume_mode.min_contracts <= 0:
        errors.append("strategy.volume_eval_mode.min_contracts must be > 0")
    if volume_mode.max_trades <= 0:
        errors.append("strategy.volume_eval_mode.max_trades must be > 0")
    if volume_mode.buffer_avg_multiplier <= 0:
        errors.append("strategy.volume_eval_mode.buffer_avg_multiplier must be > 0")
    if volume_mode.min_buffer_threshold_usd < 0:
        errors.append("strategy.volume_eval_mode.min_buffer_threshold_usd must be >= 0")
    if not volume_mode.entry_min_current_volume_diffs:
        errors.append("strategy.volume_eval_mode.entry_min_current_volume_diffs must have at least 1 value")
    for i, value in enumerate(volume_mode.entry_min_current_volume_diffs, start=1):
        if value < 0:
            errors.append(
                f"strategy.volume_eval_mode.entry_min_current_volume_diffs[{i}] must be >= 0"
            )

    if config.buffer < 0:
        errors.append("buffer must be >= 0")

    if config.btc_price_feed.provider not in {"chainlink", "binance"}:
        errors.append("btc_price_feed.provider must be 'chainlink' or 'binance'")

    if config.web_dashboard.enabled:
        if not (1 <= config.web_dashboard.port <= 65535):
            errors.append("web_dashboard.port must be between 1 and 65535")
    
    return errors
