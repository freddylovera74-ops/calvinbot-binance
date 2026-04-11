"""
execution_bot.py — Maestro de Ejecución para el sistema de trading Calvin5.

Es el ÚNICO componente autorizado a enviar órdenes reales al exchange (Polymarket CLOB).
Recibe señales de calvin5.py (calculador) vía Redis y gestiona toda la ejecución.

Arquitectura de canales Redis:
  ENTRADA  ← signals:trade            (señales de calvin5)
  SALIDA   → execution:receipts       (confirmaciones de fill → calvin5)
  SALIDA   → execution:logs           (log completo → dynamic_optimizer)
  SALIDA   → health:heartbeats        (latido cada 5s → watchdog)

Nota: Polymarket usa su propio CLOB (py-clob-client), no CCXT.
      La interfaz sigue el patrón CCXT (exchange-agnostic) para intercambiabilidad.

Dependencias extra:
  pip install redis py-clob-client web3 requests python-dotenv

Variables de entorno (.env):
  REDIS_URL, PRIVATE_KEY, POLYMARKET_ADDRESS, CLOB_API_KEY,
  CLOB_SECRET, CLOB_PASSPHRASE, POLYGON_RPC, DRY_RUN_CALVIN5

Uso:
  python execution_bot.py
  python execution_bot.py --dry-run   # no ejecuta órdenes reales
"""

import asyncio
import enum
import json
import logging
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

from utils import madrid_now as _madrid_now, in_trading_hours as _in_trading_hours, madrid_today_str as _madrid_today_str, configure_structlog  # noqa: E402
configure_structlog("executor", log_file="executor.log")


class _UnifiedLogHandler(logging.Handler):
    """Publica mensajes INFO/WARNING/ERROR al canal logs:unified de Redis."""
    _redis_client = None  # se inyecta desde ExecutionBot.run() tras conectar Redis

    def emit(self, record: logging.LogRecord) -> None:
        if self._redis_client is None:
            return
        try:
            level_map = {
                logging.INFO:     "INFO",
                logging.WARNING:  "WARN",
                logging.ERROR:    "ERROR",
                logging.CRITICAL: "CRITICAL",
            }
            lvl = level_map.get(record.levelno, "INFO")
            payload = json.dumps({
                "bot":       "executor",
                "level":     lvl,
                "msg":       record.getMessage()[:300],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            # Fire-and-forget en un thread pool para no bloquear el loop async
            import threading
            threading.Thread(
                target=self._redis_client.publish,
                args=("logs:unified", payload),
                daemon=True,
            ).start()
        except Exception as e:
            # ISSUE #5: Log en lugar de silenciar — handler fire-and-forget no es crítico
            pass  # Logging a Redis en thread daemon e ignorable si falla


_unified_handler = _UnifiedLogHandler()
_unified_handler.setLevel(logging.INFO)


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("execution_bot")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s [EXECUTOR] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler("executor.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    logger.addHandler(_unified_handler)
    return logger

log = _setup_logger()


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379")
CHANNEL_SIGNALS      = "signals:trade"          # Calvin5 → Executor (pub/sub legacy fallback)
STREAM_SIGNALS       = "stream:signals:trade"   # Stream persistente (at-least-once)
STREAM_GROUP         = "executors"              # Consumer group para el stream de señales
STREAM_CONSUMER      = "executor-1"             # Nombre de este consumidor
CHANNEL_EMERGENCY    = "emergency:commands"      # Watchdog → Executor (autoridad SUPREMA)
CHANNEL_RECEIPTS     = "execution:receipts"      # Executor → Calvin5
CHANNEL_LOGS         = "execution:logs"          # Executor → Optimizer
CHANNEL_HEARTBEATS   = "health:heartbeats"       # Executor → Watchdog
CHANNEL_ALERTS       = "alerts:executor"         # Executor → Watchdog (Circuit Breaker alerts)
HEARTBEAT_INTERVAL_S = 5

# Selector de adaptador de exchange — cambiar a "binance" para Binance Testnet
# Polymarket → py-clob-client (comportamiento actual)
# Binance    → BinanceSpotExchange (binance_exchange.py)
EXCHANGE_ADAPTER  = os.getenv("EXCHANGE_ADAPTER", "polymarket").lower()

# Polymarket CLOB
CLOB_API          = "https://clob.polymarket.com"
GAMMA_API         = "https://gamma-api.polymarket.com"
_PRIVATE_KEY      = os.getenv("PRIVATE_KEY", "")
_WALLET_ADDR      = os.getenv("POLYMARKET_ADDRESS", "")
_CLOB_API_KEY     = os.getenv("CLOB_API_KEY", "")
_CLOB_SECRET      = os.getenv("CLOB_SECRET", "")
_CLOB_PASS        = os.getenv("CLOB_PASSPHRASE", "")
_CHAIN_ID         = 137
POLYGON_RPC       = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")

# ISSUE #4: Validación temprana de env vars en REAL_MODE
def _validate_env_vars() -> None:
    """Fail-fast: verifica que las env vars críticas estén configuradas antes de operar."""
    if DRY_RUN:
        return  # En DRY_RUN no es crítico
    required_vars = {
        "PRIVATE_KEY": _PRIVATE_KEY,
        "POLYMARKET_ADDRESS": _WALLET_ADDR,
        "CLOB_API_KEY": _CLOB_API_KEY,
        "CLOB_SECRET": _CLOB_SECRET,
        "CLOB_PASSPHRASE": _CLOB_PASS,
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        log.critical(f"❌ FALTA CONFIGURACIÓN EN REAL_MODE: {', '.join(missing)}")
        log.critical("Adiciona estas variables a tu archivo .env y reinicia.")
        sys.exit(1)

# Polymarket contratos Polygon
_CTF_TOKENS        = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
_CTF_EXCHANGE      = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
_NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Modo simulación — heredado de calvin5 o forzado con --dry-run
DRY_RUN = (
    "--dry-run" in sys.argv
    or os.getenv("DRY_RUN", "true").lower() == "true"
)
MICRO_TEST = os.getenv("MICRO_TEST", "false").lower() == "true"

# O14: Parámetros de simulación realista para DRY_RUN
DRY_FOK_REJECT_RATE  = float(os.getenv("DRY_FOK_REJECT_RATE",  "0.30"))   # 30% de rechazos FOK
DRY_SLIPPAGE_MAX     = float(os.getenv("DRY_SLIPPAGE_MAX",     "0.015"))  # ±1.5% slippage en fill
DRY_LATENCY_MIN_MS   = int(os.getenv("DRY_LATENCY_MIN_MS",     "500"))    # latencia mínima simulada (ms)
DRY_LATENCY_MAX_MS   = int(os.getenv("DRY_LATENCY_MAX_MS",     "2000"))   # latencia máxima simulada (ms)

# ── Guardrails de riesgo local (Fat-Finger Protection) ───────────────────────
MAX_ORDER_VALUE_USD  = float(os.getenv("MAX_ORDER_VALUE_USD", "50.0"))   # orden máxima en $
MAX_DAILY_TRADES     = int(os.getenv("MAX_DAILY_TRADES", "200"))          # órdenes máximas por día
MIN_SELL_TOKENS      = 5.0        # mínimo tokens para SELL en CLOB
SLIP_MAX_PCT         = float(os.getenv("SLIP_MAX_PCT", "0.04"))           # slippage máx
FOK_PRICE_BUF        = float(os.getenv("FOK_PRICE_BUF", "0.01"))         # buffer FOK sobre señal

# Timeout máximo esperando fill del CLOB antes de reportar fallo
FILL_TIMEOUT_S = 10.0

# ── Circuit Breaker ───────────────────────────────────────────────────────────
CB_WATCHDOG_TIMEOUT_S   = float(os.getenv("CB_WATCHDOG_TIMEOUT_S",   "600.0")) # s sin Watchdog → advertencia (watchdog no envía pings periódicos)
CB_REDIS_LATENCY_MS_MAX = float(os.getenv("CB_REDIS_LATENCY_MS_MAX", "500.0")) # ms latencia → CRITICO
CB_MAX_RED_ERRORS       = int(os.getenv("CB_MAX_RED_ERRORS",          "3"))     # RED consecutivos → CRITICO
CB_RECONCILE_TIMEOUT_S  = float(os.getenv("CB_RECONCILE_TIMEOUT_S",  "20.0"))  # timeout reconciliación


# ─────────────────────────────────────────────────────────────────────────────
#  ERC1155 ABI (mínimo para aprobaciones)
# ─────────────────────────────────────────────────────────────────────────────

_ERC1155_ABI = [
    {"inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}],
     "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
     "name": "setApprovalForAll", "outputs": [],
     "stateMutability": "nonpayable", "type": "function"},
]


# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeSignal:
    """Señal de trading recibida desde calvin5.py vía Redis."""
    action:         str            # "BUY" | "SELL"
    signal_id:      str
    side:           str            # "UP" | "DOWN"
    token_id:       str
    price:          float          # precio de señal
    size_usd:       float          # tamaño en USDC
    market_slug:    str
    market_end_ts:  float
    timestamp:      str
    # Solo en SELL:
    position_id:    str   = ""
    size_tokens:    float = 0.0    # tokens a vender (Polymarket) o qty BTC (Binance)
    reason:         str   = ""     # motivo del cierre
    entry_price:    float = 0.0    # precio de entrada original
    stop_order_id:  str   = ""     # ID de la orden STOP_LOSS_LIMIT a cancelar (Binance)


@dataclass
class ExecutionReceipt:
    """Confirmación de ejecución publicada de vuelta a calvin5 y a execution:logs."""
    signal_id:       str
    action:          str            # "BUY" | "SELL"
    status:          str            # "FILLED" | "FAILED" | "SKIPPED"
    position_id:     str   = ""
    fill_price:      float = 0.0
    tokens_received: float = 0.0    # en BUY: tokens recibidos; en SELL: USDC recibidos
    size_usd:        float = 0.0
    slippage_pct:    float = 0.0
    fees_usd:        float = 0.0
    latency_ms:      float = 0.0
    stop_order_id:   str   = ""    # ID del STOP_LOSS_LIMIT colocado (Binance) — pasado de vuelta a la estrategia
    error:           str   = ""
    timestamp:       str   = ""

    def to_json(self) -> str:
        d = asdict(self)
        d["timestamp"] = datetime.utcnow().isoformat()
        return json.dumps(d, ensure_ascii=False)


# ─────────────────────────────────────────────────────────────────────────────
#  INTERFAZ DE EXCHANGE (patrón CCXT-like para intercambiabilidad)
# ─────────────────────────────────────────────────────────────────────────────

class PolymarketCLOBExchange:
    """
    Wrapper sobre py-clob-client que sigue la interfaz conceptual de CCXT.
    Para cambiar a otro exchange (ej. Binance con CCXT), solo hay que
    reemplazar esta clase manteniendo los métodos públicos.

    Métodos públicos:
      initialize()        → inicializa cliente y aprobaciones
      fetch_balance()     → USDC disponible en CLOB
      create_buy_order()  → BUY FOK
      create_sell_order() → SELL FOK con reintentos
      close()             → limpieza (no-op aquí)
    """

    def __init__(self):
        self._client = None
        self._initialized = False
        self._loop = None

    def initialize(self) -> None:
        """Inicializa el cliente CLOB y verifica aprobaciones ERC1155."""
        if DRY_RUN:
            log.info("[CLOB] DRY RUN — sin conexión real al exchange")
            self._initialized = True
            return

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            client = ClobClient(
                CLOB_API,
                key=_PRIVATE_KEY,
                chain_id=_CHAIN_ID,
                signature_type=1,
                funder=_WALLET_ADDR,
            )

            if _CLOB_API_KEY and _CLOB_SECRET and _CLOB_PASS:
                client.set_api_creds(ApiCreds(
                    api_key=_CLOB_API_KEY,
                    api_secret=_CLOB_SECRET,
                    api_passphrase=_CLOB_PASS,
                ))
                log.info("[CLOB] Credenciales cargadas desde .env")
            else:
                creds = client.create_or_derive_api_creds()
                client.set_api_creds(creds)
                log.info("[CLOB] Credenciales derivadas desde wallet")

            self._client = client
            short = f"{_WALLET_ADDR[:10]}...{_WALLET_ADDR[-4:]}"
            log.info(f"[CLOB] LIVE conectado | wallet {short}")

            # Allowances USDC y tokens
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            try:
                client.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
                log.info("[CLOB] Allowance USDC OK")
            except Exception as ae:
                log.warning(f"[CLOB] Allowance USDC warning: {ae}")

            try:
                client.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL))
                log.info("[CLOB] Allowance tokens OK")
            except Exception as ae:
                log.warning(f"[CLOB] Allowance tokens warning: {ae}")

            self._ensure_approvals()
            self._initialized = True

        except Exception as exc:
            log.error(f"[CLOB] ERROR inicializando: {exc}")
            raise

    def _ensure_approvals(self) -> None:
        """Verifica y setea setApprovalForAll ERC1155 on-chain en Polygon."""
        if not _PRIVATE_KEY or not _WALLET_ADDR:
            return
        try:
            from web3 import Web3
            from eth_account import Account

            w3 = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={"timeout": 15}))
            if not w3.is_connected():
                log.warning("[APPROVAL] No se pudo conectar a Polygon RPC")
                return

            account   = Account.from_key(_PRIVATE_KEY)
            wallet    = Web3.to_checksum_address(_WALLET_ADDR)
            ctf       = w3.eth.contract(
                address=Web3.to_checksum_address(_CTF_TOKENS),
                abi=_ERC1155_ABI
            )

            for raw_addr, label in [
                (_CTF_EXCHANGE,      "CTF Exchange"),
                (_NEG_RISK_EXCHANGE, "NegRisk Exchange"),
            ]:
                operator = Web3.to_checksum_address(raw_addr)
                if ctf.functions.isApprovedForAll(wallet, operator).call():
                    log.info(f"[APPROVAL] {label}: ya aprobado ✓")
                    continue
                log.warning(f"[APPROVAL] {label}: enviando setApprovalForAll...")
                nonce     = w3.eth.get_transaction_count(account.address)
                tx        = ctf.functions.setApprovalForAll(operator, True).build_transaction({
                    "from": account.address, "nonce": nonce,
                    "gas": 80_000, "gasPrice": w3.eth.gas_price, "chainId": _CHAIN_ID,
                })
                signed    = account.sign_transaction(tx)
                tx_hash   = w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt   = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
                if receipt.status == 1:
                    log.info(f"[APPROVAL] {label}: aprobado ✓ tx={tx_hash.hex()[:16]}")
                else:
                    log.warning(f"[APPROVAL] {label}: tx fallida")
        except Exception as exc:
            log.warning(f"[APPROVAL] Error ERC1155: {exc}")

    async def fetch_balance(self) -> float:
        """Retorna el balance USDC disponible en el CLOB."""
        if DRY_RUN or self._client is None:
            return 9999.0  # DRY RUN: balance ilimitado

        loop = asyncio.get_running_loop()
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            await loop.run_in_executor(
                None, lambda: self._client.update_balance_allowance(params))
            data = await loop.run_in_executor(
                None, lambda: self._client.get_balance_allowance(params))
            raw = float(data.get("balance", 0) or 0)
            return raw / 1e6 if raw > 1000 else raw
        except Exception as exc:
            log.warning(f"[CLOB] Error consultando balance USDC: {exc}")
            return 0.0

    async def create_buy_order(
        self,
        token_id: str,
        price: float,
        size_usd: float,
    ) -> Tuple[float, float]:
        """
        BUY FOK en Polymarket CLOB.
        Retorna (tokens_recibidos, precio_real_fill).
        (0.0, price) si no hubo fill o slippage excesivo.
        """
        if DRY_RUN or self._client is None:
            # O14: Simular latencia realista de fill
            latency_ms = random.randint(DRY_LATENCY_MIN_MS, DRY_LATENCY_MAX_MS)
            await asyncio.sleep(latency_ms / 1000)
            # O14: Simular rechazo FOK con probabilidad DRY_FOK_REJECT_RATE
            if random.random() < DRY_FOK_REJECT_RATE:
                log.info(f"[CLOB][DRY] BUY rechazado (FOK simulado): token={token_id[:16]} price={price:.4f}")
                return 0.0, price
            # O14: Simular slippage adverso en precio de fill
            slip = random.uniform(0, DRY_SLIPPAGE_MAX)
            fill_price = round(min(price * (1 + slip), 0.97), 4)
            tokens = round(size_usd / fill_price, 2)
            log.info(f"[CLOB][DRY] BUY simulado: token={token_id[:16]} price={fill_price:.4f} "
                     f"(slip={slip:.1%}) tokens={tokens:.2f} lat={latency_ms}ms")
            return tokens, fill_price

        if MICRO_TEST and size_usd < 2.0:
            log.warning(
                f"[CLOB] MICRO_TEST: orden de ${size_usd:.2f} — puede fallar si "
                f"liquidez insuficiente para tokens mínimos ({size_usd/price:.2f} tokens estimados)"
            )

        loop      = asyncio.get_running_loop()
        t_start   = time.time()

        try:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            fok_limit  = round(min(price + FOK_PRICE_BUF, 0.97), 4)
            order_args = MarketOrderArgs(
                token_id=token_id, amount=size_usd, side=BUY,
                price=fok_limit, order_type=OrderType.FOK,
            )
            signed = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(order_args))
            resp = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed, orderType=OrderType.FOK))

            latency_ms = (time.time() - t_start) * 1000
            status     = resp.get("status", "")
            taking     = resp.get("takingAmount", "")  # tokens recibidos
            making     = resp.get("makingAmount", "")  # USDC pagado

            if status == "matched" and taking and float(taking) > 0:
                actual_tokens = float(taking)
                actual_price = float(making) / actual_tokens if making and float(making) > 0 else price

                if actual_tokens < MIN_SELL_TOKENS - 0.01:
                    # Calvin5 debería haber bloqueado esta compra (tokens < MIN_TOKENS_BUY).
                    # Si llegó aquí igual (ej. MICRO_TEST o slippage en fill), reportar FILLED
                    # con los tokens reales — Calvin5 decide si mantener o no.
                    log.warning(
                        f"[CLOB] BUY fill con tokens bajos: {actual_tokens:.4f} < {MIN_SELL_TOKENS} min "
                        f"— TP/SL no podrán ejecutarse, posición irá a resolución"
                    )
                    return actual_tokens, actual_price
                slip_pct = abs(actual_price - price) / price

                if slip_pct > SLIP_MAX_PCT:
                    log.warning(
                        f"[CLOB] BUY fill con slippage alto: {slip_pct*100:.2f}% > máx "
                        f"{SLIP_MAX_PCT*100:.1f}% (señal={price:.4f} fill={actual_price:.4f}) "
                        f"— reportando FILLED para evitar posición fantasma"
                    )
                    return actual_tokens, actual_price  # NO retornar 0.0: el fill ya ocurrió en Polymarket

                log.info(
                    f"[CLOB] BUY FILLED token={token_id[:16]} señal={price:.4f} "
                    f"fill={actual_price:.4f} tokens={actual_tokens:.2f} "
                    f"usdc={making} latencia={latency_ms:.0f}ms"
                )
                return actual_tokens, actual_price

            log.warning(f"[CLOB] BUY FOK sin fill (status={status})")
            return 0.0, price

        except Exception as exc:
            exc_str = str(exc).lower()
            # Polymarket a veces reporta error pero SÍ ejecuta — verificar trades recientes
            if "couldn't be fully filled" in exc_str or "fok" in exc_str:
                try:
                    trade_resp = requests.get(
                        f"{CLOB_API}/trades",
                        params={"asset_id": token_id, "limit": 3},
                        timeout=5,
                    )
                    trades = trade_resp.json().get("data", []) if trade_resp.ok else []
                    for t in trades:
                        trade_ts = float(t.get("timestamp", "0")) / 1000
                        if time.time() - trade_ts < 30:
                            actual_tokens = float(t.get("size", 0))
                            actual_price  = float(t.get("price", price))
                            if actual_tokens > 0:
                                log.warning(
                                    f"[CLOB] BUY: CLOB error pero trade real detectado "
                                    f"tokens={actual_tokens:.4f} fill={actual_price:.4f}"
                                    + (" (tokens bajos — TP/SL no disponibles)" if actual_tokens < MIN_SELL_TOKENS else "")
                                )
                                return actual_tokens, actual_price  # siempre reportar fill real
                except Exception as te:
                    log.warning(f"[CLOB] No se pudo verificar trades post-FOK: {te}")
                log.warning(f"[CLOB] BUY FOK sin liquidez token={token_id[:16]}")
            else:
                log.error(f"[CLOB] BUY error: {exc}")
            return 0.0, price

    async def create_sell_order(
        self,
        token_id: str,
        price: float,
        size_tokens: float,
        current_prices: Optional[Dict[str, float]] = None,
        side_key: str = "",
        max_retries: int = 3,
        entry_price: float = 0.0,
    ) -> bool:
        """
        SELL FOK en Polymarket CLOB con reintentos y refresco de balance.
        entry_price: precio de entrada original para calcular floor del last-resort (C2).
        Retorna True si la orden se ejecutó (o era innecesaria), False si falló.
        """
        if DRY_RUN or self._client is None:
            # O14: Simular latencia realista de fill
            latency_ms = random.randint(DRY_LATENCY_MIN_MS, DRY_LATENCY_MAX_MS)
            await asyncio.sleep(latency_ms / 1000)
            slip = random.uniform(-DRY_SLIPPAGE_MAX, 0)  # slippage adverso en SELL (precio baja)
            fill_price = round(max(price * (1 + slip), 0.01), 4)
            log.info(f"[CLOB][DRY] SELL simulado: token={token_id[:16]} tokens={size_tokens:.4f} "
                     f"price={fill_price:.4f} (slip={slip:.1%}) lat={latency_ms}ms")
            return True

        if size_tokens < MIN_SELL_TOKENS:
            log.error(
                f"[CLOB] SELL RECHAZADO: {size_tokens:.2f} tokens < mínimo {MIN_SELL_TOKENS} "
                f"— posición no debería haber sido abierta con tokens insuficientes"
            )
            return False  # no mentir: la posición sigue abierta en Polymarket

        loop = asyncio.get_running_loop()

        from py_clob_client.clob_types import (
            MarketOrderArgs, OrderType, BalanceAllowanceParams, AssetType)
        from py_clob_client.order_builder.constants import SELL

        # Refrescar balance real
        sell_size     = size_tokens
        using_tracked = False
        try:
            bal_params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL, token_id=token_id)
            await loop.run_in_executor(
                None, lambda: self._client.update_balance_allowance(bal_params))
            bal_data = await loop.run_in_executor(
                None, lambda: self._client.get_balance_allowance(bal_params))
            raw_bal    = float(bal_data.get("balance", 0) or 0)
            actual_bal = raw_bal / 1e6 if raw_bal > 1000 else raw_bal
            log.info(
                f"[CLOB] Balance: raw={raw_bal:.0f} → {actual_bal:.6f} tokens "
                f"| tracked={size_tokens:.4f}"
            )
            if actual_bal < MIN_SELL_TOKENS - 0.01:
                if size_tokens >= MIN_SELL_TOKENS:
                    log.warning(f"[CLOB] Balance API=0 pero tracked={size_tokens:.4f} — lag de caché")
                    using_tracked = True
                else:
                    log.warning("[CLOB] SELL omitido: balance insuficiente")
                    return True
            else:
                sell_size = min(size_tokens, actual_bal)
        except Exception as be:
            log.warning(f"[CLOB] No se pudo verificar balance antes de SELL: {be}")

        # Reintentos con precio más agresivo en cada intento
        for attempt in range(1, max_retries + 1):
            try:
                mkt_price  = (current_prices or {}).get(side_key, price)
                # Bajar 0.04 por intento (antes 0.01) — llega antes a precio de mercado real
                sell_price = round(max(mkt_price - 0.04 * attempt, 0.01), 4)

                order_args = MarketOrderArgs(
                    token_id=token_id, amount=sell_size, side=SELL,
                    price=sell_price, order_type=OrderType.FOK,
                )
                signed = await loop.run_in_executor(
                    None, lambda: self._client.create_market_order(order_args))
                resp = await loop.run_in_executor(
                    None, lambda: self._client.post_order(signed, orderType=OrderType.FOK))

                sell_status = resp.get("status", "")
                sell_taking = resp.get("takingAmount", "")

                if sell_status == "matched" and sell_taking and float(sell_taking) > 0:
                    log.info(
                        f"[CLOB] SELL FILLED intento={attempt} "
                        f"token={token_id[:16]} price={sell_price} "
                        f"tokens={sell_size:.2f} usdc={sell_taking}"
                    )
                    return True

                log.warning(
                    f"[CLOB] SELL FOK sin fill (status={sell_status}) "
                    f"intento={attempt} price={sell_price}"
                )

            except Exception as exc:
                exc_str = str(exc).lower()
                wait    = attempt * 0.5

                if "not enough balance" in exc_str or "allowance" in exc_str:
                    log.warning(
                        f"[CLOB] SELL intento {attempt}/{max_retries} "
                        f"balance/allowance insuficiente token={token_id[:16]}"
                    )
                    if attempt < max_retries:
                        try:
                            bal_params = BalanceAllowanceParams(
                                asset_type=AssetType.CONDITIONAL, token_id=token_id)
                            await loop.run_in_executor(
                                None, lambda: self._client.update_balance_allowance(bal_params))
                            bal_data = await loop.run_in_executor(
                                None, lambda: self._client.get_balance_allowance(bal_params))
                            raw_r = float(bal_data.get("balance", 0) or 0)
                            refreshed = raw_r / 1e6 if raw_r > 1000 else raw_r
                            if refreshed < MIN_SELL_TOKENS - 0.01 and using_tracked:
                                log.warning(
                                    f"[CLOB] Balance aún 0 (lag) — esperando "
                                    f"{attempt*3}s con tracked={sell_size:.4f}"
                                )
                                await asyncio.sleep(attempt * 3.0)
                                continue
                            sell_size = min(sell_size, refreshed) if refreshed >= MIN_SELL_TOKENS else sell_size
                        except Exception as rbe:
                            log.warning(f"[CLOB] Error refrescando balance: {rbe}")
                        await asyncio.sleep(wait)
                else:
                    log.warning(
                        f"[CLOB] SELL intento {attempt}/{max_retries} FALLIDO: {exc}"
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(wait)

        # ── Último recurso: venta a mercado con floor de seguridad (C2) ────────
        # Floor = max(entry_price × 0.40, 0.01) — nunca vender por menos del
        # 40% del precio de entrada. Evita ser front-run a precios irrisorios.
        last_resort_floor = round(max(entry_price * 0.40, 0.01), 4) if entry_price > 0 else 0.01
        log.warning(
            f"[CLOB] SELL último recurso (floor={last_resort_floor:.4f}) — "
            f"token={token_id[:16]} size={sell_size:.2f}"
        )
        try:
            order_args = MarketOrderArgs(
                token_id=token_id, amount=sell_size, side=SELL,
                price=last_resort_floor, order_type=OrderType.FOK,
            )
            signed = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(order_args))
            resp = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed, orderType=OrderType.FOK))
            sell_status = resp.get("status", "")
            sell_taking = resp.get("takingAmount", "")
            if sell_status == "matched" and sell_taking and float(sell_taking) > 0:
                log.warning(
                    f"[CLOB] SELL MERCADO FILLED token={token_id[:16]} "
                    f"usdc={sell_taking} (precio de mercado)"
                )
                return True
            log.error(f"[CLOB] SELL mercado sin fill (status={sell_status}) — sin liquidez")
        except Exception as exc:
            log.error(f"[CLOB] SELL mercado FALLIDO: {exc}")

        log.error(
            f"[CLOB] SELL FALLIDO DEFINITIVO tras {max_retries} intentos + mercado "
            f"— token {token_id[:16]}... size={sell_size:.4f} TOKENS EN WALLET"
        )
        return False

    async def close(self) -> None:
        """Limpieza de recursos. No-op para CLOB."""
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  FACTORY DE EXCHANGE — selecciona adaptador según EXCHANGE_ADAPTER env var
# ─────────────────────────────────────────────────────────────────────────────

def _create_exchange():
    """
    Instancia el adaptador de exchange correcto según EXCHANGE_ADAPTER.

    "polymarket" → PolymarketCLOBExchange (comportamiento actual)
    "binance"    → BinanceSpotExchange (Binance Spot Testnet/Mainnet)

    La interfaz pública es la misma en ambos (initialize, fetch_balance,
    create_buy_order, create_sell_order) por lo que execution_bot no
    necesita saber qué exchange está usando.
    """
    if EXCHANGE_ADAPTER == "binance":
        try:
            from binance_exchange import BinanceSpotExchange
            log.info("[FACTORY] Usando adaptador: BinanceSpotExchange")
            return BinanceSpotExchange()
        except ImportError as exc:
            log.critical(
                f"[FACTORY] No se puede importar BinanceSpotExchange: {exc}\n"
                f"Asegúrate de que binance_exchange.py esté en el directorio y "
                f"que ccxt esté instalado (pip install ccxt)"
            )
            raise
    else:
        log.info("[FACTORY] Usando adaptador: PolymarketCLOBExchange")
        return PolymarketCLOBExchange()


# ─────────────────────────────────────────────────────────────────────────────
#  GESTOR DE RIESGO LOCAL (Fat-Finger + Daily Limits + Balance Check)
# ─────────────────────────────────────────────────────────────────────────────

class RiskGuard:
    """
    Valida cada señal antes de enviarla al exchange.
    Bloquea la ejecución si se detecta un riesgo de capital.
    ISSUE #11: Usa Redis para contador diario atómico (evita race conditions).
    """

    def __init__(self, exchange: PolymarketCLOBExchange, redis_pub: Optional[aioredis.Redis] = None):
        self.exchange    = exchange
        self._redis_pub  = redis_pub
        self._daily_count = 0
        self._daily_date  = ""

    async def _get_daily_count_from_redis(self) -> int:
        """ISSUE #11: Obtiene el contador diario desde Redis (fuente única de verdad)."""
        if self._redis_pub is None:
            return self._daily_count  # Fallback en memoria si Redis no disponible
        try:
            today = _madrid_today_str()
            key = f"daily_trades:{today}"
            count_str = await self._redis_pub.get(key)
            return int(count_str) if count_str else 0
        except Exception:
            return self._daily_count

    async def _increment_daily_counter_in_redis(self) -> int:
        """ISSUE #11: Incrementa atmómicamente el contador diario en Redis."""
        if self._redis_pub is None:
            self._daily_count += 1
            return self._daily_count
        try:
            today = _madrid_today_str()
            key = f"daily_trades:{today}"
            count = await self._redis_pub.incr(key)
            # Expira después de 1 día + 1 hora (por DST/edge cases)
            await self._redis_pub.expire(key, 86400 + 3600)
            return count
        except Exception:
            self._daily_count += 1
            return self._daily_count

    async def validate_buy(self, signal: TradeSignal) -> Tuple[bool, str]:
        """
        Valida una señal BUY. Devuelve (ok, motivo_rechazo).
        """
        if MICRO_TEST:
            log.warning("[RISK] MICRO_TEST activo — balance y límites diarios ignorados")
            return True, ""

        # Fat-finger: límite de valor por orden
        if signal.size_usd > MAX_ORDER_VALUE_USD:
            return False, (
                f"FAT_FINGER: size_usd ${signal.size_usd:.2f} > "
                f"máx ${MAX_ORDER_VALUE_USD:.2f}"
            )

        # Daily trade limit — ISSUE #11: usar contador atómico de Redis
        daily_count = await self._get_daily_count_from_redis()
        if daily_count >= MAX_DAILY_TRADES:
            return False, (
                f"DAILY_LIMIT: {daily_count} trades hoy >= "
                f"límite {MAX_DAILY_TRADES}"
            )

        # Verificar balance USDC disponible
        balance = await self.exchange.fetch_balance()
        if balance < signal.size_usd:
            return False, (
                f"BALANCE: USDC disponible ${balance:.2f} < "
                f"orden ${signal.size_usd:.2f}"
            )

        return True, ""

    async def record_trade(self) -> None:
        """Registra una ejecución para el contador diario — ISSUE #11: atómico."""
        count = await self._increment_daily_counter_in_redis()
        log.debug(f"[RISK] Trade diario #{count}/{MAX_DAILY_TRADES}")


# ─────────────────────────────────────────────────────────────────────────────
#  MANEJADOR DE RATE LIMIT (backoff exponencial)
# ─────────────────────────────────────────────────────────────────────────────

class RateLimitHandler:
    """Detecta errores 429 y aplica backoff exponencial."""

    def __init__(self, base_wait_s: float = 2.0, max_wait_s: float = 60.0):
        self._base    = base_wait_s
        self._max     = max_wait_s
        self._current = base_wait_s
        self._hits    = 0

    async def handle_if_rate_limited(self, error_str: str) -> bool:
        """
        Si el error es RateLimit, espera con backoff y devuelve True.
        Si no es RateLimit, devuelve False.
        """
        if "429" not in error_str and "rate limit" not in error_str.lower():
            return False

        self._hits   += 1
        wait          = min(self._current * (2 ** (self._hits - 1)), self._max)
        self._current = wait
        log.warning(
            f"[RATE LIMIT] 429 detectado (hit #{self._hits}) — "
            f"backoff exponencial {wait:.1f}s"
        )
        await asyncio.sleep(wait)
        return True

    def reset(self) -> None:
        self._current = self._base
        self._hits    = 0


# ─────────────────────────────────────────────────────────────────────────────
#  CIRCUIT BREAKER (Clasificación → Contención → Reconciliación → Escalada)
# ─────────────────────────────────────────────────────────────────────────────

class ErrorSeverity(enum.Enum):
    LEVE    = "LEVE"     # Error de negocio suave — log, skip, continuar
    RED     = "RED"      # Error de infraestructura — pausar, reconciliar
    CRITICO = "CRITICO"  # Fallo grave — acción inmediata, escalar Watchdog


class CircuitBreaker:
    """
    Implementa el protocolo de 4 pasos de manejo de errores del Maestro de Ejecución.

    Estados:
      CLOSED    → operación normal
      OPEN      → error detectado, Calvin5 bloqueado, esperando reconciliación
      HALF_OPEN → reconciliación en curso con Polymarket (Golden Source)

    Clasificación de errores:
      LEVE    → Balance insuficiente, param inválido, FOK sin fill → ignorar
      RED     → Timeout, 502, WebSocket, conexión → pausar y reconciliar
      CRITICO → Watchdog caído, latencia extrema, RED repetidos → escalar
    """

    # Patrones de clasificación
    _LEVE_PATTERNS = (
        "balance", "insufficient", "fat_finger", "daily_limit",
        "fok sin fill", "slippage", "expirada", "skipped",
        "couldn't be fully filled", "not enough balance",
        "invalid param", "sin liquidez", "fill insuficiente",
    )
    _CRITICO_PATTERNS = (
        "watchdog_timeout", "redis_latency_critical",
        "kill switch", "critical_consecutive_errors",
    )
    _RED_PATTERNS = (
        "timeout", "timed out", "502", "503", "504", "500",
        "connection", "websocket", "network", "read timeout",
        "connect timeout", "ssl", "reset by peer", "connectionerror",
        "sell fallido definitivo", "remotedisconnected",
    )

    def __init__(self):
        self._state       = "CLOSED"
        self._open_reason = ""
        self._open_since  = 0.0
        self._red_count   = 0     # errores RED consecutivos

    @property
    def is_open(self) -> bool:
        return self._state != "CLOSED"

    def classify(self, error_str: str) -> ErrorSeverity:
        """Paso 1 — Triage: clasifica el error en LEVE / RED / CRITICO."""
        err = error_str.lower()
        if any(p in err for p in self._LEVE_PATTERNS):
            return ErrorSeverity.LEVE
        if any(p in err for p in self._CRITICO_PATTERNS):
            return ErrorSeverity.CRITICO
        if any(p in err for p in self._RED_PATTERNS):
            return ErrorSeverity.RED
        # Errores desconocidos → RED por defecto (principio de menor privilegio)
        return ErrorSeverity.RED

    def record_red(self) -> bool:
        """Registra error RED. Devuelve True si se cruzó el umbral → CRITICO."""
        self._red_count += 1
        return self._red_count >= CB_MAX_RED_ERRORS

    def open(self, reason: str, severity: ErrorSeverity) -> None:
        self._state       = "OPEN"
        self._open_reason = f"[{severity.value}] {reason}"
        self._open_since  = time.time()

    def set_half_open(self) -> None:
        self._state = "HALF_OPEN"

    def close(self) -> None:
        self._state       = "CLOSED"
        self._open_reason = ""
        self._red_count   = 0

    def status_dict(self) -> dict:
        return {
            "state":        self._state,
            "reason":       self._open_reason,
            "open_since_s": round(time.time() - self._open_since) if self._open_since else 0,
            "red_count":    self._red_count,
        }


# ─────────────────────────────────────────────────────────────────────────────
#  ROBOT MAESTRO DE EJECUCIÓN
# ─────────────────────────────────────────────────────────────────────────────

class ExecutionBot:
    """
    Orquestador principal del Robot de Ejecución.

    Ciclo de vida:
      1. Conectar a Redis y al CLOB de Polymarket
      2. En paralelo:
         a) Escuchar signals:trade → ejecutar → publicar receipts
         b) Publicar heartbeats a health:heartbeats cada 5s
    """

    def __init__(self):
        self.exchange      = _create_exchange()
        self.risk          = RiskGuard(self.exchange, redis_pub=None)  # ISSUE #11: será actualizado después
        self.rate_limiter  = RateLimitHandler()
        self._redis_pub:  Optional[aioredis.Redis]      = None
        self._redis_sub:  Optional[aioredis.Redis]      = None
        self._start_time  = time.time()
        self._total_fills = 0
        self._total_fails = 0
        self._last_prices: Dict[str, float] = {}

        # ── Estado de control de autoridad ────────────────────────────────
        # Cuando el Watchdog envía PAUSE, el executor deja de procesar señales
        # de Calvin5 pero SIGUE escuchando al Watchdog.
        # Solo el Watchdog puede enviar RESUME.
        self._paused:       bool  = False
        self._pause_reason: str   = ""

        # Posiciones abiertas trackeadas por el executor para CLOSE_ALL
        # {position_id: {"token_id":..., "size_tokens":..., "side":..., "price":...}}
        self._open_positions: Dict[str, dict] = {}

        # ── Circuit Breaker ────────────────────────────────────────────
        self.circuit_breaker      = CircuitBreaker()
        self._last_watchdog_ts    = time.time()  # última señal del Watchdog

        # Pub/sub fallback para Redis < 5.0 que no soporta Streams (XREADGROUP)
        self._use_pubsub: bool = False

        # Timestamp del último _dispatch_signal procesado (para stuck monitor)
        self._last_signal_ts: float = time.time()

    # ── Circuit Breaker: métodos del protocolo de 4 pasos ────────────────────

    async def _alert_watchdog(self, error_type: str, action_taken: str, severity: str) -> None:
        """Paso 4a — Escalada: publica alerta al Watchdog en alerts:executor."""
        if self._redis_pub is None:
            log.error("[CB] Sin Redis — no se pudo alertar al Watchdog")
            return
        alert = {
            "source":         "executor",
            "estado":         "ERROR",
            "tipo":           error_type[:120],
            "severidad":      severity,
            "accion_tomada":  action_taken,
            "open_positions": len(self._open_positions),
            "circuit_breaker": self.circuit_breaker.status_dict(),
            "timestamp":      datetime.utcnow().isoformat(),
        }
        try:
            await self._redis_pub.publish(CHANNEL_ALERTS, json.dumps(alert, ensure_ascii=False))
            log.error(f"[CB] Alerta enviada al Watchdog | tipo={error_type[:60]} | {action_taken[:80]}")
        except Exception as exc:
            log.error(f"[CB] Error publicando alerta al Watchdog: {exc}")

    async def _reconcile_with_polymarket(self) -> bool:
        """
        Paso 3 — Reconciliación con la Golden Source (Polymarket).

        Consulta Gamma API para el estado real del portfolio y lo compara
        con _open_positions. Elimina posiciones fantasma. Devuelve True si OK.
        """
        if DRY_RUN:
            log.info("[CB][DRY] Reconciliacion simulada con Polymarket — OK")
            return True

        if not _WALLET_ADDR:
            log.warning("[CB] WALLET_ADDR no configurado — reconciliacion saltada")
            return True

        log.info("[CB] Iniciando reconciliacion con Polymarket (Golden Source)...")
        self.circuit_breaker.set_half_open()

        try:
            loop = asyncio.get_running_loop()
            resp = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: requests.get(
                        f"{GAMMA_API}/positions",
                        params={"user": _WALLET_ADDR, "sizeThreshold": "0.1"},
                        timeout=10,
                    )
                ),
                timeout=CB_RECONCILE_TIMEOUT_S,
            )

            if not resp.ok:
                log.warning(f"[CB] Gamma API {resp.status_code} — reconciliacion parcial")
                return False

            raw = resp.json()
            real_positions = raw if isinstance(raw, list) else raw.get("data", [])

            real_token_ids: set = set()
            for pos in real_positions:
                size = float(pos.get("size", 0) or 0)
                if size > 0.1:
                    asset_id = pos.get("asset", {}).get("id") or pos.get("asset_id", "")
                    if asset_id:
                        real_token_ids.add(asset_id)

            tracked_token_ids = {v["token_id"] for v in self._open_positions.values()}
            ghost_positions   = tracked_token_ids - real_token_ids
            unknown_positions = real_token_ids - tracked_token_ids

            if ghost_positions:
                log.error(f"[CB] POSICIONES FANTASMA detectadas (en tracker, NO en Polymarket): {ghost_positions}")
                for pos_id, pos_data in list(self._open_positions.items()):
                    if pos_data["token_id"] in ghost_positions:
                        self._open_positions.pop(pos_id, None)
                        log.warning(f"[CB] Posicion fantasma eliminada del tracker: {pos_id}")

            if unknown_positions:
                log.warning(f"[CB] Posiciones en Polymarket NO trackeadas (considerar CLOSE_ALL): {unknown_positions}")

            log.info(
                f"[CB] Reconciliacion OK — "
                f"real={len(real_token_ids)} tracked={len(tracked_token_ids)} "
                f"fantasmas_eliminadas={len(ghost_positions)} desconocidas={len(unknown_positions)}"
            )
            return True

        except asyncio.TimeoutError:
            log.error(f"[CB] Timeout reconciliando con Polymarket ({CB_RECONCILE_TIMEOUT_S}s)")
            return False
        except Exception as exc:
            log.error(f"[CB] Error reconciliando con Polymarket: {exc}")
            return False

    async def _circuit_breaker_protocol(self, error_str: str, context: str = "") -> None:
        """
        Protocolo completo de 4 pasos del Circuit Breaker.
        Se invoca cuando se detecta un error RED o CRITICO en la ejecucion.

        Paso 1: Triage — clasificar severidad
        Paso 2: Contencion inmediata — pausar Calvin5, cancelar ordenes pendientes
        Paso 3: Reconciliacion — verificar estado real en Polymarket antes de reanudar
        Paso 4: Escalada — alertar Watchdog y bloquear hasta recibir RESUME
        """
        # ── Paso 1: Triage ──────────────────────────────────────────────────
        severity = self.circuit_breaker.classify(error_str)

        if severity == ErrorSeverity.LEVE:
            log.debug(f"[CB] Error LEVE — sin accion: {error_str[:80]}")
            return

        # Errores RED consecutivos escalan a CRITICO
        if severity == ErrorSeverity.RED:
            if self.circuit_breaker.record_red():
                severity = ErrorSeverity.CRITICO
                error_str = f"critical_consecutive_errors ({CB_MAX_RED_ERRORS} RED): {error_str}"
                log.error(f"[CB] Umbral RED cruzado ({CB_MAX_RED_ERRORS}) — escalando a CRITICO")

        log.error(
            f"[CB] ============ CIRCUIT BREAKER ACTIVADO ============\n"
            f"  Severidad : {severity.value}\n"
            f"  Error     : {error_str[:120]}\n"
            f"  Contexto  : {context}"
        )

        # ── Paso 2: Contencion inmediata ────────────────────────────────────
        self.circuit_breaker.open(error_str, severity)
        self._paused       = True
        self._pause_reason = f"CircuitBreaker [{severity.value}]: {error_str[:80]}"
        log.error(
            f"[CB] CONTENCION INMEDIATA — senales Calvin5 BLOQUEADAS\n"
            f"  Posiciones abiertas trackeadas: {len(self._open_positions)}"
        )

        # Intentar cancelar ordenes pendientes en el exchange (exchange-agnostic)
        cancel_ok = "N/A (DRY_RUN)"
        if not DRY_RUN:
            try:
                if EXCHANGE_ADAPTER == "binance" and hasattr(self.exchange, "cancel_all_orders"):
                    # Binance: método async nativo en BinanceSpotExchange
                    await asyncio.wait_for(
                        self.exchange.cancel_all_orders(),
                        timeout=8.0,
                    )
                elif hasattr(self.exchange, "_client") and self.exchange._client is not None:
                    # Polymarket: método sync en py-clob-client
                    loop = asyncio.get_running_loop()
                    await asyncio.wait_for(
                        loop.run_in_executor(None, self.exchange._client.cancel_all_orders),
                        timeout=8.0,
                    )
                cancel_ok = "OK"
                log.info("[CB] cancel_all_orders ejecutado correctamente")
            except asyncio.TimeoutError:
                cancel_ok = "TIMEOUT"
                log.warning("[CB] cancel_all_orders timeout — pueden quedar ordenes pendientes")
            except Exception as exc:
                cancel_ok = f"ERROR: {exc}"
                log.warning(f"[CB] cancel_all_orders error: {exc}")

        # ── Paso 3: Reconciliacion con Polymarket ────────────────────────────
        reconciled = await self._reconcile_with_polymarket()
        reconcile_status = "OK" if reconciled else "FAILED"

        # ── Paso 4: Escalada al Watchdog ─────────────────────────────────────
        action_taken = (
            f"PAUSED Calvin5 signals | "
            f"cancel_all_orders={cancel_ok} | "
            f"reconcile={reconcile_status} | "
            f"open_positions={len(self._open_positions)}"
        )
        await self._alert_watchdog(
            error_type=error_str[:100],
            action_taken=action_taken,
            severity=severity.value,
        )

        log.error(
            f"[CB] Bot BLOQUEADO — esperando RESUME del Watchdog\n"
            f"  Canal de reanudacion: '{CHANNEL_EMERGENCY}' (source=watchdog, command=RESUME)"
        )

    async def _watchdog_monitor_loop(self) -> None:
        """
        Monitorea la presencia del Watchdog.
        El Watchdog NO envía pings periódicos — solo habla en emergencias (PAUSE/KILL_SWITCH/RESUME).
        Por eso NO disparamos el Circuit Breaker por silencio; simplemente logueamos una advertencia.
        _last_watchdog_ts se actualiza en _handle_emergency_command() cuando el Watchdog habla.
        """
        await asyncio.sleep(CB_WATCHDOG_TIMEOUT_S)  # grace period al arrancar
        while True:
            await asyncio.sleep(60)
            silence_s = time.time() - self._last_watchdog_ts
            if silence_s > CB_WATCHDOG_TIMEOUT_S:
                log.warning(
                    f"[WD] Watchdog silencioso hace {silence_s/60:.1f}min — "
                    f"bot continúa activo (el watchdog solo habla en emergencias)"
                )
                # Resetear timestamp para evitar spam de warnings
                self._last_watchdog_ts = time.time()

    # ── Probe de latencia al arranque ─────────────────────────────────────────

    async def _run_latency_probe(self) -> None:
        """Mide latencias reales al arrancar: Redis, CLOB API y Polygon RPC.
        Solo informativo — no bloquea el arranque si algo falla."""
        log.info("=" * 65)
        log.info("[LATENCY PROBE] Midiendo latencias de conexión...")

        # ── Redis ──────────────────────────────────────────────────────────
        redis_times = []
        for _ in range(5):
            t0 = time.time()
            try:
                await self._redis_pub.ping()
                redis_times.append((time.time() - t0) * 1000)
            except Exception:
                pass
        if redis_times:
            log.info(
                f"[LATENCY] Redis ping (5x): "
                f"avg={sum(redis_times)/len(redis_times):.1f}ms  "
                f"min={min(redis_times):.1f}ms  "
                f"max={max(redis_times):.1f}ms"
            )
        else:
            log.warning("[LATENCY] Redis: sin respuesta")

        if DRY_RUN:
            log.info("[LATENCY PROBE] DRY RUN — omitiendo probe CLOB y RPC (sin credenciales reales)")
            log.info("=" * 65)
            return

        # ── CLOB API (Polymarket REST) ─────────────────────────────────────
        clob_url = os.getenv("CLOB_API_URL", "https://clob.polymarket.com")
        clob_times = []
        for _ in range(3):
            t0 = time.time()
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    lambda: __import__("requests").get(
                        f"{clob_url}/health", timeout=8
                    )
                )
                clob_times.append((time.time() - t0) * 1000)
            except Exception as exc:
                log.warning(f"[LATENCY] CLOB API error: {exc}")
                break
        if clob_times:
            log.info(
                f"[LATENCY] CLOB API (3x): "
                f"avg={sum(clob_times)/len(clob_times):.1f}ms  "
                f"min={min(clob_times):.1f}ms  "
                f"max={max(clob_times):.1f}ms"
            )

        # ── Polygon RPC ────────────────────────────────────────────────────
        polygon_rpc = os.getenv("POLYGON_RPC", "")
        if polygon_rpc:
            t0 = time.time()
            try:
                loop = asyncio.get_running_loop()
                connected = await loop.run_in_executor(
                    None,
                    lambda: __import__("requests").post(
                        polygon_rpc,
                        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
                        timeout=8
                    )
                )
                rpc_ms = (time.time() - t0) * 1000
                block_hex = connected.json().get("result", "0x0")
                block_num = int(block_hex, 16) if block_hex else 0
                log.info(
                    f"[LATENCY] Polygon RPC: {rpc_ms:.1f}ms  "
                    f"bloque_actual={block_num:,}"
                )
            except Exception as exc:
                log.warning(f"[LATENCY] Polygon RPC error: {exc}")
        else:
            log.info("[LATENCY] Polygon RPC: no configurado (POLYGON_RPC vacío)")

        log.info("=" * 65)

    # ── Conexión Redis ────────────────────────────────────────────────────────

    async def _connect_redis(self) -> None:
        log.info(f"Conectando a Redis: {REDIS_URL}")
        self._redis_pub     = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
        self._redis_sub     = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
        self._redis_signals = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
        self.risk._redis_pub = self._redis_pub  # ISSUE #11: Conectar Redis a RiskGuard para contador atómico
        await self._redis_pub.ping()
        log.info("Redis conectado ✓")
        await self._ensure_stream_group()

    async def _ensure_stream_group(self) -> None:
        """Crea el consumer group del stream de señales. Cae a pub/sub si Redis < 5.0."""
        try:
            info = await self._redis_pub.info()
            version = info.get("redis_version", "0.0.0")
            major = int(version.split(".")[0])
            if major < 5:
                log.warning(
                    f"[STREAM] Redis {version} no soporta Streams — "
                    f"usando pub/sub fallback en '{CHANNEL_SIGNALS}'"
                )
                self._use_pubsub = True
                return
        except Exception:
            pass

        # ISSUE #6: Verificar que AMBOS canales estén disponibles (Calvin5 publica a ambos)
        try:
            # Verificar stream
            await self._redis_pub.xinfo_stream(STREAM_SIGNALS)
            log.info(f"[STREAM] Stream '{STREAM_SIGNALS}' disponible ✓")
        except Exception as exc:
            if "no such key" in str(exc).lower():
                log.info(f"[STREAM] Stream '{STREAM_SIGNALS}' no existe aún (se creará en primer write)")
            else:
                log.warning(f"[STREAM] Error verificando stream: {exc}")

        try:
            # mkstream=True crea el stream si tampoco existe aún
            await self._redis_pub.xgroup_create(
                STREAM_SIGNALS, STREAM_GROUP, id="0", mkstream=True
            )
            log.info(f"[STREAM] Consumer group '{STREAM_GROUP}' creado en '{STREAM_SIGNALS}'")
        except Exception as exc:
            # BUSYGROUP = ya existe — normal tras restart
            if "BUSYGROUP" in str(exc):
                log.info(f"[STREAM] Consumer group '{STREAM_GROUP}' ya existe — OK")
            elif "unknown command" in str(exc).lower() or "ERR" in str(exc):
                log.warning(f"[STREAM] Streams no disponibles ({exc}) — usando pub/sub fallback")
                self._use_pubsub = True
            else:
                log.warning(f"[STREAM] No se pudo crear consumer group: {exc}")
        
        # ISSUE #6: Health check del pub/sub como backup
        try:
            await self._redis_pub.publish(f"{CHANNEL_SIGNALS}:health", "ping")
            log.info(f"[PUBSUB] Canal '{CHANNEL_SIGNALS}' disponible como backup ✓")
        except Exception as exc:
            log.warning(f"[PUBSUB] Error verificando canal: {exc}")
        
        # Inyectar cliente sync para el handler de logs unificados
        import redis as _redis_sync
        _unified_handler._redis_client = _redis_sync.Redis.from_url(
            REDIS_URL, decode_responses=True, socket_timeout=1
        )

    # ── Publicadores ──────────────────────────────────────────────────────────

    async def _publish_receipt(self, receipt: ExecutionReceipt) -> None:
        """Publica el recibo al canal de retorno (calvin5 escucha aquí)."""
        if self._redis_pub is None:
            return
        payload = receipt.to_json()
        try:
            # Canal específico para este signal_id → calvin5 lo espera
            await self._redis_pub.publish(
                f"{CHANNEL_RECEIPTS}:{receipt.signal_id}", payload)
            # Canal general para el optimizer y otros listeners
            await self._redis_pub.publish(CHANNEL_LOGS, payload)
            log.debug(
                f"[PUB] Receipt {receipt.signal_id} → "
                f"status={receipt.status} fill={receipt.fill_price:.4f}"
            )
        except Exception as exc:
            log.error(f"[PUB] Error publicando receipt: {exc}")

    async def _publish_signal_ack(self, signal_id: str) -> None:
        """ISSUE #1: Publica ACK inmediato de que la señal fue recibida."""
        if self._redis_pub is None:
            return
        try:
            ack_data = {
                "timestamp": str(time.time()),
                "executor": STREAM_CONSUMER,
            }
            # Stream ACK para que Calvin5 lo vea — garantía de entrega
            await self._redis_pub.xadd(
                f"ack:signals:{signal_id}",
                ack_data,
                maxlen=10,
                approximate=True
            )
            log.debug(f"[ACK] Signal ACK para {signal_id[:16]}")
        except Exception as exc:
            log.warning(f"[ACK] Error ACK: {exc}")

    async def _heartbeat_loop(self) -> None:
        """Publica latido cada HEARTBEAT_INTERVAL_S segundos (Pub/Sub legacy)."""
        exchange_ok = self.exchange._initialized
        while True:
            try:
                if self._redis_pub:
                    # Medir latencia de Redis como proxy de latencia de red
                    t0    = time.time()
                    await self._redis_pub.ping()
                    ms    = (time.time() - t0) * 1000

                    # Latencia alta en Redis → CRITICO
                    if ms > CB_REDIS_LATENCY_MS_MAX and not self.circuit_breaker.is_open:
                        log.warning(f"[HB] Latencia Redis CRITICA: {ms:.0f}ms > {CB_REDIS_LATENCY_MS_MAX}ms")
                        asyncio.create_task(self._circuit_breaker_protocol(
                            "redis_latency_critical",
                            context=f"Redis latency {ms:.0f}ms > {CB_REDIS_LATENCY_MS_MAX}ms",
                        ))

                    cb_status = self.circuit_breaker.status_dict()
                    status = "open" if self.circuit_breaker.is_open else ("online" if exchange_ok else "degraded")
                    hb = {
                        "bot":                "executor",
                        "status":             status,
                        "paused":             self._paused,
                        "exchange_connected": self.exchange._initialized,
                        "dry_run":            DRY_RUN,
                        "redis_latency_ms":   round(ms, 1),
                        "uptime_s":           round(time.time() - self._start_time),
                        "madrid_hour":        _madrid_now().hour,
                        "in_trading_hours":   _in_trading_hours(),
                        "total_fills":        self._total_fills,
                        "total_fails":        self._total_fails,
                        "circuit_breaker":    cb_status,
                        "timestamp":          datetime.utcnow().isoformat(),
                    }
                    await self._redis_pub.publish(CHANNEL_HEARTBEATS, json.dumps(hb))
                    log.debug(f"[HB] Heartbeat enviado — latencia Redis {ms:.1f}ms")
            except Exception as exc:
                log.warning(f"[HB] Error heartbeat: {exc}")

            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    async def _executor_heartbeat_stream_loop(self) -> None:
        """
        Publica heartbeats persistentes del executor.
        - Siempre actualiza la key 'state:executor:latest' (TTL=90s) para que watchdog la lea.
        - Intenta Redis Stream (at-least-once) si disponible; si no, pub/sub como fallback.
        """
        while True:
            try:
                if self._redis_pub:
                    # Consultar balance USDT (cada heartbeat)
                    try:
                        balance_usdt = await self.exchange.fetch_balance()
                    except Exception:
                        balance_usdt = None

                    hb_data = {
                        "timestamp":    str(time.time()),
                        "uptime_s":     str(round(time.time() - self._start_time)),
                        "status":       "online" if self.exchange._initialized else "degraded",
                        "paused":       "true" if self._paused else "false",
                        "pause_reason": self._pause_reason[:120] if self._pause_reason else "",
                        "circuit_open": "true" if self.circuit_breaker.is_open else "false",
                        "bot":          "executor",
                        "balance_usdt": str(round(balance_usdt, 2)) if balance_usdt is not None else "",
                    }

                    # Clave persistente: watchdog la lee con .get() para detectar ausencia
                    # TTL de 90s — si el executor no escribe en 90s, la key desaparece
                    await self._redis_pub.setex(
                        "state:executor:latest",
                        90,
                        json.dumps(hb_data),
                    )

                    # Intentar Stream primero; si falla (Redis < 5.0), usar pub/sub
                    try:
                        if not self._use_pubsub:
                            await self._redis_pub.xadd(
                                "heartbeats:executor",
                                hb_data,
                                maxlen=100,
                                approximate=True,
                            )
                            log.debug("[HB-STREAM] Heartbeat publicado a Stream y key persistente")
                        else:
                            raise Exception("pubsub_mode")
                    except Exception:
                        # Fallback pub/sub (Redis < 5.0 o error en xadd)
                        await self._redis_pub.publish(
                            "heartbeats:executor", json.dumps(hb_data)
                        )
                        log.debug("[HB-STREAM] Heartbeat publicado a pub/sub (fallback) y key persistente")

            except Exception as exc:
                log.warning(f"[HB-STREAM] Error publicando heartbeat: {exc}")

            await asyncio.sleep(HEARTBEAT_INTERVAL_S)

    async def _watchdog_heartbeat_check(self) -> None:
        """Monitorea heartbeats del watchdog desde Redis Stream (robusto)."""
        await asyncio.sleep(30)  # Grace period al iniciar
        last_seen_idx = "0"  # Leer desde el inicio del stream

        while True:
            try:
                if self._use_pubsub or self._redis_pub is None:
                    # Redis < 5.0 — Streams no disponibles, watchdog_monitor_loop cubre esto
                    await asyncio.sleep(15)
                    continue

                # Leer últimos heartbeats del watchdog (últimos 10 segundos)
                entries = await self._redis_pub.xrevrange(
                    "heartbeats:watchdog",
                    count=5  # últimas 5 entradas
                )

                if entries:
                    # entries es lista de tuplas: (id, {campos})
                    latest_id, latest_data = entries[0]
                    latest_ts = float(latest_data.get(b"timestamp", b"0").decode() or 0)
                    now = time.time()
                    silence_s = now - latest_ts

                    if silence_s > CB_WATCHDOG_TIMEOUT_S:
                        log.warning(
                            f"[WD-STREAM] Watchdog ausente {silence_s:.0f}s "
                            f"(umbral={CB_WATCHDOG_TIMEOUT_S}s)"
                        )
                        # Nota: NO disparamos CB, solo alertamos. El watchdog es independiente.
                        self._last_watchdog_ts = now  # Resetear para evitar spam
                    else:
                        # Watchdog está vivo
                        log.debug(f"[WD-STREAM] Watchdog OK — último HB hace {silence_s:.1f}s")
                        self._last_watchdog_ts = now
                else:
                    log.debug("[WD-STREAM] No hay heartbeats del watchdog aún en el stream")

            except Exception as exc:
                log.debug(f"[WD-STREAM] Error verificando watchdog: {exc}")

            await asyncio.sleep(15)  # Verificar cada 15 segundos

    # ── Procesamiento de señales ──────────────────────────────────────────────

    async def _process_buy(self, signal: TradeSignal) -> ExecutionReceipt:
        """Valida y ejecuta una señal BUY."""
        t_start = time.time()

        # Validación de riesgo
        ok, reason = await self.risk.validate_buy(signal)
        if not ok:
            log.warning(f"[RISK] BUY rechazado ({signal.signal_id}): {reason}")
            return ExecutionReceipt(
                signal_id=signal.signal_id, action="BUY",
                status="SKIPPED", position_id=signal.position_id,
                size_usd=signal.size_usd, error=reason,
            )

        # Ejecución con manejo de rate limit
        for retry in range(3):
            try:
                tokens, fill_price = await self.exchange.create_buy_order(
                    token_id=signal.token_id,
                    price=signal.price,
                    size_usd=signal.size_usd,
                )
                latency_ms = (time.time() - t_start) * 1000

                if tokens <= 0:
                    return ExecutionReceipt(
                        signal_id=signal.signal_id, action="BUY",
                        status="FAILED", position_id=signal.position_id,
                        size_usd=signal.size_usd, latency_ms=latency_ms,
                        error="FOK sin fill o slippage excesivo",
                    )

                slip_pct = abs(fill_price - signal.price) / signal.price * 100
                if slip_pct > SLIP_MAX_PCT * 100:
                    log.warning(
                        f"[SLIPPAGE] ALTO: {slip_pct:.1f}% > {SLIP_MAX_PCT*100:.0f}% "
                        f"(señal={signal.price:.4f} fill={fill_price:.4f})"
                    )
                    if not DRY_RUN and slip_pct > 5.0:
                        log.error("[SLIPPAGE] Slippage crítico en REAL — activando pausa temporal")
                        self._paused = True
                        self._pause_reason = f"Slippage {slip_pct:.1f}% > 5%"

                await self.risk.record_trade()  # ISSUE #11: await para atomicidad
                self._total_fills += 1

                # Actualizar precio de mercado para SELL retry
                self._last_prices[signal.side] = fill_price

                # ── Stop-Loss nativo en Binance ────────────────────────────
                # Tras cada BUY exitoso en Binance, colocamos un STOP_LOSS_LIMIT
                # automáticamente. El stop se cancela cuando la estrategia envía SELL.
                stop_order_id = ""
                if EXCHANGE_ADAPTER == "binance" and hasattr(self.exchange, "place_stop_loss"):
                    try:
                        stop_p, limit_p = self.exchange.compute_stop_prices(fill_price)
                        stop_order_id   = await self.exchange.place_stop_loss(
                            symbol      = signal.token_id,
                            qty         = tokens,
                            stop_price  = stop_p,
                            limit_price = limit_p,
                        )
                        if stop_order_id:
                            log.info(
                                f"[EXEC] Stop-loss nativo colocado: "
                                f"stop={stop_p:.2f} limit={limit_p:.2f} "
                                f"orderId={stop_order_id}"
                            )
                        else:
                            log.warning(
                                "[EXEC] Stop-loss nativo NO pudo colocarse — "
                                "la estrategia debe monitorear el SL manualmente"
                            )
                    except Exception as sl_exc:
                        log.error(f"[EXEC] Error colocando stop-loss: {sl_exc}")

                # Registrar posición para CLOSE_ALL del Watchdog
                if signal.position_id:
                    self._open_positions[signal.position_id] = {
                        "token_id":      signal.token_id,
                        "side":          signal.side,
                        "price":         fill_price,
                        "size_usd":      signal.size_usd,
                        "size_tokens":   tokens,
                        "market_slug":   signal.market_slug,
                        "market_end_ts": signal.market_end_ts,
                        "stop_order_id": stop_order_id,   # para cancelar en SELL / CLOSE_ALL
                    }

                return ExecutionReceipt(
                    signal_id=signal.signal_id, action="BUY",
                    status="FILLED", position_id=signal.position_id,
                    fill_price=fill_price, tokens_received=tokens,
                    size_usd=signal.size_usd, slippage_pct=slip_pct,
                    latency_ms=latency_ms,
                    stop_order_id=stop_order_id,   # devuelto a la estrategia
                )

            except Exception as exc:
                exc_str = str(exc)
                if await self.rate_limiter.handle_if_rate_limited(exc_str):
                    continue  # reintentar tras backoff exponencial
                log.error(f"[EXEC] Error en BUY intento {retry+1}: {exc}")
                self._total_fails += 1
                # Clasificar y activar Circuit Breaker si corresponde
                severity = self.circuit_breaker.classify(exc_str)
                if severity != ErrorSeverity.LEVE:
                    asyncio.create_task(self._circuit_breaker_protocol(
                        exc_str, context=f"BUY {signal.side} signal_id={signal.signal_id[:16]}"
                    ))
                return ExecutionReceipt(
                    signal_id=signal.signal_id, action="BUY",
                    status="FAILED", position_id=signal.position_id,
                    size_usd=signal.size_usd, error=exc_str[:200],
                )

        self._total_fails += 1
        return ExecutionReceipt(
            signal_id=signal.signal_id, action="BUY",
            status="FAILED", size_usd=signal.size_usd,
            error="Rate limit: reintentos agotados",
        )

    async def _process_sell(self, signal: TradeSignal) -> ExecutionReceipt:
        """Ejecuta una señal SELL."""
        t_start = time.time()

        # ── Cancelar stop-loss nativo antes de vender (solo Binance) ──────────
        # Si Binance ya ejecutó el stop por precio, cancel_order() lo detecta
        # (Unknown Order) y lo trata como éxito silencioso.
        if EXCHANGE_ADAPTER == "binance" and signal.stop_order_id and hasattr(self.exchange, "cancel_order"):
            try:
                cancelled = await self.exchange.cancel_order(
                    signal.stop_order_id, signal.token_id
                )
                log.info(
                    f"[EXEC] Pre-SELL cancel stop {signal.stop_order_id}: "
                    f"{'cancelado' if cancelled else 'fallo/ya-ejecutado'}"
                )
            except Exception as cancel_exc:
                log.warning(f"[EXEC] Error cancelando stop pre-SELL: {cancel_exc}")

        try:
            sold = await self.exchange.create_sell_order(
                token_id=signal.token_id,
                price=signal.price,
                size_tokens=signal.size_tokens,
                current_prices=self._last_prices,
                side_key=signal.side,
                entry_price=signal.entry_price,
            )
            latency_ms = (time.time() - t_start) * 1000

            if sold:
                self._total_fills += 1
                # Eliminar posición del tracking local
                self._open_positions.pop(signal.position_id, None)
                return ExecutionReceipt(
                    signal_id=signal.signal_id, action="SELL",
                    status="FILLED", position_id=signal.position_id,
                    fill_price=signal.price,
                    tokens_received=signal.size_tokens,
                    size_usd=signal.size_usd,
                    latency_ms=latency_ms,
                )
            else:
                self._total_fails += 1
                asyncio.create_task(self._circuit_breaker_protocol(
                    "sell fallido definitivo",
                    context=f"SELL pos={signal.position_id[:16]} tokens={signal.size_tokens:.4f}",
                ))
                return ExecutionReceipt(
                    signal_id=signal.signal_id, action="SELL",
                    status="FAILED", position_id=signal.position_id,
                    size_usd=signal.size_usd,
                    error="SELL FALLIDO DEFINITIVO",
                )

        except Exception as exc:
            exc_str = str(exc)
            await self.rate_limiter.handle_if_rate_limited(exc_str)
            log.error(f"[EXEC] Error en SELL: {exc}")
            self._total_fails += 1
            # Clasificar y activar Circuit Breaker si corresponde
            severity = self.circuit_breaker.classify(exc_str)
            if severity != ErrorSeverity.LEVE:
                asyncio.create_task(self._circuit_breaker_protocol(
                    exc_str, context=f"SELL {signal.side} signal_id={signal.signal_id[:16]}"
                ))
            return ExecutionReceipt(
                signal_id=signal.signal_id, action="SELL",
                status="FAILED", position_id=signal.position_id,
                error=exc_str[:200],
            )

    async def _dispatch_signal(self, raw_msg: str) -> None:
        """Procesa un mensaje JSON del canal signals:trade."""
        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError:
            log.error(f"[DISPATCH] Mensaje inválido (no es JSON): {raw_msg[:100]}")
            return

        try:
            signal = TradeSignal(
                action        = data["action"],
                signal_id     = data.get("signal_id", str(uuid.uuid4())),
                side          = data.get("side", ""),
                token_id      = data.get("token_id", ""),
                price         = float(data.get("price", 0)),
                size_usd      = float(data.get("size_usd", 0)),
                market_slug   = data.get("market_slug", ""),
                market_end_ts = float(data.get("market_end_ts", 0)),
                timestamp     = data.get("timestamp", ""),
                position_id   = data.get("position_id", ""),
                size_tokens   = float(data.get("size_tokens", 0)),
                reason        = data.get("reason", ""),
                entry_price   = float(data.get("entry_price", 0)),
                stop_order_id = data.get("stop_order_id", ""),  # Binance: ID del stop a cancelar
            )
        except (KeyError, ValueError) as exc:
            log.error(f"[DISPATCH] Señal malformada: {exc} — {data}")
            return

        # Actualizar timestamp de último mensaje recibido (para stuck monitor)
        self._last_signal_ts = time.time()

        # C3: Idempotencia real — rechazar señales ya procesadas (60s TTL)
        # Protege contra duplicados por: reconexión pub/sub, fallback stream→pubsub,
        # retransmisión tras timeout de Calvin5, y reinicios del executor.
        dedup_key = f"sig:processed:{signal.signal_id}"
        try:
            already = await self._redis_pub.set(dedup_key, "1", ex=60, nx=True)
            if already is None:
                # NX=True devuelve None si la key YA existía → señal duplicada
                log.warning(
                    f"[DEDUP] Señal duplicada descartada: {signal.signal_id[:20]} "
                    f"(ya procesada en los últimos 60s)"
                )
                return
        except Exception as dedup_exc:
            # Redis no disponible: continuar sin dedup (degraded mode)
            log.warning(f"[DEDUP] Redis dedup no disponible: {dedup_exc} — procesando sin dedup")

        # ISSUE #1: Publicar ACK INMEDIATO (antes de procesamiento)
        await self._publish_signal_ack(signal.signal_id)

        log.info(
            f"[DISPATCH] Señal recibida: {signal.action} {signal.side} "
            f"price={signal.price:.4f} size=${signal.size_usd:.2f} "
            f"id={signal.signal_id[:16]}"
        )

        # Aviso si la señal llega fuera del horario operativo de Madrid
        # (la responsabilidad de respetar el horario es de Calvin5, no del Executor)
        if not _in_trading_hours():
            log.warning(
                f"[DISPATCH] Señal recibida fuera de horario Madrid "
                f"({_madrid_now().hour}h) — ejecutando igualmente "
                f"(horario es responsabilidad de Calvin5)"
            )

        # Verificar frescura de la señal (máx 10s de retraso)
        age_s = time.time() - float(data.get("_ts_unix", time.time()))
        if age_s > 10:
            log.warning(
                f"[DISPATCH] Señal descartada por antigüedad: {age_s:.1f}s > 10s "
                f"(mercado puede haber cambiado)"
            )
            receipt = ExecutionReceipt(
                signal_id=signal.signal_id, action=signal.action,
                status="SKIPPED", error=f"Señal expirada ({age_s:.1f}s)")
            await self._publish_receipt(receipt)
            return

        # Ejecutar según acción
        if signal.action == "BUY":
            receipt = await self._process_buy(signal)
        elif signal.action == "SELL":
            receipt = await self._process_sell(signal)
        else:
            log.warning(f"[DISPATCH] Acción desconocida: {signal.action}")
            return

        # Publicar recibo de vuelta a calvin5 y a logs
        await self._publish_receipt(receipt)
        self.rate_limiter.reset()

        log.info(
            f"[DISPATCH] Completado: {signal.action} → status={receipt.status} "
            f"fill={receipt.fill_price:.4f} tokens={receipt.tokens_received:.4f} "
            f"latencia={receipt.latency_ms:.0f}ms"
        )

    # ── Manejador de comandos de emergencia del Watchdog ─────────────────────

    async def _handle_emergency_command(self, raw_msg: str) -> None:
        """
        Procesa comandos del Watchdog con AUTORIDAD SUPREMA.
        Estos comandos tienen prioridad sobre cualquier señal de Calvin5.

        CLOSE_ALL → cierra todas las posiciones abiertas inmediatamente
        PAUSE     → bloquea señales de Calvin5 (no cierra posiciones)
        RESUME    → desbloquea el bot (solo el Watchdog puede hacer esto)
        """
        try:
            cmd = json.loads(raw_msg)
        except json.JSONDecodeError:
            log.error(f"[EMERGENCY] Comando inválido: {raw_msg[:100]}")
            return

        command = cmd.get("command", "")
        reason  = cmd.get("reason", "Sin razón especificada")
        source  = cmd.get("source", "unknown")

        # Solo aceptar comandos del Watchdog
        if source != "watchdog" or cmd.get("priority") != "SUPREME":
            log.warning(f"[EMERGENCY] Comando rechazado — fuente no autorizada: {source}")
            return

        # Registrar contacto con el Watchdog (para el monitor de silencio)
        self._last_watchdog_ts = time.time()

        log.error(f"[EMERGENCY] COMANDO SUPREMO recibido: {command} — {reason}")

        if command == "CLOSE_ALL":
            log.error("[EMERGENCY] CLOSE_ALL: cerrando todas las posiciones abiertas...")
            if not self._open_positions:
                log.info("[EMERGENCY] No hay posiciones abiertas que cerrar")
            else:
                close_tasks = []
                for pos_id, pos_data in list(self._open_positions.items()):
                    sig_id = f"EMERGENCY_SELL_{pos_id}_{int(time.time()*1000)}"
                    signal = TradeSignal(
                        action="SELL", signal_id=sig_id,
                        side=pos_data.get("side", ""),
                        token_id=pos_data.get("token_id", ""),
                        price=pos_data.get("price", 0.5),
                        size_usd=pos_data.get("size_usd", 0),
                        market_slug=pos_data.get("market_slug", ""),
                        market_end_ts=pos_data.get("market_end_ts", 0),
                        timestamp=datetime.utcnow().isoformat(),
                        position_id=pos_id,
                        size_tokens=pos_data.get("size_tokens", 0),
                        reason="EMERGENCY_CLOSE_ALL",
                    )
                    close_tasks.append(self._process_sell(signal))
                results = await asyncio.gather(*close_tasks, return_exceptions=True)
                ok = sum(1 for r in results if isinstance(r, ExecutionReceipt) and r.status == "FILLED")
                log.error(f"[EMERGENCY] CLOSE_ALL completado: {ok}/{len(results)} posiciones cerradas")

            # Pausa automática tras CLOSE_ALL
            self._paused       = True
            self._pause_reason = f"Auto-pausa post CLOSE_ALL: {reason}"
            log.error("[EMERGENCY] Execution Bot PAUSADO tras CLOSE_ALL")

        elif command == "PAUSE":
            self._paused       = True
            self._pause_reason = reason
            log.error(f"[EMERGENCY] Execution Bot PAUSADO por Watchdog: {reason}")

        elif command == "RESUME":
            self._paused       = False
            self._pause_reason = ""
            self.circuit_breaker.close()   # Circuit Breaker vuelve a CLOSED
            log.info(f"[EMERGENCY] Execution Bot REANUDADO por Watchdog: {reason}")
            log.info("[CB] Circuit Breaker reseteado a CLOSED por RESUME del Watchdog")

        else:
            log.warning(f"[EMERGENCY] Comando desconocido: {command}")

    # ── Monitor de stuck ──────────────────────────────────────────────────────

    async def _stuck_monitor(self) -> None:
        """
        Detecta si execution_bot lleva demasiado tiempo sin procesar señales durante
        horario activo. Puede indicar que el pub/sub loop quedó colgado.
        """
        await asyncio.sleep(90)  # grace period al arrancar (dejar que el bot se suscriba)
        while True:
            await asyncio.sleep(30)
            if self._paused:
                continue
            if _in_trading_hours():
                silence_s = time.time() - self._last_signal_ts
                if silence_s > 120:
                    log.critical(
                        f"[STUCK] ExecutionBot sin señales en {silence_s:.0f}s durante horario de trading "
                        f"— posible stuck en pub/sub. Considera reiniciar el proceso."
                    )
                    # Alertar al watchdog para que tome acción
                    await self._alert_watchdog(
                        "executor_stuck",
                        f"no signals in {silence_s:.0f}s during trading hours",
                        "CRITICO",
                    )
                    # Reset para no spammear cada 30s
                    self._last_signal_ts = time.time()

    # ── Bucle de escucha de señales ───────────────────────────────────────────

    async def _signal_stream_loop(self) -> None:
        """
        Consume señales de Calvin5.
        - Redis >= 5.0: usa XREADGROUP (at-least-once, crash recovery)
        - Redis < 5.0:  cae a pub/sub en 'signals:trade' (fire-and-forget)
        """
        if self._use_pubsub:
            await self._signal_pubsub_loop()
            return

        # Al arrancar: primero reprocesar mensajes pendientes (crash recovery)
        pending_id = "0"  # "0" = mensajes pendientes sin ACK; ">" = nuevos
        while True:
            try:
                results = await self._redis_signals.xreadgroup(
                    groupname=STREAM_GROUP,
                    consumername=STREAM_CONSUMER,
                    streams={STREAM_SIGNALS: pending_id},
                    count=10,
                    block=2000,
                )
                if not results:
                    pending_id = ">"
                    continue

                for _stream, messages in results:
                    for msg_id, fields in messages:
                        raw = fields.get("payload") or fields.get("data", "")
                        if self._paused:
                            log.warning(
                                f"[STREAM] Señal IGNORADA (bot pausado): {self._pause_reason}"
                            )
                            await self._redis_signals.xack(STREAM_SIGNALS, STREAM_GROUP, msg_id)
                            continue
                        await self._dispatch_signal(raw)
                        await self._redis_signals.xack(STREAM_SIGNALS, STREAM_GROUP, msg_id)

                if pending_id == "0":
                    pending_id = ">"

            except asyncio.CancelledError:
                break
            except Exception as exc:
                err_str = str(exc).lower()
                if "unknown command" in err_str or "xreadgroup" in err_str or "wrongtype" in err_str:
                    log.warning(
                        f"[STREAM] XREADGROUP no soportado en runtime (Redis < 5.0) — "
                        f"cambiando a pub/sub permanentemente. Error: {exc}"
                    )
                    self._use_pubsub = True
                    await self._signal_pubsub_loop()
                    return
                log.error(f"[STREAM] Error en signal stream loop: {exc} — reintentando en 3s...")
                await asyncio.sleep(3)

    async def _signal_pubsub_loop(self) -> None:
        """
        Fallback pub/sub para Redis < 5.0 (sin Streams). Reconexión automática infinita.
        Rastrea tiempo del último mensaje para detectar si el canal quedó mudo.
        """
        while True:
            pubsub = None
            try:
                pubsub = self._redis_signals.pubsub()
                await pubsub.subscribe(CHANNEL_SIGNALS)
                log.info(f"[SUB] Suscrito a '{CHANNEL_SIGNALS}' (pub/sub fallback — Redis < 5.0)")
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    self._last_signal_ts = time.time()
                    if self._paused:
                        log.warning(f"[SUB] Señal IGNORADA (bot pausado): {self._pause_reason}")
                        continue
                    await self._dispatch_signal(message["data"])
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error(f"[SUB] Error en signal pubsub loop: {exc} — reconectando en 3s...")
                # Si llevamos >60s sin mensajes, forzar limpieza completa del pubsub
                if time.time() - self._last_signal_ts > 60:
                    log.warning("[SUB] 60s sin mensajes — forzando recreación de pubsub")
                if pubsub is not None:
                    try:
                        await pubsub.unsubscribe(CHANNEL_SIGNALS)
                        await pubsub.aclose()
                    except Exception:
                        pass
                await asyncio.sleep(3)

    async def _emergency_listener_loop(self) -> None:
        """
        Suscribe al canal pub/sub 'emergency:commands' (Watchdog — AUTORIDAD SUPREMA).
        Se mantiene como pub/sub porque los comandos de emergencia son síncronos
        e inmediatos; no necesitan persistencia (el Watchdog los reenvía si es necesario).
        """
        while True:
            try:
                pubsub = self._redis_sub.pubsub()
                await pubsub.subscribe(CHANNEL_EMERGENCY)
                log.info(f"[SUB] Suscrito a '{CHANNEL_EMERGENCY}' (Watchdog — emergencias)")

                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    await self._handle_emergency_command(message["data"])

            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.error(f"[SUB] Error en emergency listener: {exc} — reconectando en 3s...")
                await asyncio.sleep(3)

    # ── Alias de compatibilidad ────────────────────────────────────────────────
    async def _signal_listener_loop(self) -> None:
        """Alias mantenido; lanza los dos loops (stream + emergency) como subtareas."""
        await asyncio.gather(
            self._signal_stream_loop(),
            self._emergency_listener_loop(),
        )

    # ── Arranque ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Lanza el bot: conexiones + tareas paralelas."""
        log.info("=" * 65)
        log.info(f"ExecutionBot iniciando — DRY_RUN={DRY_RUN}")
        log.info(f"  MAX_ORDER_USD=${MAX_ORDER_VALUE_USD} | "
                 f"MAX_DAILY_TRADES={MAX_DAILY_TRADES} | "
                 f"SLIP_MAX={SLIP_MAX_PCT*100:.0f}%")
        log.info("=" * 65)

        # Inicializar exchange
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.exchange.initialize)

        # Si es Binance: cargar exchangeInfo de forma async (filtros de símbolo)
        if EXCHANGE_ADAPTER == "binance" and hasattr(self.exchange, "_async_init"):
            log.info("[INIT] Cargando exchangeInfo de Binance (filtros de símbolo)...")
            await self.exchange._async_init()

        # Conectar Redis
        await self._connect_redis()

        # Probe de latencia — mide Redis, CLOB API y Polygon RPC al arranque
        await self._run_latency_probe()

        # Lanzar tareas paralelas
        await asyncio.gather(
            self._signal_listener_loop(),
            self._heartbeat_loop(),                    # Pub/Sub legacy (compatibilidad)
            self._executor_heartbeat_stream_loop(),    # Heartbeat persistente (Stream + fallback pub/sub)
            self._watchdog_heartbeat_check(),          # Monitorea watchdog desde stream
            self._watchdog_monitor_loop(),             # Monitorea último comando del watchdog
            self._stuck_monitor(),                     # Detecta si el bot se cuelga sin procesar señales
        )

    async def shutdown(self) -> None:
        log.info("Cerrando ExecutionBot...")
        await self.exchange.close()
        if self._redis_pub:
            await self._redis_pub.aclose()
        if self._redis_sub:
            await self._redis_sub.aclose()
        if self._redis_signals:  # ISSUE #3: Cierre _redis_signals para evitar fugas
            await self._redis_signals.aclose()
        log.info("ExecutionBot detenido.")


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def _main() -> None:
    _validate_env_vars()  # ISSUE #4: Verificación temprana
    bot = ExecutionBot()
    try:
        await bot.run()
    except KeyboardInterrupt:
        log.info("Interrumpido por el usuario")
    finally:
        await bot.shutdown()


if __name__ == "__main__":
    from utils import configure_structlog
    configure_structlog("execution_bot", log_file="executor.log")
    asyncio.run(_main())
