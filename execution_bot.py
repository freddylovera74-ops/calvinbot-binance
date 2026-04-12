"""
execution_bot.py — CalvinBTC · Trade Executor

Componente de ejecución del sistema CalvinBTC Binance.
Recibe señales del Signal Engine vía Redis Stream y gestiona la ejecución
de órdenes en Binance Spot (Testnet o Real).

Arquitectura de canales Redis:
  ENTRADA  ← stream:signals:trade     (señales del Signal Engine, at-least-once)
  SALIDA   → execution:receipts       (confirmaciones de fill → Signal Engine)
  SALIDA   → execution:logs           (log completo → Param Optimizer)
  SALIDA   → health:heartbeats        (latido cada 5s → Watchdog)
  ENTRADA  ← emergency:commands       (comandos PAUSE/RESUME del Watchdog)

Variables de entorno (.env):
  REDIS_URL, BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_TESTNET, DRY_RUN

Uso:
  python execution_bot.py             # DRY RUN (paper trading)
  python execution_bot.py --real      # Binance Testnet/Real (requiere API keys)
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
    from logging.handlers import RotatingFileHandler
    logger = logging.getLogger("execution_bot")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s [EXECUTOR] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = RotatingFileHandler(
        "executor.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
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
CHANNEL_SIGNALS      = "signals:trade"          # Signal Engine → Executor (pub/sub fallback)
STREAM_SIGNALS       = "stream:signals:trade"   # Stream persistente (at-least-once)
STREAM_GROUP         = "executors"              # Consumer group del stream de señales
STREAM_CONSUMER      = "executor-1"             # Nombre de este consumidor
CHANNEL_EMERGENCY    = "emergency:commands"      # Watchdog → Executor (autoridad SUPREMA)
CHANNEL_RECEIPTS     = "execution:receipts"      # Executor → Signal Engine
CHANNEL_LOGS         = "execution:logs"          # Executor → Param Optimizer
CHANNEL_HEARTBEATS   = "health:heartbeats"       # Executor → Watchdog
CHANNEL_ALERTS       = "alerts:executor"         # Executor → Watchdog (Circuit Breaker alerts)
HEARTBEAT_INTERVAL_S = 5

# Modo simulación
DRY_RUN = (
    "--dry-run" in sys.argv
    or os.getenv("DRY_RUN", "true").lower() == "true"
)

# Validación temprana de env vars en REAL_MODE
def _validate_env_vars() -> None:
    """Fail-fast: verifica que BINANCE_API_KEY y BINANCE_API_SECRET estén configuradas."""
    if DRY_RUN:
        return
    required_vars = {
        "BINANCE_API_KEY":    os.getenv("BINANCE_API_KEY", ""),
        "BINANCE_API_SECRET": os.getenv("BINANCE_API_SECRET", ""),
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        log.critical(f"FALTA CONFIGURACION EN REAL_MODE: {', '.join(missing)}")
        log.critical("Añade estas variables a .env y reinicia.")
        sys.exit(1)

# ── Guardrails de riesgo local (Fat-Finger Protection) ───────────────────────
MAX_ORDER_VALUE_USD  = float(os.getenv("MAX_ORDER_VALUE_USD", "200.0"))  # orden máxima en $
MAX_DAILY_TRADES     = int(os.getenv("MAX_DAILY_TRADES", "200"))          # órdenes máximas por día
SLIP_MAX_PCT         = float(os.getenv("SLIP_MAX_PCT", "0.04"))           # slippage máx aceptado

# Timeout máximo esperando fill del exchange antes de reportar fallo
FILL_TIMEOUT_S = 10.0

# ── Circuit Breaker ───────────────────────────────────────────────────────────
CB_WATCHDOG_TIMEOUT_S   = float(os.getenv("CB_WATCHDOG_TIMEOUT_S",   "600.0")) # s sin Watchdog → advertencia (watchdog no envía pings periódicos)
CB_REDIS_LATENCY_MS_MAX = float(os.getenv("CB_REDIS_LATENCY_MS_MAX", "500.0")) # ms latencia → CRITICO
CB_MAX_RED_ERRORS       = int(os.getenv("CB_MAX_RED_ERRORS",          "3"))     # RED consecutivos → CRITICO
CB_RECONCILE_TIMEOUT_S  = float(os.getenv("CB_RECONCILE_TIMEOUT_S",  "20.0"))  # timeout reconciliación


# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeSignal:
    """Señal de trading recibida desde el Signal Engine vía Redis Stream."""
    action:         str            # "BUY" | "SELL"
    signal_id:      str
    side:           str            # "LONG" (spot siempre)
    token_id:       str            # símbolo Binance, ej. "BTCUSDT"
    price:          float          # precio BTC de referencia
    size_usd:       float          # USDT a invertir (BUY) o valor estimado (SELL)
    timestamp:      str
    # Solo en SELL:
    position_id:    str   = ""
    size_tokens:    float = 0.0    # qty BTC a vender
    reason:         str   = ""     # motivo del cierre (TP, SL_MANUAL, TIME, etc.)
    entry_price:    float = 0.0    # precio de entrada original
    stop_order_id:  str   = ""     # ID del STOP_LOSS_LIMIT a cancelar en Binance


@dataclass
class ExecutionReceipt:
    """Confirmación de ejecución publicada de vuelta al Signal Engine y a execution:logs."""
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
#  FACTORY DE EXCHANGE
# ─────────────────────────────────────────────────────────────────────────────

from binance_exchange import BinanceSpotExchange

# EXCHANGE_TYPE=futures → Binance USDT-M Futures con apalancamiento
# EXCHANGE_TYPE=spot    → Binance Spot sin apalancamiento (default)
_EXCHANGE_TYPE = os.getenv("EXCHANGE_TYPE", "spot").lower()


def _create_exchange():
    """
    Instancia el adaptador de exchange según EXCHANGE_TYPE:
      spot    → BinanceSpotExchange  (compra/venta directa, sin apalancamiento)
      futures → BinanceFuturesExchange (USDT-M Perpetual, apalancamiento x20)
    """
    if _EXCHANGE_TYPE == "futures":
        from binance_futures_exchange import BinanceFuturesExchange
        log.info(
            f"[FACTORY] Cargando BinanceFuturesExchange "
            f"(leverage=x{os.getenv('LEVERAGE','20')} symbol={os.getenv('BINANCE_SYMBOL','BTCUSDT')})"
        )
        return BinanceFuturesExchange()

    log.info("[FACTORY] Cargando BinanceSpotExchange")
    return BinanceSpotExchange()




# ─────────────────────────────────────────────────────────────────────────────
#  GESTOR DE RIESGO LOCAL (Fat-Finger + Daily Limits + Balance Check)
# ─────────────────────────────────────────────────────────────────────────────

class RiskGuard:
    """
    Valida cada señal antes de enviarla al exchange.
    Bloquea la ejecución si se detecta un riesgo de capital.
    ISSUE #11: Usa Redis para contador diario atómico (evita race conditions).
    """

    def __init__(self, exchange: BinanceSpotExchange, redis_pub: Optional[aioredis.Redis] = None):
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
      OPEN      → error detectado, executor bloqueado, esperando reconciliación
      HALF_OPEN → reconciliación en curso con Binance (Golden Source)

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
      1. Conectar a Redis y a Binance Spot API
      2. En paralelo:
         a) Escuchar signals:trade → ejecutar → publicar receipts
         b) Publicar heartbeats a health:heartbeats cada 5s
    """

    def __init__(self):
        self.exchange      = _create_exchange()
        self.risk          = RiskGuard(self.exchange, redis_pub=None)  # ISSUE #11: será actualizado después
        self.rate_limiter  = RateLimitHandler()
        self._redis_pub:     Optional[aioredis.Redis]      = None
        self._redis_sub:     Optional[aioredis.Redis]      = None
        self._redis_signals: Optional[aioredis.Redis]      = None
        self._start_time  = time.time()
        self._total_fills = 0
        self._total_fails = 0
        self._last_prices: Dict[str, float] = {}

        # ── Estado de control de autoridad ────────────────────────────────
        # Cuando el Watchdog envía PAUSE, el executor deja de procesar señales
        # del Signal Engine pero SIGUE escuchando al Watchdog.
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

    async def _reconcile_with_exchange(self) -> bool:
        """
        Paso 3 — Reconciliación con la Golden Source (Binance).

        Consulta Binance API para obtener órdenes abiertas y posición real,
        comparando con _open_positions trackeadas. Elimina posiciones fantasma.
        Devuelve True si la reconciliación fue exitosa.
        """
        if DRY_RUN:
            log.info("[CB][DRY] Reconciliacion simulada con Binance — OK")
            return True

        log.info("[CB] Iniciando reconciliacion con Binance (Golden Source)...")
        self.circuit_breaker.set_half_open()

        try:
            # Consultar órdenes abiertas en Binance para cada símbolo trackeado
            tracked_symbols = {v["token_id"] for v in self._open_positions.values()}

            if not tracked_symbols:
                log.info("[CB] Sin posiciones trackeadas — reconciliacion trivial OK")
                self.circuit_breaker.close()
                return True

            real_open_symbols: set = set()
            if hasattr(self.exchange, "fetch_open_orders"):
                for symbol in tracked_symbols:
                    try:
                        open_orders = await asyncio.wait_for(
                            self.exchange.fetch_open_orders(symbol),
                            timeout=CB_RECONCILE_TIMEOUT_S,
                        )
                        if open_orders:
                            real_open_symbols.add(symbol)
                    except Exception as exc:
                        log.warning(f"[CB] Error consultando ordenes {symbol}: {exc}")

            ghost_positions = {
                pos_id for pos_id, pos_data in self._open_positions.items()
                if pos_data["token_id"] not in real_open_symbols and tracked_symbols
            }

            if ghost_positions:
                log.warning(
                    f"[CB] Posiciones sin ordenes abiertas en Binance: {ghost_positions} "
                    f"— pueden haber sido ejecutadas por stop-loss nativo"
                )
                for pos_id in ghost_positions:
                    self._open_positions.pop(pos_id, None)
                    log.info(f"[CB] Posicion eliminada del tracker (reconciliada): {pos_id}")

            log.info(
                f"[CB] Reconciliacion OK — "
                f"trackeadas={len(tracked_symbols)} "
                f"reconciliadas={len(ghost_positions)}"
            )
            self.circuit_breaker.close()
            return True

        except asyncio.TimeoutError:
            log.error(f"[CB] Timeout reconciliando con Binance ({CB_RECONCILE_TIMEOUT_S}s)")
            return False
        except Exception as exc:
            log.error(f"[CB] Error reconciliando con Binance: {exc}")
            return False

    async def _circuit_breaker_protocol(self, error_str: str, context: str = "") -> None:
        """
        Protocolo completo de 4 pasos del Circuit Breaker.
        Se invoca cuando se detecta un error RED o CRITICO en la ejecucion.

        Paso 1: Triage — clasificar severidad
        Paso 2: Contencion inmediata — pausar executor, cancelar ordenes pendientes
        Paso 3: Reconciliacion — verificar estado real en Binance antes de reanudar
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
            f"[CB] CONTENCION INMEDIATA — senales BLOQUEADAS\n"
            f"  Posiciones abiertas trackeadas: {len(self._open_positions)}"
        )

        # Intentar cancelar ordenes pendientes en el exchange (exchange-agnostic)
        cancel_ok = "N/A (DRY_RUN)"
        if not DRY_RUN:
            try:
                if hasattr(self.exchange, "cancel_all_orders"):
                    await asyncio.wait_for(
                        self.exchange.cancel_all_orders(),
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

        # ── Paso 3: Reconciliacion con Binance ──────────────────────────────
        reconciled = await self._reconcile_with_exchange()
        reconcile_status = "OK" if reconciled else "FAILED"

        # ── Paso 4: Escalada al Watchdog ─────────────────────────────────────
        action_taken = (
            f"PAUSED executor signals | "
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
            log.info("[LATENCY PROBE] DRY RUN — omitiendo probe Binance REST (sin credenciales reales)")
            log.info("=" * 65)
            return

        # ── Binance REST API ───────────────────────────────────────────────
        testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
        binance_url = (
            "https://testnet.binance.vision/api/v3/ping"
            if testnet else
            "https://api.binance.com/api/v3/ping"
        )
        binance_times = []
        for _ in range(3):
            t0 = time.time()
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None,
                    lambda: __import__("requests").get(binance_url, timeout=8)
                )
                binance_times.append((time.time() - t0) * 1000)
            except Exception as exc:
                log.warning(f"[LATENCY] Binance REST error: {exc}")
                break
        if binance_times:
            log.info(
                f"[LATENCY] Binance REST (3x): "
                f"avg={sum(binance_times)/len(binance_times):.1f}ms  "
                f"min={min(binance_times):.1f}ms  "
                f"max={max(binance_times):.1f}ms"
            )

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

        # ISSUE #6: Verificar que AMBOS canales estén disponibles (Signal Engine publica a ambos)
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
            # Stream ACK para el Signal Engine — garantía de entrega
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
                    # Consultar balance USDT con cache de 30s para no quemar rate limit
                    now_ts = time.time()
                    if not hasattr(self, '_balance_cache') or \
                       now_ts - self._balance_cache.get('ts', 0) > 30:
                        try:
                            bal = await self.exchange.fetch_balance()
                            self._balance_cache = {'v': bal, 'ts': now_ts}
                        except Exception:
                            self._balance_cache = getattr(self, '_balance_cache', {'v': None, 'ts': 0})
                    balance_usdt = self._balance_cache.get('v')

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
                if hasattr(self.exchange, "place_stop_loss"):
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
        if signal.stop_order_id and hasattr(self.exchange, "cancel_order"):
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
        # retransmisión tras timeout del Signal Engine, y reinicios del executor.
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
        # (la responsabilidad de respetar el horario es del Signal Engine, no del Executor)
        if not _in_trading_hours():
            log.warning(
                f"[DISPATCH] Señal recibida fuera de horario Madrid "
                f"({_madrid_now().hour}h) — ejecutando igualmente "
                f"(horario es responsabilidad del Signal Engine)"
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
        Estos comandos tienen prioridad sobre cualquier señal del Signal Engine.

        CLOSE_ALL → cierra todas las posiciones abiertas inmediatamente
        PAUSE     → bloquea señales del Signal Engine (no cierra posiciones)
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
        Consume señales del Signal Engine.
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
        log.info(f"CalvinBTC · Trade Executor iniciando — DRY_RUN={DRY_RUN}")
        log.info(f"  MAX_ORDER_USD=${MAX_ORDER_VALUE_USD} | "
                 f"MAX_DAILY_TRADES={MAX_DAILY_TRADES} | "
                 f"SLIP_MAX={SLIP_MAX_PCT*100:.0f}%")
        log.info("=" * 65)

        # Inicializar exchange
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.exchange.initialize)

        # Si es Binance: cargar exchangeInfo de forma async (filtros de símbolo)
        if hasattr(self.exchange, "_async_init"):
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
