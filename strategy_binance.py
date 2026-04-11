"""
strategy_binance.py — Estrategia de Momentum BTC para Binance Spot Testnet.

Reemplaza la lógica de Polymarket de calvin5.py con una estrategia equivalente
adaptada a trading spot real. Mantiene los mismos conceptos (momentum BTC, TP/SL,
sizing, circuit breakers) pero opera sobre BTC/USDT en lugar de tokens binarios.

Diferencias clave vs calvin5.py:
  - Sin rondas de 5min (Polymarket) — opera en cualquier momento
  - Sin token_id, market_slug — usa símbolo "BTCUSDT"
  - Precios reales de BTC, no rango 0-1
  - TP/SL como % desde precio de entrada, no cents absolutos
  - SL delegado a Binance nativo (STOP_LOSS_LIMIT) vía execution_bot
  - Signal format compatible con execution_bot.py (mismos campos Redis)

Estrategia:
  1. Monitorear momentum BTC (via btc_price.py — ya funciona con Binance)
  2. Si momentum >= BTC_MIN_PCT en ventana BTC_WINDOW_S → señal LONG
  3. Esperar receipt del execution_bot con fill_price y stop_order_id
  4. Monitorear precio BTC para TP
  5. Cuando precio >= entry_price * (1 + TP_PCT) → enviar señal SELL

Uso:
    python strategy_binance.py          # DRY RUN (paper trading)
    python strategy_binance.py --real   # modo real (requiere BINANCE_API_KEY etc.)
"""

import asyncio
import csv
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import btc_price as bp
from metrics import metrics
import loss_tracker
from utils import madrid_now, in_trading_hours, write_json_atomic, send_telegram_async

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

# Exchange
SYMBOL           = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
BASE_ASSET       = SYMBOL.replace("USDT", "")  # "BTC"

# Sizing
STAKE_USD        = float(os.getenv("STAKE_USD_OVERRIDE", "50.0"))  # USDT por operación

# BTC Momentum — filtro de entrada
BTC_WINDOW_S     = int(os.getenv("BTC_WINDOW_S",   "30"))    # ventana momentum (s)
BTC_MIN_PCT      = float(os.getenv("BTC_MIN_PCT",  "0.10"))  # % mínimo de movimiento

# Take Profit (% desde precio de entrada)
TP_PCT           = float(os.getenv("TP_PCT",       "0.015")) # 1.5% TP principal
TP_PARTIAL_PCT   = float(os.getenv("TP_PARTIAL_PCT","0.008"))# 0.8% TP parcial (50%)

# Stop Loss — pasado a execution_bot para colocar STOP_LOSS_LIMIT nativo
SL_DROP_PCT      = float(os.getenv("SL_DROP_PCT",  "0.010")) # 1% debajo de entrada

# Tiempo máximo en posición (segundos) antes de cerrar a mercado
MAX_HOLD_S       = int(os.getenv("MAX_HOLD_S",    "300"))   # 5 minutos máximo

# Anti-entrada rápida: mínimo entre entradas consecutivas
THROTTLE_S       = int(os.getenv("THROTTLE_S",     "30"))   # 30s entre entradas

# Máximo de posiciones simultáneas
MAX_OPEN_POS     = int(os.getenv("MAX_OPEN_POS",   "1"))    # conservador

# Slippage máximo aceptado (para rechazar fills malos)
SLIP_MAX_PCT     = float(os.getenv("SLIP_MAX_PCT", "0.004")) # 0.4%

# Circuit breaker: pérdida máxima de sesión
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "50.0"))

# Horario de operación (hora Madrid)
TRADING_HOUR_START = int(os.getenv("TRADING_HOUR_START", "9"))
TRADING_HOUR_END   = int(os.getenv("TRADING_HOUR_END",   "22"))

# Scan interval
SCAN_INTERVAL_S  = float(os.getenv("SCAN_INTERVAL_S", "1.0"))

# Modo
DRY_RUN = "--real" not in sys.argv and os.getenv("DRY_RUN", "true").lower() == "true"

# Archivos de persistencia
TRADES_CSV       = "binance_trades.csv"
OPEN_POS_FILE    = "binance_open.json"

# Redis
REDIS_URL             = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CH_SIGNALS      = "signals:trade"
REDIS_STREAM_SIGNALS  = "stream:signals:trade"
REDIS_CH_RECEIPTS     = "execution:receipts"
REDIS_CH_HEARTBEATS   = "health:heartbeats"
REDIS_CH_UNIFIED_LOG  = "logs:unified"
EXEC_RECEIPT_TIMEOUT  = 10.0

# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Position:
    """Posición abierta en Binance Spot."""
    position_id:    str
    symbol:         str
    side:           str           # "LONG" (único en spot)
    entry_price:    float         # precio de fill del BUY
    size_usd:       float         # USDT invertido
    qty_base:       float         # BTC recibido en el BUY
    stop_order_id:  str           # ID de la orden STOP_LOSS_LIMIT en Binance
    tp_price:       float         # precio objetivo de Take Profit
    sl_price:       float         # precio de stop (informativo)
    opened_at:      float         # timestamp de apertura
    status:         str = "open"  # "open" | "closing"
    partial_tp_done: bool = False  # True si ya se tomó ganancia parcial


# ─────────────────────────────────────────────────────────────────────────────
#  ESTADO GLOBAL
# ─────────────────────────────────────────────────────────────────────────────

_redis:            Optional[aioredis.Redis] = None
_open_pos:         List[Position]           = []
_session_pnl:      float                   = 0.0    # PnL de la sesión actual
_last_entry_ts:    float                   = 0.0    # timestamp de última entrada
_entries_paused:   bool                    = False
_pending_receipts: Dict[str, asyncio.Future] = {}

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

import logging as _logging

_log = _logging.getLogger("strategy_binance")
_log.setLevel(_logging.DEBUG)
_fh  = _logging.FileHandler("strategy_binance.log", encoding="utf-8")
_fh.setFormatter(_logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S"))
_log.addHandler(_fh)


def _linfo(msg: str) -> None:
    _log.info(msg)
    print(msg)
    _publish_log_fire_forget(msg, "INFO")


def _lwarn(msg: str) -> None:
    _log.warning(msg)
    print(f"[WARN] {msg}")
    _publish_log_fire_forget(msg, "WARN")


def _lerr(msg: str) -> None:
    _log.error(msg)
    print(f"[ERROR] {msg}")
    _publish_log_fire_forget(msg, "ERROR")


def _publish_log_fire_forget(msg: str, level: str) -> None:
    """Publica al log unificado del dashboard. Fire-and-forget."""
    if _redis is None:
        return
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_publish_log(msg, level))
    except RuntimeError:
        pass


async def _publish_log(msg: str, level: str) -> None:
    if _redis is None:
        return
    try:
        await _redis.publish(REDIS_CH_UNIFIED_LOG, json.dumps({
            "bot":       "strategy_binance",
            "level":     level,
            "msg":       msg[:300],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  SEÑALES REDIS
# ─────────────────────────────────────────────────────────────────────────────

async def _send_signal(signal: dict) -> Optional[dict]:
    """
    Publica una señal al execution_bot vía Redis Stream y espera el receipt.

    La señal usa el mismo formato que calvin5.py para compatibilidad con
    execution_bot.py sin modificaciones.

    Campos del signal dict:
        action:       "BUY" | "SELL"
        signal_id:    UUID único
        side:         "LONG" (siempre para spot)
        token_id:     símbolo Binance (ej. "BTCUSDT")
        price:        precio BTC de referencia
        size_usd:     USDT a gastar (BUY) o valor estimado (SELL)
        market_slug:  "" (no aplica en Binance)
        market_end_ts: 0 (no aplica)
        position_id:  UUID de la posición
        size_tokens:  qty BTC a vender (solo SELL)
        reason:       motivo del cierre (TP, SL_MANUAL, TIME, etc.)
        entry_price:  precio de entrada (solo SELL)
    """
    global _redis

    if _redis is None:
        _lerr("[SIGNAL] Redis no conectado — no se puede enviar señal")
        return None

    signal_id = signal.get("signal_id", str(uuid.uuid4()))
    signal["signal_id"] = signal_id

    try:
        # Stream persistente (at-least-once delivery)
        await _redis.xadd(
            REDIS_STREAM_SIGNALS,
            {"payload": json.dumps(signal, ensure_ascii=False)},
            maxlen=1000,
            approximate=True,
        )
        # Pub/sub legacy para compatibilidad con execution_bot sin streams
        await _redis.publish(REDIS_CH_SIGNALS, json.dumps(signal, ensure_ascii=False))

        _linfo(
            f"[SIGNAL] {signal['action']} enviado → id={signal_id[:16]} "
            f"symbol={signal.get('token_id')} price={signal.get('price'):.2f}"
        )

    except Exception as exc:
        _lerr(f"[SIGNAL] Error publicando señal: {exc}")
        return None

    # Esperar receipt del execution_bot
    fut = asyncio.get_event_loop().create_future()
    _pending_receipts[signal_id] = fut

    try:
        receipt_raw = await asyncio.wait_for(fut, timeout=EXEC_RECEIPT_TIMEOUT)
        return json.loads(receipt_raw) if isinstance(receipt_raw, str) else receipt_raw
    except asyncio.TimeoutError:
        _lwarn(f"[SIGNAL] Timeout esperando receipt {signal_id[:16]} ({EXEC_RECEIPT_TIMEOUT}s)")
        return None
    finally:
        _pending_receipts.pop(signal_id, None)


async def _receipt_listener() -> None:
    """Escucha receipts del execution_bot y resuelve los futures pendientes."""
    global _redis

    sub = await aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    psub = sub.pubsub()

    # Suscribirse al canal de receipts con wildcard pattern
    await psub.psubscribe(f"{REDIS_CH_RECEIPTS}:*")
    _linfo("[RECEIPTS] Escuchando receipts del execution_bot...")

    async for msg in psub.listen():
        if msg["type"] not in ("pmessage", "message"):
            continue
        try:
            data     = json.loads(msg["data"])
            sig_id   = data.get("signal_id", "")
            fut      = _pending_receipts.get(sig_id)
            if fut and not fut.done():
                fut.set_result(data)
        except Exception as exc:
            _lwarn(f"[RECEIPTS] Error procesando receipt: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  LÓGICA DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def _check_entry_conditions() -> Tuple[bool, str, str]:
    """
    Verifica si las condiciones de entrada están dadas.

    Returns:
        (can_enter: bool, direction: str, reason: str)
        direction: "UP" si momentum positivo, "DOWN" si negativo (solo UP en spot)
    """
    global _open_pos, _session_pnl, _last_entry_ts, _entries_paused

    if _entries_paused:
        return False, "", "entradas pausadas por control externo"

    if len(_open_pos) >= MAX_OPEN_POS:
        return False, "", f"máx posiciones abiertas ({MAX_OPEN_POS})"

    if time.time() - _last_entry_ts < THROTTLE_S:
        remaining = THROTTLE_S - (time.time() - _last_entry_ts)
        return False, "", f"throttle activo ({remaining:.0f}s restantes)"

    if _session_pnl <= -DAILY_LOSS_LIMIT:
        return False, "", f"circuit breaker: PnL sesión ${_session_pnl:.2f}"

    if not in_trading_hours(TRADING_HOUR_START, TRADING_HOUR_END):
        return False, "", "fuera de horario de operación"

    # Verificar momentum BTC
    btc_price_current = bp.get_btc_price()
    if btc_price_current is None:
        return False, "", "sin precio BTC"

    mom_pct, direction = bp.get_momentum(window_s=BTC_WINDOW_S)

    if direction is None or abs(mom_pct) < BTC_MIN_PCT:
        return False, "", f"momentum insuficiente ({mom_pct:+.3f}% < {BTC_MIN_PCT:.3f}%)"

    # En Binance Spot solo podemos operar LONG (comprar y vender)
    # Para DOWN necesitaríamos futuros. Aquí solo operamos UP.
    if direction != "UP":
        return False, "", f"momentum DOWN={mom_pct:.3f}% — spot solo opera LONG"

    return True, direction, f"momentum {mom_pct:+.3f}% en {BTC_WINDOW_S}s"


# ─────────────────────────────────────────────────────────────────────────────
#  APERTURA DE POSICIÓN
# ─────────────────────────────────────────────────────────────────────────────

async def _open_position() -> None:
    """Intenta abrir una posición LONG en BTC."""
    global _open_pos, _last_entry_ts

    can_enter, direction, reason = _check_entry_conditions()
    if not can_enter:
        return

    btc_price = bp.get_btc_price()
    if btc_price is None:
        return

    pos_id = str(uuid.uuid4())[:16]

    signal = {
        "action":        "BUY",
        "signal_id":     str(uuid.uuid4()),
        "side":          "LONG",
        "token_id":      SYMBOL,         # execution_bot lo usa como símbolo
        "price":         btc_price,
        "size_usd":      STAKE_USD,
        "market_slug":   "",              # no aplica en Binance
        "market_end_ts": 0,              # no aplica
        "position_id":   pos_id,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }

    _linfo(
        f"[ENTRY] LONG {SYMBOL} | precio={btc_price:.2f} "
        f"stake=${STAKE_USD:.2f} | {reason}"
    )
    _last_entry_ts = time.time()

    receipt = await _send_signal(signal)

    if receipt is None or receipt.get("status") != "FILLED":
        status  = receipt.get("status", "TIMEOUT") if receipt else "TIMEOUT"
        err_msg = receipt.get("error", "") if receipt else ""
        _lwarn(f"[ENTRY] BUY rechazado/fallido: {status} {err_msg}")
        return

    # Extraer datos del fill
    fill_price    = float(receipt.get("fill_price", btc_price))
    qty_btc       = float(receipt.get("tokens_received", 0.0))
    stop_order_id = receipt.get("stop_order_id", "")

    if qty_btc <= 0:
        _lerr(f"[ENTRY] Receipt FILLED pero qty_btc=0 — revisar execution_bot")
        return

    slip_pct = abs(fill_price - btc_price) / btc_price
    if slip_pct > SLIP_MAX_PCT:
        _lwarn(f"[ENTRY] Slippage alto: {slip_pct:.3%} (señal={btc_price:.2f} fill={fill_price:.2f})")

    # Calcular niveles de TP
    tp_price = fill_price * (1 + TP_PCT)
    sl_price = fill_price * (1 - SL_DROP_PCT)

    pos = Position(
        position_id   = pos_id,
        symbol        = SYMBOL,
        side          = "LONG",
        entry_price   = fill_price,
        size_usd      = STAKE_USD,
        qty_base      = qty_btc,
        stop_order_id = stop_order_id,
        tp_price      = tp_price,
        sl_price      = sl_price,
        opened_at     = time.time(),
    )
    _open_pos.append(pos)

    _linfo(
        f"[ENTRY] Posición abierta: {pos_id} "
        f"entry={fill_price:.2f} qty={qty_btc:.6f}BTC "
        f"TP={tp_price:.2f} SL={sl_price:.2f} "
        f"stopOrderId={stop_order_id or 'manual'}"
    )

    _save_open_positions()
    _log_trade_csv("OPEN", pos, fill_price, 0.0, "ENTRY")

    if stop_order_id:
        _linfo(f"[ENTRY] Stop-loss nativo activo en Binance: orderId={stop_order_id}")
    else:
        _lwarn(
            f"[ENTRY] Sin stop-loss nativo — monitoring manual activo. "
            f"Verificar que EXCHANGE_ADAPTER=binance en .env"
        )

    metrics.set_open_positions(len(_open_pos))


# ─────────────────────────────────────────────────────────────────────────────
#  GESTIÓN DE POSICIONES ABIERTAS
# ─────────────────────────────────────────────────────────────────────────────

async def _manage_positions() -> None:
    """
    Revisa todas las posiciones abiertas y ejecuta TP o cierre por tiempo.

    El SL es manejado por Binance nativo (STOP_LOSS_LIMIT). Sin embargo,
    este monitor también detecta si el SL fue disparado verificando el precio
    actual vs sl_price, y actualiza el tracker si es necesario.
    """
    global _open_pos, _session_pnl

    btc_price = bp.get_btc_price()
    if btc_price is None:
        return

    now = time.time()

    for pos in list(_open_pos):
        if pos.status == "closing":
            continue

        holding_s = now - pos.opened_at
        pnl_pct   = (btc_price - pos.entry_price) / pos.entry_price

        # ── Verificar si SL fue disparado por Binance ────────────────────────
        # Si precio < sl_price, el SL de Binance debería haberse ejecutado.
        # Lo detectamos aquí y limpiamos el tracker.
        if btc_price < pos.sl_price and pos.stop_order_id:
            _lwarn(
                f"[MANAGE] SL detectado en pos {pos.position_id}: "
                f"precio={btc_price:.2f} < sl={pos.sl_price:.2f} "
                f"— Binance debería haber ejecutado orderId={pos.stop_order_id}"
            )
            # El cierre ya lo gestionó Binance — solo actualizar tracker
            pnl_usd = pos.size_usd * pnl_pct
            await _close_position_local(pos, btc_price, "SL_BINANCE", pnl_usd)
            continue

        # ── Take Profit parcial ──────────────────────────────────────────────
        if not pos.partial_tp_done and pnl_pct >= TP_PARTIAL_PCT:
            _linfo(
                f"[TP PARCIAL] pos={pos.position_id} precio={btc_price:.2f} "
                f"pnl={pnl_pct:.3%} >= {TP_PARTIAL_PCT:.3%} — vendiendo 50%"
            )
            qty_sell = pos.qty_base * 0.5  # 50% parcial
            await _execute_sell(pos, btc_price, qty_sell, "TP_PARTIAL")
            pos.partial_tp_done = True
            pos.qty_base       -= qty_sell
            continue

        # ── Take Profit completo ─────────────────────────────────────────────
        if pnl_pct >= TP_PCT:
            _linfo(
                f"[TP FULL] pos={pos.position_id} precio={btc_price:.2f} "
                f"pnl={pnl_pct:.3%} >= {TP_PCT:.3%}"
            )
            await _execute_sell(pos, btc_price, pos.qty_base, "TP_FULL")
            continue

        # ── Cierre por tiempo máximo ─────────────────────────────────────────
        if holding_s >= MAX_HOLD_S:
            _lwarn(
                f"[TIME EXIT] pos={pos.position_id} holding={holding_s:.0f}s >= {MAX_HOLD_S}s "
                f"precio={btc_price:.2f} pnl={pnl_pct:.3%} — cerrando a mercado"
            )
            await _execute_sell(pos, btc_price, pos.qty_base, "TIME_EXIT")
            continue

        # ── Log periódico ────────────────────────────────────────────────────
        if int(holding_s) % 30 == 0 and holding_s > 0:
            _linfo(
                f"[POS] {pos.position_id[:8]} holding={holding_s:.0f}s "
                f"precio={btc_price:.2f} pnl={pnl_pct:+.3%} "
                f"TP={pos.tp_price:.2f} SL={pos.sl_price:.2f}"
            )


async def _execute_sell(
    pos: Position,
    current_price: float,
    qty_to_sell: float,
    reason: str,
) -> None:
    """Envía señal SELL al execution_bot y procesa el resultado."""
    global _open_pos, _session_pnl

    pos.status = "closing"

    signal = {
        "action":        "SELL",
        "signal_id":     str(uuid.uuid4()),
        "side":          "LONG",
        "token_id":      pos.symbol,
        "price":         current_price,
        "size_usd":      pos.size_usd,
        "market_slug":   "",
        "market_end_ts": 0,
        "position_id":   pos.position_id,
        "size_tokens":   qty_to_sell,     # qty BTC a vender (campo reutilizado)
        "reason":        reason,
        "entry_price":   pos.entry_price,
        "stop_order_id": pos.stop_order_id,  # para que executor cancele el stop
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }

    receipt = await _send_signal(signal)

    if receipt is None or receipt.get("status") not in ("FILLED", "SKIPPED"):
        status = receipt.get("status", "TIMEOUT") if receipt else "TIMEOUT"
        _lerr(
            f"[SELL] FALLIDO pos={pos.position_id} reason={reason} "
            f"status={status} — reintentando en próximo ciclo"
        )
        pos.status = "open"  # volver a estado abierto para reintentar
        return

    # Calcular PnL real del fill
    fill_price = float(receipt.get("fill_price", current_price))
    pnl_pct    = (fill_price - pos.entry_price) / pos.entry_price
    pnl_usd    = pos.size_usd * pnl_pct  # aproximado (qty_to_sell puede ser parcial)

    if qty_to_sell < pos.qty_base:
        # PnL proporcional en cierre parcial
        pnl_usd = (qty_to_sell / pos.qty_base) * pos.size_usd * pnl_pct

    await _close_position_local(pos, fill_price, reason, pnl_usd)


async def _close_position_local(
    pos: Position,
    exit_price: float,
    reason: str,
    pnl_usd: float,
) -> None:
    """Actualiza el tracker local tras el cierre de una posición."""
    global _open_pos, _session_pnl

    if pos in _open_pos:
        _open_pos.remove(pos)

    _session_pnl += pnl_usd
    loss_tracker.record_pnl(pnl_usd)

    pnl_pct = (exit_price - pos.entry_price) / pos.entry_price

    _linfo(
        f"[CLOSE] {pos.position_id[:8]} reason={reason} "
        f"entry={pos.entry_price:.2f} exit={exit_price:.2f} "
        f"pnl={pnl_pct:+.3%} ({pnl_usd:+.2f}$) "
        f"session={_session_pnl:+.2f}$"
    )

    _log_trade_csv("CLOSE", pos, exit_price, pnl_usd, reason)
    _save_open_positions()

    mode = "DRY" if DRY_RUN else "REAL"
    result = "WIN" if pnl_usd >= 0 else "LOSS"
    metrics.inc_trade(mode=mode, side="LONG", result=result)
    metrics.set_pnl(mode=mode, value=_session_pnl)
    metrics.set_open_positions(len(_open_pos))

    # Alerta Telegram en pérdidas significativas
    if pnl_usd < -10.0:
        await send_telegram_async(
            f"LOSS ${pnl_usd:.2f} | {pos.position_id[:8]} | {reason}\n"
            f"Entry={pos.entry_price:.2f} Exit={exit_price:.2f} "
            f"PnL={pnl_pct:+.3%}",
            level="WARNING",
            prefix_label="BinanceBot",
        )


# ─────────────────────────────────────────────────────────────────────────────
#  PERSISTENCIA
# ─────────────────────────────────────────────────────────────────────────────

def _save_open_positions() -> None:
    """Guarda posiciones abiertas a disco para sobrevivir reinicios."""
    data = [asdict(p) for p in _open_pos]
    try:
        write_json_atomic(OPEN_POS_FILE, data)
    except Exception as exc:
        _lwarn(f"[PERSIST] Error guardando posiciones: {exc}")


def _load_open_positions() -> None:
    """Carga posiciones abiertas desde disco al reiniciar."""
    global _open_pos
    path = Path(OPEN_POS_FILE)
    if not path.exists():
        return
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        _open_pos = [Position(**p) for p in raw]
        if _open_pos:
            _lwarn(
                f"[PERSIST] {len(_open_pos)} posiciones cargadas desde disco — "
                f"VERIFICAR MANUALMENTE que siguen abiertas en Binance"
            )
    except Exception as exc:
        _lwarn(f"[PERSIST] Error cargando posiciones: {exc}")


def _log_trade_csv(
    event:  str,
    pos:    Position,
    price:  float,
    pnl:    float,
    reason: str,
) -> None:
    """Registra el evento en el CSV de trades."""
    file_exists = Path(TRADES_CSV).exists()
    try:
        with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow([
                    "timestamp", "event", "position_id", "symbol", "side",
                    "entry_price", "exit_price", "qty_btc", "size_usd", "pnl_usd", "reason",
                ])
            writer.writerow([
                datetime.now(timezone.utc).isoformat(),
                event,
                pos.position_id[:16],
                pos.symbol,
                pos.side,
                f"{pos.entry_price:.2f}",
                f"{price:.2f}",
                f"{pos.qty_base:.6f}",
                f"{pos.size_usd:.2f}",
                f"{pnl:.2f}",
                reason,
            ])
    except Exception as exc:
        _lwarn(f"[CSV] Error escribiendo trade: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  HEARTBEAT
# ─────────────────────────────────────────────────────────────────────────────

async def _heartbeat_loop() -> None:
    """Publica heartbeat cada 5 segundos para watchdog y dashboard."""
    global _redis
    while True:
        try:
            if _redis:
                btc = bp.get_btc_price()
                hb = {
                    "bot":           "strategy_binance",
                    "status":        "online",
                    "dry_run":       DRY_RUN,
                    "open_positions": len(_open_pos),
                    "session_pnl":   round(_session_pnl, 2),
                    "btc_price":     round(btc, 2) if btc else None,
                    "entries_paused": _entries_paused,
                    "timestamp":     datetime.now(timezone.utc).isoformat(),
                }
                await _redis.publish(REDIS_CH_HEARTBEATS, json.dumps(hb))
        except Exception:
            pass
        await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def _main_loop() -> None:
    """Loop principal: scan → entrada → gestión de posiciones."""
    _linfo(f"[MAIN] Iniciando strategy_binance | DRY_RUN={DRY_RUN} | symbol={SYMBOL}")
    _linfo(f"[MAIN] TP={TP_PCT:.1%} SL={SL_DROP_PCT:.1%} STAKE=${STAKE_USD:.2f}")

    while True:
        try:
            # Verificar circuit breaker de loss_tracker
            lt_status, lt_msg = loss_tracker.check()
            if lt_status == loss_tracker.STATUS_BREACHED:
                _lerr(f"[LOSS TRACKER] {lt_msg} — ESTRATEGIA DETENIDA")
                await asyncio.sleep(60)
                continue
            elif lt_status == loss_tracker.STATUS_WARNING:
                _lwarn(f"[LOSS TRACKER] {lt_msg}")

            # Intentar abrir nueva posición
            await _open_position()

            # Gestionar posiciones existentes
            await _manage_positions()

        except Exception as exc:
            _lerr(f"[MAIN] Error inesperado: {exc}")

        await asyncio.sleep(SCAN_INTERVAL_S)


async def main() -> None:
    """Punto de entrada principal."""
    global _redis

    _linfo("=" * 60)
    mode_label = "DRY RUN (paper trading)" if DRY_RUN else "REAL MONEY"
    _linfo(f"  CalvinBot — Binance Spot Testnet Strategy [{mode_label}]")
    _linfo(f"  Símbolo: {SYMBOL}  |  Stake: ${STAKE_USD:.2f}  |  Max pos: {MAX_OPEN_POS}")
    _linfo("=" * 60)

    if not DRY_RUN:
        _lwarn("MODO REAL ACTIVADO — las órdenes se ejecutarán en Binance Testnet real")

    # Conectar Redis
    _redis = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    await _redis.ping()
    _linfo(f"[REDIS] Conectado: {REDIS_URL}")

    # Cargar posiciones persistidas
    _load_open_positions()

    # Iniciar tareas paralelas
    await asyncio.gather(
        bp.run_btc_poller(),
        _receipt_listener(),
        _heartbeat_loop(),
        _main_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _linfo("[MAIN] Estrategia detenida por usuario")
        _save_open_positions()
