"""
strategy_binance.py — Signal Engine de Momentum BTC para Binance Spot.

Detecta movimientos fuertes de precio en BTC/USDT y genera señales de compra
con gestión de Take Profit, Stop Loss nativo y cierre por tiempo máximo.

Estrategia:
  1. Monitorear momentum BTC via WebSocket (btc_price.py)
  2. Si momentum >= BTC_MIN_PCT en ventana BTC_WINDOW_S → señal LONG
  3. Esperar receipt del execution_bot con fill_price y stop_order_id
  4. Monitorear precio BTC para TP parcial y TP completo
  5. Cuando precio >= entry * (1 + TP_PCT) → enviar señal SELL

Uso:
    python strategy_binance.py          # DRY RUN (paper trading)
    python strategy_binance.py --real   # modo real (requiere BINANCE_API_KEY)
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
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
from binance_exchange import BinanceSpotExchange

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN (defaults del entorno — el optimizer puede sobreescribirlos)
# ─────────────────────────────────────────────────────────────────────────────

SYMBOL           = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
BASE_ASSET       = SYMBOL.replace("USDT", "")

# Sizing — defaults riesgo 7.5/10
STAKE_USD        = float(os.getenv("STAKE_USD_OVERRIDE", "75.0"))
BTC_WINDOW_S     = int(os.getenv("BTC_WINDOW_S",   "30"))    # ventana momentum (s)
BTC_MIN_PCT      = float(os.getenv("BTC_MIN_PCT",  "0.04"))  # 0.04% en 30s — equilibrio señal/ruido
TP_PCT           = float(os.getenv("TP_PCT",       "0.020"))
TP_PARTIAL_PCT   = float(os.getenv("TP_PARTIAL_PCT","0.010"))
SL_DROP_PCT      = float(os.getenv("SL_DROP_PCT",  "0.008"))
MAX_HOLD_S       = int(os.getenv("MAX_HOLD_S",    "240"))
THROTTLE_S       = int(os.getenv("THROTTLE_S",     "15"))
MAX_OPEN_POS     = int(os.getenv("MAX_OPEN_POS",   "2"))
SLIP_MAX_PCT       = float(os.getenv("SLIP_MAX_PCT",      "0.002"))  # rechazar si slippage > 0.2%
DAILY_LOSS_LIMIT   = float(os.getenv("DAILY_LOSS_LIMIT",  "100.0"))
ENTRY_GRACE_S      = int(os.getenv("ENTRY_GRACE_S",       "15"))    # segundos sin revisar SL tras abrir
SHORT_BLOCK_MOM    = float(os.getenv("SHORT_BLOCK_MOM",   "0.5"))   # bloquear SHORT si mom > +0.5%
HOURLY_LOSS_LIMIT  = float(os.getenv("HOURLY_LOSS_LIMIT", "80.0"))  # pérdida máx en 1h → auto-pause
NIGHT_STAKE_USD    = float(os.getenv("NIGHT_STAKE_USD",   "20.0"))  # stake nocturno
NIGHT_HOUR_START   = int(os.getenv("NIGHT_HOUR_START",    "22"))    # inicio modo nocturno (hora Madrid)
NIGHT_HOUR_END     = int(os.getenv("NIGHT_HOUR_END",       "9"))    # fin modo nocturno

REDIS_KEY_PARAMS     = "config:strategy_binance:current"
TRADING_HOUR_START   = int(os.getenv("TRADING_HOUR_START", "9"))
TRADING_HOUR_END     = int(os.getenv("TRADING_HOUR_END",   "22"))
# Pon DISABLE_TRADING_HOURS=true en .env para operar 24/7 (ideal en testnet)
DISABLE_TRADING_HOURS = os.getenv("DISABLE_TRADING_HOURS", "false").lower() == "true"
SCAN_INTERVAL_S      = float(os.getenv("SCAN_INTERVAL_S", "1.0"))
DRY_RUN              = "--real" not in sys.argv and os.getenv("DRY_RUN", "true").lower() == "true"
TRADES_CSV           = "binance_trades.csv"
OPEN_POS_FILE        = "binance_open.json"
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CH_SIGNALS     = "signals:trade"
REDIS_STREAM_SIGNALS = "stream:signals:trade"
REDIS_CH_RECEIPTS    = "execution:receipts"
REDIS_CH_HEARTBEATS  = "health:heartbeats"
REDIS_CH_UNIFIED_LOG = "logs:unified"
EXEC_RECEIPT_TIMEOUT = 10.0
PARAM_RELOAD_INTERVAL = 60.0

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

_log = logging.getLogger("strategy_binance")
_log.setLevel(logging.DEBUG)
_fh  = RotatingFileHandler(
    "strategy_binance.log", maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                   datefmt="%Y-%m-%d %H:%M:%S"))
_log.addHandler(_fh)

# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Position:
    """Posición abierta en Binance Spot."""
    position_id:     str
    symbol:          str
    side:            str           # "LONG" (único en spot)
    entry_price:     float
    size_usd:        float
    qty_base:        float         # BTC recibido en el BUY
    stop_order_id:   str           # ID de la orden STOP_LOSS_LIMIT en Binance
    tp_price:        float
    sl_price:        float
    opened_at:       float
    status:          str  = "open"
    partial_tp_done: bool = False


# ─────────────────────────────────────────────────────────────────────────────
#  SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class SignalEngine:
    """
    Orquestador principal del Signal Engine.

    Encapsula todo el estado mutable y la lógica de trading:
    - Estado de posiciones abiertas y PnL de sesión
    - Comunicación con execution_bot vía Redis Stream
    - Heartbeat y reconciliación de posiciones
    - Recarga dinámica de parámetros del optimizer
    """

    def __init__(self) -> None:
        # ── Conexión ─────────────────────────────────────────────────────────
        self.redis: Optional[aioredis.Redis] = None

        # ── Estado de trading ─────────────────────────────────────────────────
        self.open_pos:         List[Position]            = []
        self.session_pnl:      float                     = 0.0
        self.last_entry_ts:    float                     = 0.0
        self.entries_paused:   bool                      = False
        self.pending_receipts: Dict[str, asyncio.Future] = {}
        self.last_param_reload: float                    = 0.0
        self._last_blocked_log:  float                   = 0.0

        # ── Drawdown horario ──────────────────────────────────────────────────
        self._hourly_trades:    List[Tuple[float, float]] = []  # (ts, pnl_usd)
        self._drawdown_paused:  bool                      = False
        self._tg_update_id:     int                       = 0   # último update de Telegram

        # ── Modo nocturno ─────────────────────────────────────────────────────
        self._night_mode:       bool                      = False
        self._base_stake_usd:   float                     = STAKE_USD  # stake original del .env

        # ── Reporte diario ────────────────────────────────────────────────────
        self._last_report_day:  str                       = ""  # "YYYY-MM-DD"

        # ── Parámetros dinámicos (copiados de module-level, overwrite por optimizer) ──
        self.stake_usd        = STAKE_USD
        self.btc_window_s     = BTC_WINDOW_S
        self.btc_min_pct      = BTC_MIN_PCT
        self.tp_pct           = TP_PCT
        self.tp_partial_pct   = TP_PARTIAL_PCT
        self.sl_drop_pct      = SL_DROP_PCT
        self.max_hold_s       = MAX_HOLD_S
        self.throttle_s       = THROTTLE_S
        self.max_open_pos     = MAX_OPEN_POS
        self.daily_loss_limit = DAILY_LOSS_LIMIT

    # ── Logging helpers ───────────────────────────────────────────────────────

    def _linfo(self, msg: str) -> None:
        _log.info(msg)
        print(msg)
        self._publish_log_fire_forget(msg, "INFO")

    def _lwarn(self, msg: str) -> None:
        _log.warning(msg)
        print(f"[WARN] {msg}")
        self._publish_log_fire_forget(msg, "WARN")

    def _lerr(self, msg: str) -> None:
        _log.error(msg)
        print(f"[ERROR] {msg}")
        self._publish_log_fire_forget(msg, "ERROR")

    def _publish_log_fire_forget(self, msg: str, level: str) -> None:
        if self.redis is None:
            return
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._publish_log(msg, level))
        except RuntimeError:
            pass

    async def _publish_log(self, msg: str, level: str) -> None:
        if self.redis is None:
            return
        try:
            await self.redis.publish(REDIS_CH_UNIFIED_LOG, json.dumps({
                "bot":       "strategy_binance",
                "level":     level,
                "msg":       msg[:300],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }))
        except Exception:
            pass

    # ── Señales Redis ─────────────────────────────────────────────────────────

    async def _send_signal(self, signal: dict) -> Optional[dict]:
        """
        Publica una señal al execution_bot vía Redis Stream y espera el receipt.

        Campos del signal dict:
            action:       "BUY" | "SELL"
            signal_id:    UUID único
            side:         "LONG" (siempre para spot)
            token_id:     símbolo Binance (ej. "BTCUSDT")
            price:        precio BTC de referencia
            size_usd:     USDT a gastar (BUY) o valor estimado (SELL)
            position_id:  UUID de la posición
            size_tokens:  qty BTC a vender (solo SELL)
            reason:       motivo del cierre (TP, SL_MANUAL, TIME, etc.)
            entry_price:  precio de entrada (solo SELL)
            stop_order_id: ID del stop a cancelar (solo SELL)
        """
        if self.redis is None:
            self._lerr("[SIGNAL] Redis no conectado — no se puede enviar señal")
            return None

        signal_id = signal.get("signal_id", str(uuid.uuid4()))
        signal["signal_id"] = signal_id

        try:
            await self.redis.xadd(
                REDIS_STREAM_SIGNALS,
                {"payload": json.dumps(signal, ensure_ascii=False)},
                maxlen=1000,
                approximate=True,
            )
            await self.redis.publish(REDIS_CH_SIGNALS, json.dumps(signal, ensure_ascii=False))

            self._linfo(
                f"[SIGNAL] {signal['action']} enviado → id={signal_id[:16]} "
                f"symbol={signal.get('token_id')} price={signal.get('price'):.2f}"
            )
        except Exception as exc:
            self._lerr(f"[SIGNAL] Error publicando señal: {exc}")
            return None

        fut = asyncio.get_event_loop().create_future()
        self.pending_receipts[signal_id] = fut

        try:
            receipt_raw = await asyncio.wait_for(fut, timeout=EXEC_RECEIPT_TIMEOUT)
            return json.loads(receipt_raw) if isinstance(receipt_raw, str) else receipt_raw
        except asyncio.TimeoutError:
            self._lwarn(f"[SIGNAL] Timeout esperando receipt {signal_id[:16]} ({EXEC_RECEIPT_TIMEOUT}s)")
            return None
        finally:
            self.pending_receipts.pop(signal_id, None)

    async def _receipt_listener(self) -> None:
        """Escucha receipts del execution_bot y resuelve los futures pendientes."""
        sub  = await aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
        psub = sub.pubsub()
        await psub.psubscribe(f"{REDIS_CH_RECEIPTS}:*")
        self._linfo("[RECEIPTS] Escuchando receipts del execution_bot...")

        async for msg in psub.listen():
            if msg["type"] not in ("pmessage", "message"):
                continue
            try:
                data   = json.loads(msg["data"])
                sig_id = data.get("signal_id", "")
                fut    = self.pending_receipts.get(sig_id)
                if fut and not fut.done():
                    fut.set_result(data)
            except Exception as exc:
                self._lwarn(f"[RECEIPTS] Error procesando receipt: {exc}")

    # ── Lógica de entrada ─────────────────────────────────────────────────────

    def _check_entry_conditions(self) -> Tuple[bool, str, str]:
        """
        Verifica si las condiciones de entrada están dadas.

        Returns:
            (can_enter: bool, direction: str, reason: str)
        """
        if self.entries_paused:
            return False, "", "entradas pausadas por control externo"

        if len(self.open_pos) >= self.max_open_pos:
            return False, "", f"máx posiciones abiertas ({self.max_open_pos})"

        if time.time() - self.last_entry_ts < self.throttle_s:
            remaining = self.throttle_s - (time.time() - self.last_entry_ts)
            return False, "", f"throttle activo ({remaining:.0f}s restantes)"

        if self.session_pnl <= -self.daily_loss_limit:
            return False, "", f"circuit breaker: PnL sesión ${self.session_pnl:.2f}"

        if not DISABLE_TRADING_HOURS and not in_trading_hours(TRADING_HOUR_START, TRADING_HOUR_END):
            return False, "", f"fuera de horario Madrid ({TRADING_HOUR_START}h-{TRADING_HOUR_END}h)"

        btc_price_current = bp.get_btc_price()
        if btc_price_current is None:
            return False, "", "sin precio BTC"

        mom_pct, direction = bp.get_momentum(window_s=self.btc_window_s)

        if direction is None or abs(mom_pct) < self.btc_min_pct:
            return False, "", f"momentum insuficiente ({mom_pct:+.3f}% < {self.btc_min_pct:.3f}%)"

        if direction == "UP":
            return True, "LONG", f"momentum {mom_pct:+.3f}% en {self.btc_window_s}s"
        elif direction == "DOWN":
            # SHORT requiere momentum más fuerte (3x) para evitar falsas señales
            short_min_pct = self.btc_min_pct * 3
            if abs(mom_pct) < short_min_pct:
                return False, "", f"SHORT: momentum insuficiente ({mom_pct:+.3f}% < -{short_min_pct:.3f}%)"
            # Bloquear SHORT si tendencia general es alcista (ventana larga)
            trend_pct, trend_dir = bp.get_momentum(window_s=120)
            if trend_pct > SHORT_BLOCK_MOM:
                return False, "", f"SHORT bloqueado: tendencia alcista en 2min (+{trend_pct:.3f}% > +{SHORT_BLOCK_MOM:.3f}%)"
            return True, "SHORT", f"momentum {mom_pct:+.3f}% en {self.btc_window_s}s"

        return False, "", "sin dirección clara"

    # ── Apertura de posición ──────────────────────────────────────────────────

    async def _open_position(self) -> None:
        """Intenta abrir una posición LONG en BTC."""
        can_enter, direction, reason = self._check_entry_conditions()
        if not can_enter:
            # Log periódico (cada 60s) para diagnosticar por qué no entra
            now = time.time()
            if now - self._last_blocked_log >= 60:
                self._last_blocked_log = now
                btc = bp.get_btc_price()
                mom_pct, mom_dir = bp.get_momentum(window_s=self.btc_window_s)
                btc_str = f"${btc:.2f}" if btc is not None else "N/A"
                _log.info(
                    f"[SCAN] sin entrada — {reason} | "
                    f"BTC={btc_str} mom={mom_pct:+.4f}% dir={mom_dir} "
                    f"pos={len(self.open_pos)}/{self.max_open_pos} "
                    f"pnl={self.session_pnl:+.2f}$ paused={self.entries_paused}"
                )
            return

        btc_price = bp.get_btc_price()
        if btc_price is None:
            return

        pos_id = str(uuid.uuid4())[:16]

        signal = {
            "action":      "BUY",
            "signal_id":   str(uuid.uuid4()),
            "side":        direction,   # "LONG" o "SHORT"
            "token_id":    SYMBOL,
            "price":       btc_price,
            "size_usd":    self.stake_usd,
            "position_id": pos_id,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }

        self._linfo(
            f"[ENTRY] {direction} {SYMBOL} | precio={btc_price:.2f} "
            f"stake=${self.stake_usd:.2f} | {reason}"
        )
        self.last_entry_ts = time.time()

        receipt = await self._send_signal(signal)

        if receipt is None or receipt.get("status") != "FILLED":
            status  = receipt.get("status", "TIMEOUT") if receipt else "TIMEOUT"
            err_msg = receipt.get("error", "") if receipt else ""
            self._lwarn(f"[ENTRY] BUY rechazado/fallido: {status} {err_msg}")
            return

        fill_price    = float(receipt.get("fill_price", btc_price))
        qty_btc       = float(receipt.get("tokens_received", 0.0))
        stop_order_id = receipt.get("stop_order_id", "")

        if qty_btc <= 0:
            self._lerr(f"[ENTRY] Receipt FILLED pero qty_btc=0 — revisar execution_bot")
            return

        slip_pct = abs(fill_price - btc_price) / btc_price
        if slip_pct > SLIP_MAX_PCT:
            self._lwarn(
                f"[ENTRY] Slippage excesivo {slip_pct:.3%} > {SLIP_MAX_PCT:.3%} "
                f"(señal={btc_price:.2f} fill={fill_price:.2f}) — cerrando posición"
            )
            # Cerrar inmediatamente para evitar pérdida por SL mal calculado
            await self._execute_sell(
                Position(
                    position_id=pos_id, symbol=SYMBOL, side=direction,
                    entry_price=fill_price, size_usd=self.stake_usd,
                    qty_base=qty_btc, stop_order_id=stop_order_id,
                    tp_price=fill_price, sl_price=fill_price, opened_at=time.time(),
                ),
                fill_price, qty_btc, "SLIP_ABORT"
            )
            return

        if direction == "SHORT":
            tp_price = fill_price * (1 - self.tp_pct)       # TP: precio baja
            sl_price = fill_price * (1 + self.sl_drop_pct)  # SL: precio sube
        else:
            tp_price = fill_price * (1 + self.tp_pct)
            sl_price = fill_price * (1 - self.sl_drop_pct)

        pos = Position(
            position_id   = pos_id,
            symbol        = SYMBOL,
            side          = direction,
            entry_price   = fill_price,
            size_usd      = self.stake_usd,
            qty_base      = qty_btc,
            stop_order_id = stop_order_id,
            tp_price      = tp_price,
            sl_price      = sl_price,
            opened_at     = time.time(),
        )
        self.open_pos.append(pos)

        self._linfo(
            f"[ENTRY] Posición abierta: {pos_id} "
            f"entry={fill_price:.2f} qty={qty_btc:.6f}BTC "
            f"TP={tp_price:.2f} SL={sl_price:.2f} "
            f"stopOrderId={stop_order_id or 'manual'}"
        )

        self._save_open_positions()
        self._log_trade_csv("OPEN", pos, fill_price, 0.0, "ENTRY")

        if stop_order_id:
            self._linfo(f"[ENTRY] Stop-loss nativo activo en Binance: orderId={stop_order_id}")
        else:
            self._lwarn("[ENTRY] Sin stop-loss nativo — monitoring manual activo")

        metrics.set_open_positions(len(self.open_pos))

    # ── Gestión de posiciones ─────────────────────────────────────────────────

    async def _manage_positions(self) -> None:
        """
        Revisa todas las posiciones abiertas y ejecuta TP o cierre por tiempo.

        El SL nativo (STOP_LOSS_LIMIT) es gestionado por Binance directamente.
        Este monitor detecta si el SL fue disparado comparando precio vs sl_price.
        """
        btc_price = bp.get_btc_price()
        if btc_price is None:
            return

        now = time.time()

        for pos in list(self.open_pos):
            if pos.status == "closing":
                continue

            holding_s = now - pos.opened_at
            is_short  = pos.side == "SHORT"

            # PnL directo según dirección
            if is_short:
                pnl_pct = (pos.entry_price - btc_price) / pos.entry_price
                sl_hit  = btc_price > pos.sl_price   # precio sube = pérdida en SHORT
            else:
                pnl_pct = (btc_price - pos.entry_price) / pos.entry_price
                sl_hit  = btc_price < pos.sl_price   # precio baja = pérdida en LONG

            # ── Verificar si SL fue disparado por Binance ────────────────────
            # Grace period: no revisar SL los primeros ENTRY_GRACE_S segundos.
            # Evita cierres instantáneos por slippage en el momento de entrada.
            if sl_hit and holding_s < ENTRY_GRACE_S:
                continue

            if sl_hit:
                sl_dir = ">" if is_short else "<"
                self._lwarn(
                    f"[MANAGE] SL detectado en pos {pos.position_id} ({pos.side}): "
                    f"precio={btc_price:.2f} {sl_dir} sl={pos.sl_price:.2f} — cerrando"
                )
                await self._execute_sell(pos, btc_price, pos.qty_base, "SL_BINANCE")
                continue

            # ── Take Profit parcial ──────────────────────────────────────────
            if not pos.partial_tp_done and pnl_pct >= self.tp_partial_pct:
                self._linfo(
                    f"[TP PARCIAL] pos={pos.position_id} ({pos.side}) precio={btc_price:.2f} "
                    f"pnl={pnl_pct:.3%} >= {self.tp_partial_pct:.3%} — vendiendo 50%"
                )
                qty_sell = pos.qty_base * 0.5
                await self._execute_sell(pos, btc_price, qty_sell, "TP_PARTIAL")
                pos.partial_tp_done = True
                pos.qty_base       -= qty_sell
                continue

            # ── Take Profit completo ─────────────────────────────────────────
            if pnl_pct >= self.tp_pct:
                self._linfo(
                    f"[TP FULL] pos={pos.position_id} ({pos.side}) precio={btc_price:.2f} "
                    f"pnl={pnl_pct:.3%} >= {self.tp_pct:.3%}"
                )
                await self._execute_sell(pos, btc_price, pos.qty_base, "TP_FULL")
                continue

            # ── Cierre por tiempo máximo ─────────────────────────────────────
            if holding_s >= self.max_hold_s:
                self._lwarn(
                    f"[TIME EXIT] pos={pos.position_id} ({pos.side}) holding={holding_s:.0f}s >= {self.max_hold_s}s "
                    f"precio={btc_price:.2f} pnl={pnl_pct:.3%} — cerrando a mercado"
                )
                await self._execute_sell(pos, btc_price, pos.qty_base, "TIME_EXIT")
                continue

            # ── Log periódico ────────────────────────────────────────────────
            if int(holding_s) % 30 == 0 and holding_s > 0:
                self._linfo(
                    f"[POS] {pos.position_id[:8]} ({pos.side}) holding={holding_s:.0f}s "
                    f"precio={btc_price:.2f} pnl={pnl_pct:+.3%} "
                    f"TP={pos.tp_price:.2f} SL={pos.sl_price:.2f}"
                )

    async def _execute_sell(
        self,
        pos: Position,
        current_price: float,
        qty_to_sell: float,
        reason: str,
    ) -> None:
        """Envía señal SELL al execution_bot y procesa el resultado."""
        pos.status = "closing"

        signal = {
            "action":        "SELL",
            "signal_id":     str(uuid.uuid4()),
            "side":          pos.side,    # "LONG" o "SHORT"
            "token_id":      pos.symbol,
            "price":         current_price,
            "size_usd":      pos.size_usd,
            "position_id":   pos.position_id,
            "size_tokens":   qty_to_sell,
            "reason":        reason,
            "entry_price":   pos.entry_price,
            "stop_order_id": pos.stop_order_id,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }

        receipt = await self._send_signal(signal)

        if receipt is None or receipt.get("status") not in ("FILLED", "SKIPPED"):
            status = receipt.get("status", "TIMEOUT") if receipt else "TIMEOUT"
            self._lerr(
                f"[SELL] FALLIDO pos={pos.position_id} reason={reason} "
                f"status={status} — reintentando en próximo ciclo"
            )
            pos.status = "open"
            return

        fill_price = float(receipt.get("fill_price", current_price))
        # PnL directo según dirección de la posición
        qty_ref = qty_to_sell if qty_to_sell < pos.qty_base else pos.qty_base
        if pos.side == "SHORT":
            pnl_usd = qty_ref * (pos.entry_price - fill_price)
        else:
            pnl_usd = qty_ref * (fill_price - pos.entry_price)

        await self._close_position_local(pos, fill_price, reason, pnl_usd)

    async def _close_position_local(
        self,
        pos: Position,
        exit_price: float,
        reason: str,
        pnl_usd: float,
    ) -> None:
        """Actualiza el tracker local tras el cierre de una posición."""
        if pos in self.open_pos:
            self.open_pos.remove(pos)

        self.session_pnl += pnl_usd
        self._hourly_trades.append((time.time(), pnl_usd))
        loss_tracker.record_pnl(pnl_usd)

        if pos.side == "SHORT":
            pnl_pct = (pos.entry_price - exit_price) / pos.entry_price
        else:
            pnl_pct = (exit_price - pos.entry_price) / pos.entry_price

        self._linfo(
            f"[CLOSE] {pos.position_id[:8]} ({pos.side}) reason={reason} "
            f"entry={pos.entry_price:.2f} exit={exit_price:.2f} "
            f"pnl={pnl_pct:+.3%} ({pnl_usd:+.2f}$) "
            f"session={self.session_pnl:+.2f}$"
        )

        self._log_trade_csv("CLOSE", pos, exit_price, pnl_usd, reason)
        self._save_open_positions()

        mode   = "DRY" if DRY_RUN else "REAL"
        result = "WIN" if pnl_usd >= 0 else "LOSS"
        metrics.inc_trade(mode=mode, side=pos.side, result=result)
        metrics.set_pnl(mode=mode, value=self.session_pnl)
        metrics.set_open_positions(len(self.open_pos))

        if pnl_usd < -10.0:
            await send_telegram_async(
                f"LOSS ${pnl_usd:.2f} | {pos.position_id[:8]} | {reason}\n"
                f"Entry={pos.entry_price:.2f} Exit={exit_price:.2f} "
                f"PnL={pnl_pct:+.3%}",
                level="WARNING",
                prefix_label="CalvinBTC · Signal",
            )

    # ── Persistencia ──────────────────────────────────────────────────────────

    def _save_open_positions(self) -> None:
        """Guarda posiciones abiertas a disco para sobrevivir reinicios."""
        data = [asdict(p) for p in self.open_pos]
        try:
            write_json_atomic(OPEN_POS_FILE, data)
        except Exception as exc:
            self._lwarn(f"[PERSIST] Error guardando posiciones: {exc}")

    def _load_open_positions(self) -> None:
        """Carga posiciones abiertas desde disco al reiniciar."""
        path = Path(OPEN_POS_FILE)
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            self.open_pos = [Position(**p) for p in raw]
            if self.open_pos:
                self._lwarn(
                    f"[PERSIST] {len(self.open_pos)} posiciones cargadas desde disco — "
                    f"reconciliando contra Binance..."
                )
        except Exception as exc:
            self._lwarn(f"[PERSIST] Error cargando posiciones: {exc}")

    async def _reconcile_positions_at_startup(self) -> None:
        """
        Cruza posiciones en memoria contra Binance real al arrancar.
        Elimina posiciones cuyo stop-loss ya fue ejecutado mientras el bot estaba caído.
        Solo actúa en modo REAL — en DRY_RUN no hay órdenes reales que verificar.
        """
        if DRY_RUN or not self.open_pos:
            return

        self._linfo(f"[RECONCILE] Verificando {len(self.open_pos)} posiciones contra Binance...")

        try:
            exchange = BinanceSpotExchange()
            exchange.initialize()
            await exchange._async_init()

            tracked = {
                p.position_id: {
                    "token_id":      p.symbol,
                    "stop_order_id": p.stop_order_id,
                    "entry_price":   p.entry_price,
                    "qty_base":      p.qty_base,
                    "size_usd":      p.size_usd,
                }
                for p in self.open_pos
            }

            active_dict = await exchange.reconcile_open_positions(tracked)
            active_ids  = set(active_dict.keys())
            closed_pos  = [p for p in self.open_pos if p.position_id not in active_ids]
            self.open_pos = [p for p in self.open_pos if p.position_id in active_ids]

            if closed_pos:
                self._lwarn(
                    f"[RECONCILE] {len(closed_pos)} posiciones eliminadas "
                    f"(SL ejecutado por Binance mientras el bot estaba caído): "
                    + ", ".join(p.position_id[:8] for p in closed_pos)
                )
                for p in closed_pos:
                    pnl_approx = (p.sl_price - p.entry_price) * p.qty_base
                    self._log_trade_csv("CLOSE", p, p.sl_price, pnl_approx, "SL_BINANCE_OFFLINE")
                    self.session_pnl += pnl_approx

                self._save_open_positions()

            if self.open_pos:
                self._linfo(f"[RECONCILE] {len(self.open_pos)} posiciones confirmadas activas en Binance")
            else:
                self._linfo("[RECONCILE] Sin posiciones activas — arrancando limpio")

        except Exception as exc:
            self._lwarn(f"[RECONCILE] Error durante reconciliación: {exc} — manteniendo posiciones de disco")

    def _log_trade_csv(
        self,
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
                        "entry_price", "exit_price", "qty_btc", "size_usd", "pnl_usd", "reason", "mode",
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
                    "DRY" if DRY_RUN else "REAL",
                ])
        except Exception as exc:
            self._lwarn(f"[CSV] Error escribiendo trade: {exc}")

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """Publica heartbeat cada 5 segundos para watchdog y dashboard."""
        while True:
            try:
                if self.redis:
                    btc             = bp.get_btc_price()
                    mom_pct, mom_dir = bp.get_momentum(window_s=self.btc_window_s)

                    positions_data = []
                    now_ts = time.time()
                    for p in self.open_pos:
                        unrealized_pct = ((btc - p.entry_price) / p.entry_price * 100) if btc else 0
                        positions_data.append({
                            "position_id":    p.position_id[:12],
                            "symbol":         p.symbol,
                            "entry_price":    round(p.entry_price, 2),
                            "current_price":  round(btc, 2) if btc else None,
                            "qty_base":       round(p.qty_base, 6),
                            "size_usd":       round(p.size_usd, 2),
                            "tp_price":       round(p.tp_price, 2),
                            "sl_price":       round(p.sl_price, 2),
                            "unrealized_pct": round(unrealized_pct, 3),
                            "holding_s":      round(now_ts - p.opened_at),
                            "stop_order_id":  p.stop_order_id[:12] if p.stop_order_id else "",
                        })

                    hb = {
                        "bot":            "strategy_binance",
                        "status":         "online",
                        "dry_run":        DRY_RUN,
                        "symbol":         SYMBOL,
                        "open_positions": len(self.open_pos),
                        "positions_data": positions_data,
                        "session_pnl":    round(self.session_pnl, 2),
                        "btc_price":      round(btc, 2) if btc else None,
                        "btc_momentum":   round(mom_pct, 4),
                        "btc_direction":  mom_dir,
                        "entries_paused": self.entries_paused,
                        "params": {
                            "STAKE_USD":        self.stake_usd,
                            "TP_PCT":           self.tp_pct,
                            "SL_DROP_PCT":      self.sl_drop_pct,
                            "BTC_WINDOW_S":     self.btc_window_s,
                            "BTC_MIN_PCT":      self.btc_min_pct,
                            "MAX_HOLD_S":       self.max_hold_s,
                            "THROTTLE_S":       self.throttle_s,
                            "MAX_OPEN_POS":     self.max_open_pos,
                            "DAILY_LOSS_LIMIT": self.daily_loss_limit,
                        },
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

                    payload = json.dumps(hb, ensure_ascii=False)
                    await self.redis.publish(REDIS_CH_HEARTBEATS, payload)
                    await self.redis.setex("state:strategy:latest", 30, payload)

            except Exception:
                pass
            await asyncio.sleep(5)

    # ── Recarga dinámica de parámetros ────────────────────────────────────────

    async def _reload_dynamic_params(self) -> None:
        """Lee params ajustados por el optimizer desde Redis y los aplica."""
        if time.time() - self.last_param_reload < PARAM_RELOAD_INTERVAL:
            return
        self.last_param_reload = time.time()

        try:
            if not self.redis:
                return
            raw = await self.redis.get(REDIS_KEY_PARAMS)
            if not raw:
                return
            data   = json.loads(raw)
            params = data.get("params", {})
            if not params:
                return

            # Parámetros que siempre se aplican
            self.stake_usd        = float(params.get("STAKE_USD",       self.stake_usd))
            self.btc_min_pct      = float(params.get("BTC_MIN_PCT",     self.btc_min_pct))
            self.btc_window_s     = int(params.get("BTC_WINDOW_S",      self.btc_window_s))
            self.max_open_pos     = int(params.get("MAX_OPEN_POS",      self.max_open_pos))
            self.throttle_s       = int(params.get("THROTTLE_S",        self.throttle_s))
            self.max_hold_s       = int(params.get("MAX_HOLD_S",        self.max_hold_s))
            self.daily_loss_limit = float(params.get("DAILY_LOSS_LIMIT", self.daily_loss_limit))

            # Params que afectan posiciones abiertas — solo aplicar si no hay ninguna
            # (cambiar TP/SL con una posición abierta crea inconsistencia con el stop en Binance)
            if not self.open_pos:
                self.tp_pct         = float(params.get("TP_PCT",         self.tp_pct))
                self.tp_partial_pct = float(params.get("TP_PARTIAL_PCT", self.tp_partial_pct))
                self.sl_drop_pct    = float(params.get("SL_DROP_PCT",    self.sl_drop_pct))

        except Exception as ex:
            self._lwarn(f"[PARAMS] Error recargando params dinámicos: {ex}")

    # ── Auto-pause por drawdown horario ──────────────────────────────────────

    async def _drawdown_monitor(self) -> None:
        """
        Cada 30s revisa las pérdidas de la última hora.
        Si superan HOURLY_LOSS_LIMIT, pausa entradas y alerta por Telegram.
        El usuario despausa respondiendo /resume al bot de Telegram.
        """
        while True:
            await asyncio.sleep(30)
            try:
                now    = time.time()
                cutoff = now - 3600
                # Limpiar trades más viejos de 1h
                self._hourly_trades = [(ts, p) for ts, p in self._hourly_trades if ts >= cutoff]
                hourly_loss = sum(p for _, p in self._hourly_trades if p < 0)

                if hourly_loss <= -HOURLY_LOSS_LIMIT and not self._drawdown_paused:
                    self._drawdown_paused  = True
                    self.entries_paused    = True
                    self._linfo(
                        f"[DRAWDOWN] Pérdida última hora: ${hourly_loss:.2f} >= límite ${HOURLY_LOSS_LIMIT:.0f} "
                        f"— entradas PAUSADAS"
                    )
                    await send_telegram_async(
                        f"🔴 AUTO-PAUSA por drawdown\n"
                        f"Pérdida en la última hora: ${hourly_loss:.2f}\n"
                        f"Límite configurado: ${HOURLY_LOSS_LIMIT:.0f}\n\n"
                        f"El bot ha pausado las entradas automáticamente.\n"
                        f"Responde /resume para reanudar.",
                        level="WARNING",
                        prefix_label="CalvinBTC · Auto-Pausa",
                    )

            except Exception as exc:
                self._lwarn(f"[DRAWDOWN] Error en monitor: {exc}")

    async def _telegram_command_listener(self) -> None:
        """
        Escucha comandos de Telegram. Solo acepta /resume para despausar el bot
        tras una auto-pausa por drawdown.
        """
        import aiohttp
        token   = os.getenv("TG_TOKEN", "")
        chat_id = os.getenv("TG_CHAT_ID", "")
        if not token or not chat_id:
            return

        while True:
            await asyncio.sleep(10)
            try:
                url    = f"https://api.telegram.org/bot{token}/getUpdates"
                params = {"offset": self._tg_update_id + 1, "timeout": 5, "limit": 10}
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                        data = await resp.json()

                if not data.get("ok"):
                    continue

                for update in data.get("result", []):
                    self._tg_update_id = update["update_id"]
                    msg_text = update.get("message", {}).get("text", "").strip().lower()
                    from_id  = str(update.get("message", {}).get("from", {}).get("id", ""))

                    if from_id != chat_id:
                        continue  # solo acepta tu chat_id

                    if msg_text == "/resume" and self._drawdown_paused:
                        self._drawdown_paused = False
                        self.entries_paused   = False
                        self._linfo("[TELEGRAM] /resume recibido — entradas reanudadas")
                        await send_telegram_async(
                            "✅ Bot reanudado por /resume\nLas entradas vuelven a estar activas.",
                            level="INFO",
                            prefix_label="CalvinBTC · Telegram",
                        )
                    elif msg_text == "/status":
                        mom_pct, mom_dir = bp.get_momentum(window_s=self.btc_window_s)
                        btc = bp.get_btc_price()
                        await send_telegram_async(
                            f"📊 Estado CalvinBTC\n"
                            f"BTC: ${btc:,.2f} | mom: {mom_pct:+.3f}% {mom_dir or '—'}\n"
                            f"PnL sesión: ${self.session_pnl:+.2f}\n"
                            f"Posiciones: {len(self.open_pos)}/{self.max_open_pos}\n"
                            f"Modo: {'🌙 nocturno' if self._night_mode else '☀️ diurno'}\n"
                            f"Pausado: {'⛔ SÍ' if self.entries_paused else '✅ NO'}",
                            level="INFO",
                            prefix_label="CalvinBTC · Status",
                        )

            except Exception as exc:
                self._lwarn(f"[TELEGRAM] Error en listener: {exc}")

    # ── Modo conservador nocturno ─────────────────────────────────────────────

    async def _night_mode_monitor(self) -> None:
        """
        Cambia el stake automáticamente según la hora Madrid.
        Noche (22h-09h): stake reducido a NIGHT_STAKE_USD ($20)
        Día (09h-22h):   stake normal del .env ($100)
        """
        while True:
            await asyncio.sleep(60)
            try:
                hora_madrid = madrid_now().hour
                es_noche = (hora_madrid >= NIGHT_HOUR_START or hora_madrid < NIGHT_HOUR_END)

                if es_noche and not self._night_mode:
                    self._night_mode  = True
                    self.stake_usd    = NIGHT_STAKE_USD
                    self._linfo(
                        f"[NIGHT] Modo nocturno activado ({hora_madrid}h Madrid) "
                        f"— stake reducido a ${NIGHT_STAKE_USD:.0f}"
                    )
                    await send_telegram_async(
                        f"🌙 Modo nocturno activado ({hora_madrid}h Madrid)\n"
                        f"Stake reducido: ${self._base_stake_usd:.0f} → ${NIGHT_STAKE_USD:.0f}",
                        level="INFO",
                        prefix_label="CalvinBTC · Noche",
                    )

                elif not es_noche and self._night_mode:
                    self._night_mode = False
                    self.stake_usd   = self._base_stake_usd
                    self._linfo(
                        f"[NIGHT] Modo diurno activado ({hora_madrid}h Madrid) "
                        f"— stake restaurado a ${self._base_stake_usd:.0f}"
                    )
                    await send_telegram_async(
                        f"☀️ Modo diurno activado ({hora_madrid}h Madrid)\n"
                        f"Stake restaurado: ${NIGHT_STAKE_USD:.0f} → ${self._base_stake_usd:.0f}",
                        level="INFO",
                        prefix_label="CalvinBTC · Día",
                    )

            except Exception as exc:
                self._lwarn(f"[NIGHT] Error en monitor nocturno: {exc}")

    # ── Reporte diario ────────────────────────────────────────────────────────

    async def _daily_report(self) -> None:
        """
        Cada minuto revisa si son las 23:59 Madrid.
        Si es así, envía resumen del día por Telegram (una sola vez por día).
        """
        while True:
            await asyncio.sleep(60)
            try:
                now_madrid = madrid_now()
                today_str  = now_madrid.strftime("%Y-%m-%d")

                if now_madrid.hour == 23 and now_madrid.minute == 59 and today_str != self._last_report_day:
                    self._last_report_day = today_str

                    # Leer CSV para stats del día
                    import csv as _csv
                    path = Path(TRADES_CSV)
                    day_trades = []
                    if path.exists():
                        with open(path, encoding="utf-8") as f:
                            for row in _csv.DictReader(f):
                                if row.get("timestamp", "").startswith(today_str):
                                    day_trades.append(float(row.get("pnl_usd", 0)))

                    total    = len(day_trades)
                    wins     = sum(1 for p in day_trades if p > 0)
                    losses   = total - wins
                    pnl_day  = sum(day_trades)
                    wr       = (wins / total * 100) if total else 0.0
                    avg_win  = (sum(p for p in day_trades if p > 0) / wins) if wins else 0
                    avg_loss = (sum(p for p in day_trades if p < 0) / losses) if losses else 0
                    emoji    = "🟢" if pnl_day >= 0 else "🔴"

                    await send_telegram_async(
                        f"{emoji} Reporte diario — {today_str}\n\n"
                        f"PnL del día: ${pnl_day:+.2f} USDT\n"
                        f"Trades: {total} ({wins}✅ / {losses}❌)\n"
                        f"Aciertos: {wr:.1f}%\n"
                        f"Ganancia media: +${avg_win:.2f}\n"
                        f"Pérdida media: ${avg_loss:.2f}\n"
                        f"PnL sesión acumulado: ${self.session_pnl:+.2f}",
                        level="INFO",
                        prefix_label="CalvinBTC · Reporte Diario",
                    )
                    self._linfo(f"[REPORT] Reporte diario enviado: {total} trades, PnL={pnl_day:+.2f}$")

            except Exception as exc:
                self._lwarn(f"[REPORT] Error en reporte diario: {exc}")

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _main_loop(self) -> None:
        """Loop principal: scan → entrada → gestión de posiciones."""
        self._linfo(f"[MAIN] Iniciando strategy_binance | DRY_RUN={DRY_RUN} | symbol={SYMBOL}")
        self._linfo(
            f"[MAIN] TP={self.tp_pct:.1%} SL={self.sl_drop_pct:.1%} "
            f"STAKE=${self.stake_usd:.2f} [riesgo 7.5/10]"
        )

        while True:
            try:
                await self._reload_dynamic_params()

                lt_status, lt_msg = loss_tracker.check()
                if lt_status == loss_tracker.STATUS_BREACHED:
                    self._lerr(f"[LOSS TRACKER] {lt_msg} — ESTRATEGIA DETENIDA")
                    await asyncio.sleep(60)
                    continue
                elif lt_status == loss_tracker.STATUS_WARNING:
                    self._lwarn(f"[LOSS TRACKER] {lt_msg}")

                await self._open_position()
                await self._manage_positions()

            except Exception as exc:
                import traceback
                self._lerr(f"[MAIN] Error inesperado: {exc}\n{traceback.format_exc()}")

            await asyncio.sleep(SCAN_INTERVAL_S)

    async def run(self) -> None:
        """Punto de entrada — conecta Redis y lanza todas las tareas."""
        mode_label = "DRY RUN (paper trading)" if DRY_RUN else "REAL MONEY"
        exchange_type = os.getenv("EXCHANGE_TYPE", "spot").upper()
        hours_label = "24/7" if DISABLE_TRADING_HOURS else f"{TRADING_HOUR_START}h-{TRADING_HOUR_END}h Madrid"
        self._linfo("=" * 65)
        self._linfo(f"  CalvinBTC · Signal Engine [{mode_label}] [{exchange_type}]")
        self._linfo(f"  Símbolo: {SYMBOL} | Stake: ${self.stake_usd:.2f} | Max pos: {self.max_open_pos}")
        self._linfo(f"  Momentum: {self.btc_min_pct:.3f}% en {self.btc_window_s}s | Horario: {hours_label}")
        self._linfo(f"  TP: {self.tp_pct:.1%} | SL: {self.sl_drop_pct:.1%} | Throttle: {self.throttle_s}s")
        self._linfo("=" * 65)

        if not DRY_RUN:
            self._lwarn("MODO REAL ACTIVADO — las órdenes se ejecutarán en Binance")

        self.redis = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
        await self.redis.ping()
        self._linfo(f"[REDIS] Conectado: {REDIS_URL}")

        self._load_open_positions()
        await self._reconcile_positions_at_startup()

        mode_label = "DRY RUN" if DRY_RUN else "REAL TESTNET"
        open_count = len(self.open_pos)
        msg = (
            f"CalvinBTC · Signal Engine arrancó\n"
            f"Modo: {mode_label}\n"
            f"Symbol: {SYMBOL} | Stake: ${self.stake_usd}\n"
            f"TP: {self.tp_pct*100:.1f}% | SL: {self.sl_drop_pct*100:.1f}%\n"
            f"Posiciones abiertas cargadas: {open_count}"
        )
        await send_telegram_async(msg, level="INFO", prefix_label="CalvinBTC · Signal")

        await asyncio.gather(
            bp.run_btc_poller(),
            self._receipt_listener(),
            self._heartbeat_loop(),
            self._main_loop(),
            self._drawdown_monitor(),
            self._telegram_command_listener(),
            self._night_mode_monitor(),
            self._daily_report(),
        )


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    engine = SignalEngine()
    await engine.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log.info("[MAIN] Estrategia detenida por usuario")
