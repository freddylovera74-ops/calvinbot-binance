"""
calvin5.py — Sniper BTC para mercados Up/Down 5 minutos de Polymarket.

Estrategia: BTC Momentum + Late Sniper
  • Opera solo en los últimos SNIPER_WINDOW_S (60s) de cada ronda de 5min
  • Requiere que BTC tenga momentum ≥ BTC_MIN_PCT% en los últimos BTC_WINDOW_S segundos
  • Entra solo si el precio del token está en ENTRY_MIN–ENTRY_MAX (fees bajas, crowd decidido)
  • TP relativo desde entrada (+6% / +10%). SL en 5 cents de caída o floor 0.70
  • Si quedan ≤ HOLD_SECS y precio ≥ 0.87 → hold a resolución

Uso:
    python calvin5.py           # paper trading (DRY RUN)
    python calvin5.py --real    # real money (requiere POLY_API_KEY etc.)

Depende de btc_price.py (en el mismo directorio).
"""

import asyncio
import csv
import json
import os
import sys
import time

# Forzar UTF-8 en Windows (necesario para emojis en consola)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import requests
import websockets
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

import btc_price as bp
from metrics import metrics
import loss_tracker  # C4: límites de pérdida semanales/mensuales persistentes

# Métricas Prometheus — delegadas al módulo metrics.py (no-op si prometheus_client no instalado)
def _prom_record_trade(side: str, result: str) -> None:
    mode = "DRY" if DRY_RUN else "REAL"
    metrics.inc_trade(mode=mode, side=side, result=result)


def _prom_update_state(open_pos: int, pnl: float, btc_mom: float) -> None:
    mode = "DRY" if DRY_RUN else "REAL"
    metrics.set_open_positions(open_pos)
    metrics.set_pnl(mode=mode, value=pnl)
    metrics.set_btc_momentum(btc_mom)


# ── Canal Redis de señales hacia execution_bot ────────────────────────────────
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CH_SIGNALS     = "signals:trade"          # pub/sub legacy (mantenido para compatibilidad)
REDIS_STREAM_SIGNALS = "stream:signals:trade"   # Stream persistente (at-least-once delivery)
REDIS_CH_RECEIPTS    = "execution:receipts"   # prefijo; se añade :{signal_id}
REDIS_CH_CONFIG      = "config:update:calculation_bot"
REDIS_CH_HEARTBEATS  = "health:heartbeats"
REDIS_CH_CONTROL     = "signals:control"       # Dashboard → Calvin5 (PAUSE/RESUME entries)
REDIS_CH_UNIFIED_LOG = "logs:unified"          # Calvin5 → Dashboard (unified log stream)

# Timeout esperando fill del execution_bot (en segundos)
EXEC_RECEIPT_TIMEOUT = 8.0

# Singleton Redis — inicializado en main()
_redis: Optional[aioredis.Redis] = None
# Singleton HTTP session — pool de conexiones reutilizable (evita TCP overhead por llamada)
_http_session: Optional[aiohttp.ClientSession] = None
# Futures en espera de recibo: {signal_id → asyncio.Future}
_pending_receipts: Dict[str, asyncio.Future] = {}
# Control flags — gestionados por _control_listener()
_entries_paused: bool = False

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

SLUG_PATTERN   = "btc-updown-5m"
SLOT_SECONDS   = 300          # 5 minutos por ronda

GAMMA_API      = "https://gamma-api.polymarket.com"
CLOB_API       = "https://clob.polymarket.com"
CLOB_WS_URL    = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Ventana sniper
SNIPER_WINDOW_S = 250         # ÚLTIMOS 120s de cada ronda (5min = 300s, entra únicamente en [180-300])
ENTRY_MIN       = 0.52        # precio mínimo — riesgo 8/10
ENTRY_MAX       = 0.90        # precio máximo — riesgo 8/10
MIN_SELL_TOKENS  = 5.0         # mínimo de tokens para SELL en Polymarket CLOB
MIN_TOKENS_BUY   = 5.0         # mínimo tokens para abrir posición (igual al mínimo de Polymarket)
SLIP_MAX_PCT    = 0.04        # slippage máximo tolerado entre precio señal y precio real de fill (4%)
FOK_PRICE_BUF   = float(os.getenv("FOK_PRICE_BUF", "0.01"))  # margen extra sobre precio señal para el límite FOK
MAX_SPREAD      = float(os.getenv("MAX_SPREAD", "0.08"))      # spread máximo — 60% más tolerante (agresivo)
KELLY_ENABLED   = os.getenv("KELLY_ENABLED", "false").lower() in ("1", "true")  # O12: sizing dinámico
KELLY_FRACTION  = float(os.getenv("KELLY_FRACTION", "0.25"))  # fracción del Kelly completo (25% = cuarto-Kelly)
KELLY_MIN_TRADES = int(os.getenv("KELLY_MIN_TRADES", "50"))   # mínimo trades para activar Kelly
KELLY_MIN_USD   = float(os.getenv("KELLY_MIN_USD",   "5.10")) # stake mínimo aunque Kelly diga menos
KELLY_MAX_USD   = float(os.getenv("KELLY_MAX_USD",  "20.00")) # stake máximo absoluto
SCAN_INTERVAL_S = 0.1         # segundos entre scans — 5x MÁS RÁPIDO (optimizado para REAL ganador)

# TP / SL (relativos desde precio de entrada)
TP_FULL_PCT        = 0.21      # salida completa si ganancia ≥ 21%
TP_MID_PCT         = 0.12      # salida parcial si ganancia ≥ 12%
TP_LAST_15S_PCT    = 0.03      # en los últimos 15s, cerrar si ganancia ≥ 3%
HOLD_SECS          = 45        # si quedan ≤ 45s → hold a resolución
SL_DROP            = 0.18      # salir si precio cae 18 cents — riesgo 8/10
SL_FLOOR           = 0.28      # floor absoluto — riesgo 8/10
SL_TIME_S          = 230       # salir si holding ≥ 230s y precio < entrada — riesgo 8/10
SL_LAST_30S_PCT    = 0.05      # en los últimos 30s, SL más ajustado: -5% relativo desde entrada

# BTC momentum
BTC_WINDOW_S   = 10           # ventana de momentum BTC — mantener para contexto
BTC_MIN_PCT    = 0.02        # % mínimo de movimiento BTC — riesgo 8/10
BTC_REQUIRED   = True         # False = deshabilita filtro (debug)

# Late Resolution Sniper — desactivado
LATE_ENABLED      = False
LATE_WINDOW_START = 40
LATE_WINDOW_END   = 20
LATE_MIN_PRICE    = 0.85

# Anti-late-squeeze
ENTRY_CUTOFF_S   = 5      # no nuevas entradas en los últimos 5s (AGRESIVO: permite squeeze)
ENTRY_MAX_LATE   = 0.93   # ENTRY_MAX para ventana tardía (máximo permitido)
PRICE_STABILITY_SCANS = 1 # 1 scan basta — mínimo requerido

# Hold a resolución si BTC se mueve muy fuerte en la dirección de la posición
# Ejemplo: BTC sube de 76000 → 76120 = +0.16% → casi seguro que UP gana
HOLD_BTC_PCT = 0.15   # si BTC momentum >= 0.15% en dirección de la pos → hold resolución

# Circuit breaker
DAILY_LOSS_LIMIT = 80.0   # pausa si pérdidas de la sesión > $80 — riesgo 8/10

# Horario de operación — cruza medianoche: activo de 23:00h a 19:59h España
TRADING_HOUR_START  = 23          # 23:00h España
TRADING_HOUR_END    = 23          # 20:00h España (para a las 20:00h)

# Drawdown máximo del tramo horario — se desconecta si supera este límite
SESSION_DRAWDOWN_LIMIT = 150.0    # $150 máximos de pérdida — riesgo 8/10

# Sizing
# MICRO-TEST REAL: stake mínimo para validar flujo CLOB/wallet sin riesgo real.
# Cambiar STAKE_USD_OVERRIDE en .env (o aquí) cuando se pase a producción.
# Con $0.10-$0.20 los tokens serán < MIN_SELL_TOKENS(5), por lo que los SELLs
# esperarán resolución automática al round_end — ideal para testear BUY/wallet.
STAKE_USD      = float(os.getenv("STAKE_USD_OVERRIDE", "5.40"))  # $5.40 garantiza ≥6 tokens a ENTRY_MAX=0.90 (5.40/0.90=6 tokens)

# Modo micro-test: stakes forzados a $1 para probar wallet/CLOB sin arriesgar capital
MICRO_TEST = os.getenv("MICRO_TEST", "false").lower() in ("1", "true")
if MICRO_TEST:
    print("⚠️  MICRO_TEST ACTIVADO — stake forzado a $1.00")
    print("⚠️  Las posiciones NO podrán usar TP/SL (tokens < 6). Solo resolución final.")
    STAKE_USD = 1.0

# Verificación de stake mínimo para garantizar capacidad de SELL
# Con precio ENTRY_MAX y STAKE_USD, se obtienen STAKE_USD/ENTRY_MAX tokens.
# Necesitamos >= MIN_TOKENS_BUY tokens para poder abrir (y >= MIN_SELL_TOKENS para vender).
_MIN_STAKE_REQUIRED = MIN_TOKENS_BUY * ENTRY_MAX  # e.g. 6.0 * 0.85 = $5.10
if not MICRO_TEST and STAKE_USD < _MIN_STAKE_REQUIRED:
    print(
        f"⚠️  ADVERTENCIA: STAKE_USD=${STAKE_USD:.2f} puede ser insuficiente para entrar "
        f"a precio ENTRY_MAX={ENTRY_MAX}. "
        f"Mínimo recomendado: ${_MIN_STAKE_REQUIRED:.2f} "
        f"(MIN_TOKENS_BUY={MIN_TOKENS_BUY} × ENTRY_MAX={ENTRY_MAX})"
    )
# Comisión real de Polymarket en salidas anticipadas (TP/SL)
# No aplica en round_end (resolución automática al precio final)
POLYMARKET_FEE_PCT = 0.03     # 3% del USDC recibido en el SELL
MAX_OPEN_POS          = 1   # máx posiciones simultáneas (conservador para seguridad)
MAX_ENTRIES_PER_ROUND = 3   # máx entradas por ronda — riesgo 8/10
THROTTLE_S            = 3    # 3s entre entradas (era 10s) — MUCHO MÁS RÁPIDO
MAX_SELL_RETRIES      = 5   # máximo reintentos de SELL antes de marcar posición como orphaned

TRADES_CSV        = "calvin5_trades.csv"
MAX_TRADES_CSV    = 2000          # filas máximas en el CSV; al superarlo se rota (se conservan las últimas N)
OPEN_POS_FILE     = "calvin5_open.json"     # persistencia de posiciones abiertas entre reinicios
WINDOW_PNL_FILE   = "calvin5_window.json"   # persistencia del PnL del tramo horario actual
LOG_FILE          = "calvin5.log"

# ── Parámetros vivos — actualizados en caliente por el Optimizer ──────────────
# Todas las funciones de trading leen de aquí, no de las constantes globales.
# El Optimizer escribe aquí vía Redis → _config_listener → _apply_hot_params()
_P: Dict[str, float] = {}
_pending_hot_params: Dict[str, float] = {}


def _init_live_params() -> None:
    """Inicializa _P con los valores de las constantes en tiempo de arranque."""
    global _P
    _P = {
        "ENTRY_MIN":             ENTRY_MIN,
        "ENTRY_MAX":             ENTRY_MAX,
        "ENTRY_CUTOFF_S":        ENTRY_CUTOFF_S,
        "ENTRY_MAX_LATE":        ENTRY_MAX_LATE,
        "BTC_MIN_PCT":           BTC_MIN_PCT,
        "BTC_WINDOW_S":          BTC_WINDOW_S,
        "TP_FULL_PCT":           TP_FULL_PCT,
        "TP_MID_PCT":            TP_MID_PCT,
        "TP_LAST_15S_PCT":       TP_LAST_15S_PCT,
        "HOLD_SECS":             HOLD_SECS,
        "HOLD_BTC_PCT":          HOLD_BTC_PCT,
        "SL_DROP":               SL_DROP,
        "SL_FLOOR":              SL_FLOOR,
        "SL_TIME_S":             SL_TIME_S,
        "SL_LAST_30S_PCT":       SL_LAST_30S_PCT,
        "SLIP_MAX_PCT":          SLIP_MAX_PCT,
        "FOK_PRICE_BUF":         FOK_PRICE_BUF,
        "PRICE_STABILITY_SCANS": PRICE_STABILITY_SCANS,
        "STAKE_USD":             STAKE_USD,
        "THROTTLE_S":            THROTTLE_S,
        "MAX_OPEN_POS":          MAX_OPEN_POS,
        "MAX_ENTRIES_PER_ROUND": MAX_ENTRIES_PER_ROUND,
    }


def _apply_hot_params(params: dict) -> list:
    """
    Aplica nuevos parámetros del Optimizer a _P en tiempo real.
    Devuelve lista de cambios aplicados para logging.
    Si hay posición abierta, guarda en _pending_hot_params y aplica al cerrar.
    """
    global _P, _pending_hot_params
    _HOT_KEYS = set(_P.keys())

    if any(p.status == "open" for p in _open_pos):
        # Diferir — acumular en pendientes
        for key, val in params.items():
            if key in _HOT_KEYS:
                _pending_hot_params[key] = float(val)
        return ["[HOTRELOAD DIFERIDO] posición abierta — se aplicará al cerrar"]

    updated = []
    for key, val in params.items():
        if key not in _HOT_KEYS:
            continue
        old = _P.get(key)
        new = float(val)
        if old is not None and abs(new - float(old)) > 1e-9:
            _P[key] = new
            updated.append(f"{key}: {old} -> {new}")
            _linfo(f"[HOT] {key}: {old} -> {new}")
    return updated
DRY_RUN        = os.getenv("DRY_RUN", "true").lower() == "true"

# ── Credenciales CLOB (compartidas con calvinbot principal via .env) ──────────
_PRIVATE_KEY  = os.getenv("PRIVATE_KEY", "")
_WALLET_ADDR  = os.getenv("POLYMARKET_ADDRESS", "")
_CLOB_API_KEY = os.getenv("CLOB_API_KEY", "")
_CLOB_SECRET  = os.getenv("CLOB_SECRET", "")
_CLOB_PASS    = os.getenv("CLOB_PASSPHRASE", "")
_CHAIN_ID     = 137  # Polygon Mainnet
_clob5        = None  # mantenido por compatibilidad (ejecución delegada a execution_bot)

# ISSUE #4: Validación temprana de env vars en REAL_MODE
def _validate_env_vars_calvin5() -> None:
    """Fail-fast: verifica que las env vars críticas estén configuradas."""
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
        _lerr(f"❌ FALTA CONFIGURACIÓN EN REAL_MODE: {', '.join(missing)}")
        _lerr("Adiciona estas variables a tu archivo .env y reinicia.")
        import sys
        sys.exit(1)

# Polygon RPC para aprobaciones ERC1155 on-chain (gasless no disponible en py-clob-client)
POLYGON_RPC   = os.getenv("POLYGON_RPC", "https://polygon-rpc.com")

# Contratos Polymarket en Polygon (chain 137)
_CTF_TOKENS       = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # ERC1155 condicional
_CTF_EXCHANGE     = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # Exchange regular
_NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"  # Exchange neg-risk (BTC 5-min)


# ─────────────────────────────────────────────────────────────────────────────
#  LOG A FICHERO
# ─────────────────────────────────────────────────────────────────────────────

import logging as _logging

_log = _logging.getLogger("calvin5")
_log.setLevel(_logging.DEBUG)
_fh  = _logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S"))
_log.addHandler(_fh)

async def _log_unified(msg: str, level: str = "INFO") -> None:
    """Publica el mensaje al canal logs:unified para el Dashboard."""
    if _redis is None:
        return
    try:
        await _redis.publish(REDIS_CH_UNIFIED_LOG, json.dumps({
            "bot":       "calvin5",
            "level":     level,
            "msg":       msg[:300],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
    except Exception as e:
        # ISSUE #5: Fire-and-forget logging a Redis — ignorable si falla
        pass


def _linfo(msg: str) -> None:
    _log.info(msg); print(msg)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_log_unified(msg, "INFO"))
    except RuntimeError:
        pass


def _lwarn(msg: str) -> None:
    _log.warning(msg); print(msg)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_log_unified(msg, "WARN"))
    except RuntimeError:
        pass


def _lerr(msg: str) -> None:
    _log.error(msg); print(msg)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_log_unified(msg, "ERROR"))
    except RuntimeError:
        pass


async def _tg(msg: str) -> None:
    """Envía notificación a Telegram. Solo para fallos técnicos — falla silenciosamente."""
    token   = os.getenv("TG_TOKEN", "")
    chat_id = os.getenv("TG_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        url  = f"https://api.telegram.org/bot{token}/sendMessage"
        sess = _http_session or aiohttp.ClientSession()  # fallback si aún no inicializada
        await sess.post(url, json={"chat_id": chat_id, "text": f"[Calvin5] {msg}"},
                        timeout=aiohttp.ClientTimeout(total=5))
    except Exception:
        pass


def _write_json_atomic(path: str, data) -> None:
    """Escritura atómica: escribe en tmp y hace rename para evitar JSON corrupto si el proceso muere mid-write."""
    import tempfile
    p = Path(path)
    dir_ = p.parent
    try:
        with tempfile.NamedTemporaryFile("w", dir=dir_, delete=False,
                                         suffix=".tmp", encoding="utf-8") as f:
            json.dump(data, f)
            tmp_name = f.name
        os.replace(tmp_name, p)
    except Exception as exc:
        # Intento de limpieza del tmp si algo salió mal
        try:
            os.unlink(tmp_name)
        except Exception:
            pass
        raise exc


from utils import madrid_now as _madrid_now  # noqa: E402


def _in_trading_hours() -> bool:
    """True si la hora actual está en el tramo TRADING_HOUR_START–TRADING_HOUR_END (hora España).
    Soporta rangos nocturnos que cruzan medianoche (START >= END)."""
    h = _madrid_now().hour
    if TRADING_HOUR_START < TRADING_HOUR_END:
        return TRADING_HOUR_START <= h < TRADING_HOUR_END
    else:
        return h >= TRADING_HOUR_START or h < TRADING_HOUR_END


def _kelly_stake(entry_price: float) -> float:
    """
    O12: Calcula el stake óptimo usando el criterio de Kelly fraccionario.

    Fórmula (apuesta binaria):
        b = (1 / entry_price) - 1   ← beneficio neto si gana (ratio de pago)
        p = win_rate estimada de trades REAL de esta sesión
        q = 1 - p
        f* = (b*p - q) / b          ← Kelly completo
        stake = f* × KELLY_FRACTION × _session_bankroll

    Si no hay suficientes trades reales o Kelly es negativo → retorna KELLY_MIN_USD.
    Siempre clampado entre KELLY_MIN_USD y KELLY_MAX_USD.
    """
    if not KELLY_ENABLED:
        return _P["STAKE_USD"]

    real_wins   = _stats.get("real_wins",   0)
    real_losses = _stats.get("real_losses", 0)
    real_trades = real_wins + real_losses

    if real_trades < KELLY_MIN_TRADES:
        return _P["STAKE_USD"]  # insuficiente historial — usar stake base

    p = real_wins / real_trades
    q = 1.0 - p

    if entry_price <= 0 or entry_price >= 1.0:
        return _P["STAKE_USD"]

    b = (1.0 / entry_price) - 1.0
    if b <= 0:
        return KELLY_MIN_USD

    kelly_full = (b * p - q) / b
    if kelly_full <= 0:
        return KELLY_MIN_USD  # edge negativo — no entrar (pero ya filtrado arriba)

    # Bankroll aproximado: stake base × factor de escala (no tenemos saldo real aquí)
    bankroll = max(_P["STAKE_USD"] * 10, 50.0)  # conservador: 10× el stake base
    raw_stake = kelly_full * KELLY_FRACTION * bankroll

    return round(max(KELLY_MIN_USD, min(KELLY_MAX_USD, raw_stake)), 2)


def _load_window_pnl() -> None:
    """Carga el PnL del tramo horario desde disco (persiste entre reinicios del mismo día)."""
    global _window_pnl, _window_date
    try:
        path = Path(WINDOW_PNL_FILE)
        if not path.exists():
            return
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        today = _madrid_now().strftime("%Y-%m-%d")
        if data.get("date") == today:
            _window_pnl  = float(data.get("pnl", 0.0))
            _window_date = today
            if _window_pnl < 0:
                print(f"  [DRAWDOWN] PnL del tramo de hoy restaurado: ${_window_pnl:+.2f} "
                      f"(límite -${SESSION_DRAWDOWN_LIMIT:.0f})")
    except Exception:
        pass


def _save_window_pnl() -> None:
    """
    Persiste el PnL del tramo horario a disco (escritura atómica).
    ISSUE #10: También escribe a Redis como fuente autoritativa.
    """
    try:
        today = _window_date or _madrid_now().strftime("%Y-%m-%d")
        data = {"date": today, "pnl": _window_pnl}
        _write_json_atomic(WINDOW_PNL_FILE, data)
        
        # ISSUE #10: Sincronizar a Redis (fuente única de verdad)
        if _redis is not None:
            try:
                key = f"pnl:window:{today}"
                loop = asyncio.get_running_loop()
                loop.create_task(_redis.setex(key, 86400 + 3600, str(_window_pnl)))
                _linfo(f"[PnL] Guardado a Redis: ${_window_pnl:+.2f}")
            except Exception as e:
                _lwarn(f"[PnL] Error sincronizando a Redis: {e}")
    except Exception:
        pass


def _update_window_pnl(pnl: float) -> None:
    """Acumula PnL en el tramo horario y comprueba el límite de drawdown."""
    global _window_pnl, _window_date, _drawdown_hit
    today = _madrid_now().strftime("%Y-%m-%d")
    if _window_date != today:
        _window_pnl  = 0.0
        _window_date = today
    _window_pnl += pnl
    _save_window_pnl()
    if not _drawdown_hit and _window_pnl < -SESSION_DRAWDOWN_LIMIT:
        _drawdown_hit = True
        msg = (f"🛑 DRAWDOWN LÍMITE alcanzado: ${_window_pnl:+.2f} "
               f"(máx -${SESSION_DRAWDOWN_LIMIT:.0f}) — bot pausado hasta mañana 14h")
        _lerr(f"  [DRAWDOWN] {msg}")
        asyncio.ensure_future(_tg(msg))


# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Market:
    slug:          str
    up_token_id:   str
    down_token_id: str
    end_ts:        float


@dataclass
class Position:
    id:           str
    side:         str
    entry_price:  float
    size_usd:     float
    entry_time:   float
    peak_price:   float = 0.0
    status:       str   = "open"
    pnl:          float = 0.0
    exit_price:   Optional[float] = None
    exit_reason:  Optional[str]   = None
    market_slug:  str   = ""
    token_id:     str   = ""
    size_tokens:  float = 0.0   # tokens reales recibidos del BUY (makingAmount)
    fee_usd:      float = 0.0   # comisión pagada en la salida (solo SELL anticipado)


# ─────────────────────────────────────────────────────────────────────────────
#  ESTADO GLOBAL
# ─────────────────────────────────────────────────────────────────────────────

_market:      Optional[Market] = None
_prices:      Dict[str, Optional[float]] = {"UP": None, "DOWN": None}
_ws_connected: bool            = False
_open_pos:    List[Position]   = []
_pos_lock:    asyncio.Lock     = asyncio.Lock()  # fix 3: protege mutaciones concurrentes de _open_pos
_closed:     List[Position]   = []
_last_entry:    float = 0.0
_round_entries: int   = 0    # entradas realizadas en el mercado actual
_round_slug:    str   = ""   # slug del mercado actual para detectar cambio de ronda
_scan_num:   int              = 0
_stats = {
    "wins": 0, "losses": 0, "pnl": 0.0, "rounds": 0, "skipped_momentum": 0,
    # Stats separados por modo para no mezclar dry run con real money
    "dry_wins": 0, "dry_losses": 0, "dry_pnl": 0.0,
    "real_wins": 0, "real_losses": 0, "real_pnl": 0.0,
}
_session_pnl: float = 0.0   # PnL solo de esta sesión (no incluye historial CSV)
_price_updated: Optional[asyncio.Event] = None   # inicializado en main()
_price_history: Dict[str, List[float]] = {"UP": [], "DOWN": []}  # últimos precios para filtro estabilidad
_bid_prices:    Dict[str, Optional[float]] = {"UP": None, "DOWN": None}  # mejor bid por side (I8 spread filter)
_ask_prices:    Dict[str, Optional[float]] = {"UP": None, "DOWN": None}  # mejor ask por side (I8 spread filter)
_round_start_prices: Dict[str, Optional[float]] = {"UP": None, "DOWN": None}  # precio al inicio de la ronda (I9 priced-in guard)
_round_start_ts:     Dict[str, float]           = {"UP": 0.0, "DOWN": 0.0}   # fix 6: ts cuando se registró el primer precio
_closing_ids:         set = set()  # posiciones en proceso de cierre (anti-double-close, por pos.id)
_closing_token_ids:   set = set()  # token_ids de posiciones en cierre (fix: lookup O(1) en reconciliation)
_sell_retry_after: Dict[str, float] = {}   # pos.id → timestamp mínimo para reintentar SELL (anti-loop)
_sell_retry_count: Dict[str, int]   = {}   # pos.id → número de reintentos SELL fallidos consecutivos
_reconcile_sell_verify: Dict[str, float] = {}  # token_id → ts del SELL exitoso (verificación post-sell 60s)
_window_pnl:       float = 0.0    # PnL acumulado del tramo 14-22h España (persiste entre reinicios del mismo día)
_window_date:      str   = ""     # fecha España del tramo activo (YYYY-MM-DD)
_drawdown_hit:     bool  = False  # True cuando se alcanza SESSION_DRAWDOWN_LIMIT → bloquea nuevas entradas
_stream_warned:    bool  = False  # log único del fallback pub/sub (Redis < 5.0)
_bot_last_hb: Dict[str, float]  = {}  # última vez que cada bot envió heartbeat {bot: timestamp}
_btc_price:    Optional[float] = None  # Precio actual de BTC (para dashboard)
_btc_momentum: Optional[float] = None  # Momentum de BTC (para dashboard)


# ─────────────────────────────────────────────────────────────────────────────
#  REDIS — tareas de comunicación con execution_bot y dynamic_optimizer
# ─────────────────────────────────────────────────────────────────────────────

async def _receipt_listener() -> None:
    """
    Escucha los recibos de ejecución que publica execution_bot en Redis.
    Canal: execution:receipts:{signal_id}
    Resuelve los Futures que open_position y _do_close_position están esperando.
    """
    global _redis
    while True:
        try:
            if _redis is None:
                await asyncio.sleep(1)
                continue
            # Suscripción con patrón para capturar todos los signal_id
            pubsub = _redis.pubsub()
            await pubsub.psubscribe(f"{REDIS_CH_RECEIPTS}:*")
            _linfo("[REDIS] Receipt listener activo — esperando confirmaciones del executor")
            async for msg in pubsub.listen():
                if msg["type"] != "pmessage":
                    continue
                try:
                    receipt = json.loads(msg["data"])
                    sig_id  = receipt.get("signal_id", "")
                    future  = _pending_receipts.get(sig_id)
                    if future and not future.done():
                        future.set_result(receipt)
                        _linfo(
                            f"[REDIS] Recibo recibido {sig_id[:20]} → "
                            f"status={receipt.get('status')} "
                            f"fill={receipt.get('fill_price', 0):.4f}"
                        )
                except Exception as exc:
                    _lwarn(f"[REDIS] Error parseando recibo: {exc}")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            _lwarn(f"[REDIS] Receipt listener error: {exc} — reconectando en 3s")
            await asyncio.sleep(3)


async def _config_listener() -> None:
    """
    Escucha actualizaciones de configuración del dynamic_optimizer.
    Canal: config:update:calculation_bot
    Aplica hot-reload de los parámetros de trading sin reiniciar el bot.
    """
    global _redis
    while True:
        try:
            if _redis is None:
                await asyncio.sleep(1)
                continue
            pubsub = _redis.pubsub()
            await pubsub.subscribe(REDIS_CH_CONFIG)
            _linfo("[REDIS] Config listener activo — esperando actualizaciones del optimizer")
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    payload = json.loads(msg["data"])
                    params  = payload.get("params", {})
                    _linfo(f"[CONFIG] Recibido del optimizer: {list(params.keys())}")
                    updated = _apply_hot_params(params)
                    if updated:
                        _linfo(f"[CONFIG] Hot-reload aplicado:\n" + "\n".join(f"  {u}" for u in updated))
                except Exception as exc:
                    _lwarn(f"[REDIS] Error aplicando config: {exc}")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            _lwarn(f"[REDIS] Config listener error: {exc} — reconectando en 3s")
            await asyncio.sleep(3)


async def _control_listener() -> None:
    """
    Escucha comandos de control del Dashboard.
    Canal: signals:control
    Comandos: PAUSE_ENTRIES / RESUME_ENTRIES
    """
    global _redis, _entries_paused
    while True:
        try:
            if _redis is None:
                await asyncio.sleep(1)
                continue
            pubsub = _redis.pubsub()
            await pubsub.subscribe(REDIS_CH_CONTROL)
            _linfo("[REDIS] Control listener activo — esperando comandos del dashboard")
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    payload = json.loads(msg["data"])
                    cmd = payload.get("command", "")
                    if cmd == "PAUSE_ENTRIES":
                        _entries_paused = True
                        _lwarn("[CONTROL] PAUSE_ENTRIES activo — sin nuevas entradas hasta RESUME")
                    elif cmd == "RESUME_ENTRIES":
                        _entries_paused = False
                        _linfo("[CONTROL] RESUME_ENTRIES — entradas habilitadas")
                    else:
                        _lwarn(f"[CONTROL] Comando desconocido: {cmd}")
                except Exception as exc:
                    _lwarn(f"[REDIS] Error en control listener: {exc}")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            _lwarn(f"[REDIS] Control listener error: {exc} — reconectando en 3s")
            await asyncio.sleep(3)


async def _bot_monitor_task() -> None:
    """
    Suscribe a 'health:heartbeats' y registra la última vez que cada bot
    publicó su latido. Alimenta _bot_last_hb para los indicadores del scan loop.
    """
    global _redis, _bot_last_hb
    while True:
        try:
            if _redis is None:
                await asyncio.sleep(2)
                continue
            pubsub = _redis.pubsub()
            await pubsub.subscribe(REDIS_CH_HEARTBEATS)
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    data = json.loads(msg["data"])
                    bot  = data.get("bot", "")
                    if bot and bot != "calculator":   # no registrar el propio Calvin5
                        _bot_last_hb[bot] = time.time()
                except Exception:
                    pass
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(3)


async def _calc_heartbeat() -> None:
    """
    Publica latido y estado completo del calculador cada 5s.
    Canales:
      health:heartbeats      → Watchdog (latido simple)
      state:calculator:latest → Watchdog y Optimizer (estado completo auditado)
    """
    global _redis, _btc_price, _btc_momentum
    while True:
        try:
            if _redis is not None:
                now = datetime.now(timezone.utc).isoformat()
                
                # Leer BTC data actual
                btc_price_now = bp.get_btc_price() if BTC_REQUIRED else None
                btc_pct_now, _ = bp.get_momentum(BTC_WINDOW_S) if BTC_REQUIRED else (None, None)
                _btc_price = btc_price_now
                _btc_momentum = btc_pct_now

                # ── Latido simple para el Watchdog ─────────────────────────
                hb = {
                    "bot":        "calculator",
                    "status":     "online",
                    "dry_run":    DRY_RUN,
                    "open_pos":   len(_open_pos),
                    "ws_ok":      _ws_connected,
                    "timestamp":  now,
                }
                await _redis.publish(REDIS_CH_HEARTBEATS, json.dumps(hb))

                # ── Estado completo para auditoría (Watchdog + Optimizer) ──
                state = {
                    "timestamp":       now,
                    "dry_run":         DRY_RUN,
                    "market_slug":     _market.slug if _market else None,
                    "market_end_ts":   _market.end_ts if _market else None,
                    "prices": {
                        "UP":   _prices.get("UP"),
                        "DOWN": _prices.get("DOWN"),
                    },
                    "ws_connected":    _ws_connected,
                    "open_positions": [
                        {
                            "id":          p.id,
                            "side":        p.side,
                            "entry_price": p.entry_price,
                            "size_usd":    p.size_usd,
                            "size_tokens": p.size_tokens,
                            "token_id":    p.token_id,
                            "market_slug": p.market_slug,
                            "entry_time":  p.entry_time,
                            "peak_price":  p.peak_price,
                            "status":      p.status,
                        }
                        for p in _open_pos
                    ],
                    "session_pnl":     _session_pnl,
                    "window_pnl":      _window_pnl,
                    "drawdown_hit":    _drawdown_hit,
                    "stats":           _stats,
                    "params": {k: v for k, v in _P.items()},
                    "btc_price":       _btc_price,
                    "btc_momentum":    _btc_momentum,
                }
                # Guardar en key persistente (Watchdog puede leerla sin pub/sub)
                await _redis.set(
                    "state:calculator:latest",
                    json.dumps(state),
                    ex=120,  # expira en 120s — alineado con WD_HEARTBEAT_MAX_S
                )
        except Exception:
            pass
        # Escribir al log en cada ciclo → actualiza mtime de calvin5.log para el Watchdog
        _linfo(f"[HB] alive | pos={len(_open_pos)} | pnl={_window_pnl:+.2f}")
        await asyncio.sleep(5)



# ─────────────────────────────────────────────────────────────────────────────
#  DETECCIÓN DE MERCADO
# ─────────────────────────────────────────────────────────────────────────────

def _slot_ts(offset: int = 0) -> int:
    """Retorna el timestamp de inicio del slot actual + offset slots."""
    base = int(time.time()) // SLOT_SECONDS
    return (base + offset) * SLOT_SECONDS


def _fetch_event(slug: str) -> Optional[Dict]:
    try:
        r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=8)
        r.raise_for_status()
        data = r.json()
        return data[0] if data else None
    except Exception:
        return None


def detect_market() -> Optional[Market]:
    """Encuentra el mercado 5-min activo más próximo.

    El slug usa el timestamp de INICIO del slot: btc-updown-5m-{start_ts}
    El end_ts real = start_ts + SLOT_SECONDS
    """
    now = time.time()
    for offset in (0, 1, 2):
        start_ts = _slot_ts(offset)
        slug     = f"{SLUG_PATTERN}-{start_ts}"
        event    = _fetch_event(slug)
        if not event or event.get("archived"):
            continue

        mkt = event.get("markets", [{}])[0]
        if mkt.get("closed") and mkt.get("resolved"):
            continue

        # end_ts = inicio del slot + duración del slot
        end_ts = start_ts + SLOT_SECONDS
        if end_ts <= now + 5:
            continue

        raw_tokens   = mkt.get("clobTokenIds", [])
        token_ids    = json.loads(raw_tokens) if isinstance(raw_tokens, str) else raw_tokens
        raw_outcomes = mkt.get("outcomes", "[]")
        outcomes     = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes

        up_id = down_id = None
        for i, o in enumerate(outcomes):
            if i >= len(token_ids):
                break
            s = str(o).lower()
            if s == "up":
                up_id   = token_ids[i]
            elif s == "down":
                down_id = token_ids[i]
        if not up_id and len(token_ids) >= 1:
            up_id   = token_ids[0]
        if not down_id and len(token_ids) >= 2:
            down_id = token_ids[1]
        if not up_id or not down_id:
            continue

        secs = end_ts - now
        ts_utc = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime("%H:%M:%S UTC")
        print(f"  [MKT] {slug} | cierra en {secs:.0f}s ({ts_utc})")
        return Market(slug=slug, up_token_id=up_id, down_token_id=down_id, end_ts=end_ts)
    return None


def _fetch_resolution(market: Market) -> Tuple[Optional[float], Optional[float]]:
    """Obtiene precios finales de resolución (UP, DOWN) para un mercado cerrado."""
    try:
        event = _fetch_event(market.slug)
        if not event:
            return None, None
        mkt = event.get("markets", [{}])[0]
        raw = mkt.get("outcomePrices", "[]")
        prices = json.loads(raw) if isinstance(raw, str) else raw
        if len(prices) >= 2:
            return float(prices[0]), float(prices[1])
    except Exception:
        pass
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
#  PRECIOS CLOB
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_midpoint(token_id: str) -> Optional[float]:
    """Midpoint vía REST usando HTTP session pool (no bloqueante)."""
    try:
        sess = _http_session or aiohttp.ClientSession()
        async with sess.get(f"{CLOB_API}/midpoint",
                            params={"token_id": token_id},
                            timeout=aiohttp.ClientTimeout(total=4)) as resp:
            data = await resp.json(content_type=None)
            mid = data.get("mid")
            return float(mid) if mid is not None else None
    except Exception:
        return None


async def _fetch_price_buy(token_id: str) -> Optional[float]:
    """Precio de compra real vía /price?side=BUY usando HTTP session pool (no bloqueante)."""
    try:
        sess = _http_session or aiohttp.ClientSession()
        async with sess.get(f"{CLOB_API}/price",
                            params={"token_id": token_id, "side": "BUY"},
                            timeout=aiohttp.ClientTimeout(total=4)) as resp:
            data = await resp.json(content_type=None)
            p = data.get("price")
            return float(p) if p is not None else None
    except Exception:
        return None


async def _fetch_orderbook_depth(token_id: str, near_price: float, depth_usd: float = 20.0) -> float:
    """
    O13: Consulta el order book CLOB y calcula la liquidez disponible en el lado
    BUY (asks) dentro de un rango de ±5% del precio actual.

    Devuelve el total en USD de liquidez disponible cerca del precio.
    Si hay error o el book no está disponible → devuelve float('inf') para NO bloquear
    entrada (fix 2: devolver depth_usd era incorrecto porque un book vacío pasaba el
    filtro solo si depth_usd >= MIN_BOOK_DEPTH, lo que dependía del stake, no del book).
    """
    try:
        sess = _http_session or aiohttp.ClientSession()
        async with sess.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=aiohttp.ClientTimeout(total=3),
        ) as resp:
            data = await resp.json(content_type=None)

        asks = data.get("asks", [])
        if not asks:
            return float("inf")  # book vacío → no bloquear (liquidez desconocida)

        price_lo = near_price * 0.95
        price_hi = near_price * 1.05
        total_usd = 0.0
        for level in asks:
            try:
                lp   = float(level.get("price", 0) or 0)
                lsz  = float(level.get("size",  0) or 0)
                if price_lo <= lp <= price_hi:
                    total_usd += lp * lsz
            except (ValueError, TypeError):
                continue
        return total_usd
    except Exception:
        return float("inf")  # fallo de red → no bloquear entrada


def _parse_ws_message(data) -> None:
    """Parsea mensajes del WebSocket CLOB y actualiza _prices en tiempo real.

    Soporta:
      - Mensajes como lista  → itera sobre cada elemento
      - event_type "book"    → calcula mid de bids[0]/asks[0]
      - event_type "price_change" → usa changes[0]["price"]
      - event_type "last_trade_price" → usa data["price"] directamente
    """
    if isinstance(data, list):
        for item in data:
            _parse_ws_message(item)
        return

    if not isinstance(data, dict) or not _market:
        return

    asset = data.get("asset_id", "")
    if not asset:
        return

    event_type = data.get("event_type", "")
    price: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None

    if event_type == "book":
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if bids and asks:
            try:
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                price = (best_bid + best_ask) / 2
            except (KeyError, ValueError, IndexError):
                pass
        elif bids:
            try:
                price = float(bids[0]["price"])
                best_bid = price
            except (KeyError, ValueError):
                pass

    elif event_type == "price_change":
        for ch in data.get("changes", []):
            try:
                price = float(ch["price"])
                break
            except (KeyError, ValueError):
                pass

    elif event_type == "last_trade_price":
        try:
            price = float(data["price"])
        except (KeyError, ValueError):
            pass

    else:
        # fallback: intenta bids/asks directamente (formato alternativo)
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if bids and asks:
            try:
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                price = (best_bid + best_ask) / 2
            except (KeyError, ValueError, IndexError):
                pass

    if price is not None:
        if asset == _market.up_token_id:
            side_key = "UP"
        elif asset == _market.down_token_id:
            side_key = "DOWN"
        else:
            return
        _prices[side_key] = price
        if best_bid is not None:
            _bid_prices[side_key] = best_bid
        if best_ask is not None:
            _ask_prices[side_key] = best_ask
        # Historial de estabilidad de precio
        hist = _price_history[side_key]
        hist.append(price)
        if len(hist) > PRICE_STABILITY_SCANS + 2:
            hist.pop(0)
        # Notifica al exit_watcher para que evalúe SL/TP inmediatamente
        if _price_updated is not None:
            _price_updated.set()


async def _rest_price_refresher() -> None:
    """Refresca precios vía REST cada 3s como backup del WebSocket.

    Usa /price?side=BUY (capa 1) → /midpoint (capa 2) como botemi.
    Mantiene precios actualizados incluso cuando el WS no envía mensajes.
    También actualiza _price_history para que el filtro de estabilidad
    no quede bloqueado cuando el WS tarda en mandar eventos.
    """
    while True:
        await asyncio.sleep(3)
        if not _market:
            continue
        for tok, side in ((_market.up_token_id, "UP"), (_market.down_token_id, "DOWN")):
            p = await _fetch_price_buy(tok)
            if p is None:
                p = await _fetch_midpoint(tok)
            if p is not None:
                _prices[side] = p
                # Alimenta el historial de estabilidad (mismo que _parse_ws_message)
                hist = _price_history[side]
                hist.append(p)
                if len(hist) > PRICE_STABILITY_SCANS + 2:
                    hist.pop(0)
                if _price_updated is not None:
                    _price_updated.set()

            # Fix 5: actualizar bid/ask desde REST como fallback cuando el WS no envía book events
            try:
                sess = _http_session or aiohttp.ClientSession()
                async with sess.get(
                    f"{CLOB_API}/book",
                    params={"token_id": tok},
                    timeout=aiohttp.ClientTimeout(total=3),
                ) as resp_b:
                    book = await resp_b.json(content_type=None)
                raw_bids = book.get("bids", [])
                raw_asks = book.get("asks", [])
                if raw_bids:
                    _bid_prices[side] = max(
                        float(b["price"]) for b in raw_bids if b.get("price")
                    )
                if raw_asks:
                    _ask_prices[side] = min(
                        float(a["price"]) for a in raw_asks if a.get("price")
                    )
            except Exception:
                pass  # fallo silencioso — valores previos se mantienen


async def run_prices_ws() -> None:
    """WebSocket del CLOB de Polymarket para precios en tiempo real.

    Reconecta automáticamente con backoff. Si el mercado cambia,
    re-suscribe a los nuevos tokens. El REST poller actúa de fallback
    para rellenar precios antes de que el WS conecte.
    """
    global _ws_connected
    import random
    backoff        = 1   # empezar rápido en el primer intento
    WS_BACKOFF_MAX = 60  # máximo 60s entre reintentos

    while True:
        # Espera a que haya mercado activo
        if not _market:
            await asyncio.sleep(2)
            continue

        up_id   = _market.up_token_id
        down_id = _market.down_token_id

        # Seed inicial de precios vía REST mientras conecta el WS
        for tok, side in ((up_id, "UP"), (down_id, "DOWN")):
            mid = await _fetch_midpoint(tok)
            if mid is not None:
                _prices[side] = mid

        try:
            async with websockets.connect(
                CLOB_WS_URL,
                ping_interval=20, ping_timeout=10, close_timeout=5,
            ) as ws:
                sub = json.dumps({"auth": {}, "type": "subscribe",
                                  "markets": [], "assets": [up_id, down_id]})
                await ws.send(sub)
                _ws_connected = True
                backoff = 1   # reset tras conexión exitosa
                print(f"\n  [WS] CLOB conectado — UP:{up_id[:12]}... DOWN:{down_id[:12]}...")

                async for raw in ws:
                    try:
                        _parse_ws_message(json.loads(raw))
                    except Exception:
                        pass

                    # Re-suscribe si cambió el mercado
                    if _market and (_market.up_token_id != up_id or
                                    _market.down_token_id != down_id):
                        up_id   = _market.up_token_id
                        down_id = _market.down_token_id
                        sub = json.dumps({"auth": {}, "type": "subscribe",
                                          "markets": [], "assets": [up_id, down_id]})
                        await ws.send(sub)
                        print(f"  [WS] Re-suscrito nuevo mercado {up_id[:12]}...")

        except Exception as exc:
            _ws_connected = False
            metrics.inc_ws_reconnect()
            # Jitter ±20% para evitar thundering herd si múltiples instancias reconectan
            jitter  = random.uniform(0.8, 1.2)
            wait    = min(backoff * jitter, WS_BACKOFF_MAX)
            print(f"  [WS] Desconectado: {exc} — reconectando en {wait:.1f}s...")
            await asyncio.sleep(wait)
            backoff = min(backoff * 2, WS_BACKOFF_MAX)
            continue

        _ws_connected = False


# ─────────────────────────────────────────────────────────────────────────────
#  SEÑALES
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_signal(debug: bool = False) -> Tuple[Optional[str], Optional[str]]:
    """Evalúa si hay señal de entrada.

    Retorna (side, btc_info) donde side = 'UP'/'DOWN'/None.
    Si debug=True imprime la razón exacta del bloqueo para cada side.
    """
    global _stats
    if not _market:
        return None, None

    now       = time.time()
    secs_left = _market.end_ts - now

    if secs_left > SNIPER_WINDOW_S or secs_left <= 30:
        return None, None

    # Filtro horario: solo opera entre TRADING_HOUR_START y TRADING_HOUR_END (hora España)
    if not _in_trading_hours():
        if debug:
            now_es = _madrid_now()
            print(f"  [DBG] FUERA DE HORARIO: {now_es.hour:02d}:{now_es.minute:02d}h España "
                  f"(ventana {TRADING_HOUR_START}h-{TRADING_HOUR_END}h)")
        return None, None

    # Control del Dashboard — PAUSE_ENTRIES bloquea nuevas entradas
    if _entries_paused:
        if debug:
            print("  [DBG] PAUSE_ENTRIES activo — sin nuevas entradas (comando del dashboard)")
        return None, None

    # Drawdown diario: bloquear si se alcanzó el límite de pérdida del tramo
    if _drawdown_hit:
        if debug:
            print(f"  [DBG] BLOQUEADO drawdown: ${_window_pnl:+.2f} < -${SESSION_DRAWDOWN_LIMIT:.0f}")
        return None, None

    # C4: límites globales semanales/mensuales — no se resetean cada día
    _lt_status, _lt_msg = loss_tracker.check()
    if _lt_status == loss_tracker.STATUS_BREACHED:
        if debug:
            print(f"  [DBG] BLOQUEADO límite global: {_lt_msg}")
        return None, None

    # Circuit breaker: pausa si pérdidas de ESTA SESIÓN superan el límite
    if _session_pnl < -DAILY_LOSS_LIMIT:
        if debug:
            print(f"  [DBG] BLOQUEADO circuit_breaker session_pnl={_session_pnl:+.2f} < -{DAILY_LOSS_LIMIT}")
        return None, None

    n_open = sum(1 for p in _open_pos if p.status == "open")
    if n_open >= int(_P["MAX_OPEN_POS"]):
        if debug:
            print(f"  [DBG] BLOQUEADO max_pos open={n_open}/{int(_P['MAX_OPEN_POS'])}")
        return None, None

    # Límite de entradas por ronda — resetear si el mercado cambió
    cur_slug = _market.slug if _market else ""
    if cur_slug != _round_slug:
        pass  # reset se hace en open_position al confirmar entrada
    elif _round_entries >= int(_P["MAX_ENTRIES_PER_ROUND"]):
        if debug:
            print(f"  [DBG] BLOQUEADO max_entradas_ronda {_round_entries}/{int(_P['MAX_ENTRIES_PER_ROUND'])} slug={cur_slug[-12:]}")
        return None, None

    if now - _last_entry < _P["THROTTLE_S"]:
        if debug:
            print(f"  [DBG] BLOQUEADO throttle last={now-_last_entry:.1f}s < {_P['THROTTLE_S']}s")
        return None, None

    # ── Anti-late-squeeze: sin entradas en los últimos ENTRY_CUTOFF_S segundos ──
    if secs_left <= _P["ENTRY_CUTOFF_S"]:
        if debug:
            print(f"  [DBG] BLOQUEADO cutoff secs_left={secs_left:.0f}s <= {_P['ENTRY_CUTOFF_S']}s")
        return None, None

    # ── ENTRY_MAX dinámico: más restrictivo cuando queda poco tiempo ──────────
    entry_max = _P["ENTRY_MAX_LATE"] if secs_left <= _P["ENTRY_CUTOFF_S"] * 2 else _P["ENTRY_MAX"]

    # ── Modo BTC Momentum Sniper ──────────────────────────────────────────────
    for side in ("UP", "DOWN"):
        price = _prices.get(side)
        if price is None:
            if debug:
                print(f"  [DBG] {side}: sin precio")
            continue
        if not (_P["ENTRY_MIN"] <= price <= entry_max):
            if debug:
                print(f"  [DBG] {side}={price:.4f}: fuera rango [{_P['ENTRY_MIN']},{entry_max:.2f}]  entry_max={'late' if entry_max==_P['ENTRY_MAX_LATE'] else 'normal'}")
            continue

        # I8: Filtro de spread bid/ask — mercados ilíquidos tienen spreads amplios
        # Fix 5: BLOQUEAR si bid/ask desconocidos (REST fallback en _rest_price_refresher)
        bid = _bid_prices.get(side)
        ask = _ask_prices.get(side)
        if bid is None or ask is None:
            if debug:
                print(f"  [DBG] {side}: bid/ask desconocidos (esperando datos de book) — bloqueando")
            continue
        if ask > bid:
            spread = ask - bid
            if spread > MAX_SPREAD:
                if debug:
                    print(f"  [DBG] {side}: spread={spread:.4f} > MAX_SPREAD={MAX_SPREAD:.2f} — mercado ilíquido")
                continue

        # I9: Momentum priced-in guard — evita entrar cuando el precio ya se movió
        #     demasiado desde el inicio de la ronda (el momentum ya está descontado)
        if _round_start_prices[side] is None:
            _round_start_prices[side] = price        # primera observación en esta ronda
            _round_start_ts[side]     = time.time()  # fix 6: registrar cuándo se capturó
        else:
            rsp = _round_start_prices[side]
            if rsp and rsp > 0:
                # Fix 6: skip I9 si el primer precio se capturó tarde (bot tardó >60s en arrancar)
                if _market and _round_start_ts[side] > 0:
                    round_start_real = _market.end_ts - 300  # ronda de 5 min
                    elapsed = _round_start_ts[side] - round_start_real
                    if elapsed > 60:
                        _linfo(
                            f"[I9] {side}: guard no fiable — precio registrado "
                            f"{elapsed:.0f}s después del inicio de ronda — omitiendo check"
                        )
                        # No aplicar I9: el precio de referencia no es el inicio real de la ronda
                    else:
                        move_pct = abs(price - rsp) / rsp
                        if move_pct > 0.08:
                            if debug:
                                print(f"  [DBG] {side}: priced-in move={move_pct:.1%} desde inicio ronda "
                                      f"({rsp:.4f}→{price:.4f}) > 8%")
                            continue
                else:
                    move_pct = abs(price - rsp) / rsp
                    if move_pct > 0.08:
                        if debug:
                            print(f"  [DBG] {side}: priced-in move={move_pct:.1%} desde inicio ronda "
                                  f"({rsp:.4f}→{price:.4f}) > 8%")
                        continue

        # Filtro de estabilidad: precio debe llevar ≥ PRICE_STABILITY_SCANS puntos
        # en el rango de entrada (evita entrar en picos manipulados)
        hist = _price_history.get(side, [])
        stab_scans = int(_P["PRICE_STABILITY_SCANS"])
        if len(hist) < stab_scans:
            if debug:
                print(f"  [DBG] {side}={price:.4f}: hist insuficiente {len(hist)}/{stab_scans}")
            continue
        recent = hist[-stab_scans:]
        if not all(_P["ENTRY_MIN"] <= p <= entry_max for p in recent):
            out = [p for p in recent if not (_P["ENTRY_MIN"] <= p <= entry_max)]
            if debug:
                print(f"  [DBG] {side}={price:.4f}: inestable hist={[f'{p:.3f}' for p in recent]} fuera={[f'{p:.3f}' for p in out]}")
            continue

        if BTC_REQUIRED:
            if not bp.is_connected():
                if debug:
                    print(f"  [DBG] {side}: BTC feed desconectado")
                continue
            if not bp.has_enough_history(min_window_s=_P["BTC_WINDOW_S"] * 0.5):
                if debug:
                    print(f"  [DBG] {side}: BTC sin historial suficiente")
                continue
            btc_pct, btc_dir = bp.get_momentum(window_s=_P["BTC_WINDOW_S"])
            if btc_dir is None or abs(btc_pct) < _P["BTC_MIN_PCT"]:
                if debug:
                    print(f"  [DBG] {side}: momentum insuficiente btc={btc_pct:+.4f}% dir={btc_dir} min={_P['BTC_MIN_PCT']}%")
                _stats["skipped_momentum"] += 1
                continue
            if btc_dir != side:
                if debug:
                    print(f"  [DBG] {side}={price:.4f}: dirección BTC={btc_dir} no coincide con side={side} (btc={btc_pct:+.4f}%)")
                continue
            btc_info = f"BTC:{btc_pct:+.3f}%/{int(_P['BTC_WINDOW_S'])}s"
        else:
            btc_info = "BTC:off"

        return side, btc_info

    return None, None


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRADAS / SALIDAS
# ─────────────────────────────────────────────────────────────────────────────

async def _publish_signal(signal: Dict) -> None:
    """
    Publica una señal de trading al execution_bot via Redis Stream (at-least-once).
    Si Redis < 5.0 no soporta Streams, cae automáticamente a pub/sub (lo avisa una sola vez).
    """
    global _redis, _stream_warned
    if _redis is None:
        _lerr("[REDIS] Sin conexión Redis — señal no enviada")
        return
    signal["_ts_unix"] = time.time()
    try:
        await _redis.xadd(
            REDIS_STREAM_SIGNALS,
            {"data": json.dumps(signal, ensure_ascii=False)},
            maxlen=500,
            approximate=True,
        )
    except Exception as exc:
        if not _stream_warned:
            _lwarn(f"[REDIS] Stream no disponible, usando pubsub fallback (Redis < 5.0): {exc}")
            _stream_warned = True
        # Fallback: pub/sub legacy para no perder la señal
        try:
            await _redis.publish(REDIS_CH_SIGNALS, json.dumps(signal, ensure_ascii=False))
        except Exception:
            pass


async def _await_receipt(signal_id: str) -> Optional[Dict]:
    """
    Espera hasta EXEC_RECEIPT_TIMEOUT segundos el recibo del execution_bot.
    ISSUE #1: Primero espera un ACK (confirmación de entrega), luego el receipt (resultado).
    
    El receipt listener (tarea paralela) resuelve el Future cuando llega el recibo.
    Devuelve el dict del recibo o None si timeout/error.
    """
    loop   = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    _pending_receipts[signal_id] = future
    try:
        # ISSUE #1: Esperar primero confirmación de que Executor recibió la señal
        ack_received = False
        ack_timeout = EXEC_RECEIPT_TIMEOUT * 0.2  # 20% del timeout para ACK (1.6s para default 8s)
        ack_start = time.time()
        while time.time() - ack_start < ack_timeout and _redis is not None:
            try:
                ack_stream = f"ack:signals:{signal_id}"
                entries = await _redis.xrevrange(ack_stream, count=1)
                if entries:
                    ack_received = True
                    _lwarn(f"[ACK] Executor confirmó recepción de señal {signal_id[:16]}")
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        
        if not ack_received:
            _lwarn(f"[ACK] No se recibió ACK de executor para {signal_id[:16]} en {ack_timeout:.1f}s (continuando...)")
        
        # Luego esperar el receipt final del processing
        receipt = await asyncio.wait_for(
            asyncio.shield(future), timeout=EXEC_RECEIPT_TIMEOUT)
        return receipt
    except asyncio.TimeoutError:
        _lwarn(f"[REDIS] Timeout esperando recibo para {signal_id[:16]} ({EXEC_RECEIPT_TIMEOUT}s)")
        return None
    finally:
        _pending_receipts.pop(signal_id, None)


async def open_position(side: str, btc_info: str) -> Optional[Position]:
    """
    Calculador: publica señal BUY al execution_bot y espera el recibo de fill.
    Ya no llama directamente al CLOB — eso lo hace execution_bot.
    """
    global _last_entry
    price = _prices.get(side)
    if price is None:
        return None

    tok_id   = (_market.up_token_id if side == "UP"
                else _market.down_token_id) if _market else ""
    mode_tag = "[DRY]" if DRY_RUN else "[REAL]"
    sig_id   = f"SIG_{side}_{int(time.time() * 1000)}"
    pos_id   = f"{side}5_{int(time.time() * 1000)}"

    # O12: Kelly criterion — ajustar stake dinámicamente según historial de trades
    stake = _kelly_stake(price)

    # Verificar que el stake genera suficientes tokens para poder hacer SELL (siempre, incluso en MICRO_TEST)
    # Sin esta guard, el bot compra posiciones que Polymarket rechaza al vender (mínimo 5 tokens).
    estimated_tokens = stake / price
    if estimated_tokens < MIN_TOKENS_BUY:
        _lwarn(
            f"[BUY] Rechazado: ${stake:.2f} a {price:.4f} = {estimated_tokens:.2f} tokens "
            f"< mínimo {MIN_TOKENS_BUY:.0f} — sube STAKE_USD_OVERRIDE a "
            f"${MIN_TOKENS_BUY * price:.2f}+"
        )
        return None

    # O13: Verificar liquidez en el order book antes de entrar
    depth = await _fetch_orderbook_depth(tok_id, price, depth_usd=stake * 2)
    if depth < stake:
        _lwarn(f"[OB] Liquidez insuficiente: ${depth:.2f} cerca de {price:.4f} < stake ${stake:.2f} — entrada cancelada")
        return None

    # Publicar señal BUY → execution_bot
    signal = {
        "action":        "BUY",
        "signal_id":     sig_id,
        "position_id":   pos_id,
        "side":          side,
        "token_id":      tok_id,
        "price":         price,
        "size_usd":      stake,
        "market_slug":   _market.slug if _market else "",
        "market_end_ts": _market.end_ts if _market else 0,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
    }
    await _publish_signal(signal)
    _linfo(f"[CALC] BUY señal publicada → {side} @ {price:.4f} | sig={sig_id[:20]}")

    # Esperar fill del execution_bot
    receipt = await _await_receipt(sig_id)
    if receipt is None or receipt.get("status") != "FILLED":
        err = receipt.get("error", "timeout") if receipt else "timeout"
        _lwarn(f"[CALC] BUY {side} no ejecutado: {err}")
        return None

    # Construir posición con datos reales del fill
    actual_price    = float(receipt.get("fill_price", price))
    tokens_received = float(receipt.get("tokens_received", 0))

    if tokens_received <= 0:
        return None

    pos = Position(
        id           = pos_id,
        side         = side,
        entry_price  = actual_price,
        size_usd     = stake,
        entry_time   = time.time(),
        peak_price   = actual_price,
        market_slug  = _market.slug if _market else "",
        token_id     = tok_id,
        size_tokens  = tokens_received,
    )
    async with _pos_lock:
        _open_pos.append(pos)
    _last_entry = time.time()

    global _round_entries, _round_slug
    if _market and _market.slug != _round_slug:
        _round_slug    = _market.slug
        _round_entries = 1
    else:
        _round_entries += 1
    _save_open_positions()

    secs_left  = (_market.end_ts - time.time()) if _market else 0
    slip_pct   = abs(actual_price - price) / price * 100
    # Profit esperado si resuelve (round_end, sin fee)
    exp_profit_resolve = (1.0 / actual_price - 1.0) * 100
    # Profit si vendes anticipadamente al mismo precio (con fee 3%)
    exp_profit_sell    = ((1.0 * (1 - POLYMARKET_FEE_PCT)) / actual_price - 1.0) * 100
    _linfo(
        f"\n  ✅ {mode_tag} ENTRY {side} @ {actual_price:.4f} "
        f"(señal={price:.4f} slip={slip_pct:.1f}%) | ${STAKE_USD} | "
        f"+{exp_profit_resolve:.1f}% resolución / +{exp_profit_sell:.1f}% venta | {secs_left:.0f}s | {btc_info}"
    )
    return pos


async def close_position(pos: Position, reason: str,
                          price_override: Optional[float] = None) -> None:
    # Anti-double-close: si ya está en proceso de cierre, ignorar
    if pos.id in _closing_ids or pos.status != "open":
        return
    # Anti-loop: esperar cooldown tras SELL_FALLIDO_DEFINITIVO antes de reintentar
    retry_after = _sell_retry_after.get(pos.id, 0)
    if retry_after and time.time() < retry_after:
        return
    _closing_ids.add(pos.id)
    if pos.token_id:
        _closing_token_ids.add(pos.token_id)
    try:
        await _do_close_position(pos, reason, price_override)
    finally:
        _closing_ids.discard(pos.id)
        if pos.token_id:
            _closing_token_ids.discard(pos.token_id)


async def _do_close_position(pos: Position, reason: str,
                              price_override: Optional[float] = None) -> None:
    price  = price_override if price_override is not None else (
        _prices.get(pos.side) or pos.entry_price
    )
    tokens = pos.size_tokens if pos.size_tokens > 0 else round(pos.size_usd / pos.entry_price, 2)

    # Cálculo de PnL con fee real de Polymarket
    gross_usdc = price * tokens                                          # USDC bruto recibido
    fee_usdc   = gross_usdc * POLYMARKET_FEE_PCT if reason != "round_end" else 0.0
    net_usdc   = gross_usdc - fee_usdc                                   # USDC neto tras fee
    cost_usdc  = pos.entry_price * tokens                                # coste de entrada
    pnl        = net_usdc - cost_usdc                                    # PnL neto real

    # SELL: publicar señal al execution_bot (excepto round_end — Polymarket paga automáticamente)
    if reason != "round_end" and pos.token_id:
        tokens_to_sell = pos.size_tokens if pos.size_tokens > 0 else round(pos.size_usd / pos.entry_price, 2)
        sig_id = f"SIG_SELL_{pos.id}_{int(time.time()*1000)}"
        sell_signal = {
            "action":        "SELL",
            "signal_id":     sig_id,
            "position_id":   pos.id,
            "side":          pos.side,
            "token_id":      pos.token_id,
            "price":         price,
            "entry_price":   pos.entry_price,   # C2: floor = entry_price × 0.40 en executor
            "size_usd":      pos.size_usd,
            "size_tokens":   tokens_to_sell,
            "reason":        reason,
            "market_slug":   pos.market_slug,
            "market_end_ts": _market.end_ts if _market else 0,
            "timestamp":     datetime.now(timezone.utc).isoformat(),
        }
        await _publish_signal(sell_signal)
        receipt = await _await_receipt(sig_id)
        sold = receipt is not None and receipt.get("status") == "FILLED"
        if not sold:
            # Incrementar contador de reintentos
            _sell_retry_count[pos.id] = _sell_retry_count.get(pos.id, 0) + 1
            retries = _sell_retry_count[pos.id]

            if retries >= MAX_SELL_RETRIES:
                # SELL permanentemente fallido: sacar de seguimiento para no bloquear el bot
                _lerr(
                    f"[CLOSE] SELL PERMANENTE FALLIDO para {pos.id} tras {retries} intentos "
                    f"— posición marcada como orphaned. Tokens en wallet, revisar manualmente."
                )
                asyncio.ensure_future(_tg(
                    f"🚨 SELL PERMANENTE FALLIDO {pos.id} tras {retries} intentos.\n"
                    f"Tokens en wallet de Polymarket — revisar y cerrar manualmente.\n"
                    f"Token: {pos.token_id[:20] if pos.token_id else 'N/A'}"
                ))
                # Marcar como orphaned y sacar de _open_pos para desbloquear el bot
                pos.status      = "orphaned"
                pos.exit_reason = "sell_failed_permanent"
                if pos in _open_pos:
                    _open_pos.remove(pos)
                _sell_retry_count.pop(pos.id, None)
                _sell_retry_after.pop(pos.id, None)
                _save_open_positions()
                return

            # La orden SELL no se ejecutó — NO cerrar la posición en el bot.
            # Aplicar cooldown de 15s antes de reintentar (evita loop infinito).
            _sell_retry_after[pos.id] = time.time() + 15.0
            _lerr(f"[CLOSE] SELL fallido para {pos.id} (intento {retries}/{MAX_SELL_RETRIES}) — reintentando en 15s")
            asyncio.ensure_future(_tg(f"⚠️ SELL FALLIDO {pos.id} (intento {retries}/{MAX_SELL_RETRIES}) — tokens en wallet, reintento en 15s"))
            return

    # SELL exitoso — registrar para verificación post-sell 60s
    if reason != "round_end" and pos.token_id:
        _reconcile_sell_verify[pos.token_id] = time.time()

    pos.exit_price  = price
    pos.exit_reason = reason
    pos.pnl         = pnl
    pos.fee_usd     = fee_usdc
    pos.status      = "closed"
    _sell_retry_after.pop(pos.id, None)  # limpiar cooldown si existía
    _sell_retry_count.pop(pos.id, None)  # limpiar contador de reintentos

    if pos in _open_pos:
        _open_pos.remove(pos)
    _closed.append(pos)
    _save_open_positions()

    global _session_pnl
    _stats["pnl"] += pnl
    _session_pnl   += pnl
    _update_window_pnl(pnl)
    # C4: acumular en límites globales semanales/mensuales
    loss_tracker.record_pnl(pnl)
    if pnl > 0:
        _stats["wins"] += 1
    elif pnl < 0:
        _stats["losses"] += 1
    # Stats separados por modo
    mode_key = "dry" if DRY_RUN else "real"
    _stats[f"{mode_key}_pnl"] += pnl
    result_label = "win" if pnl > 0 else ("loss" if pnl < 0 else "break_even")
    if pnl > 0:
        _stats[f"{mode_key}_wins"] += 1
    elif pnl < 0:
        _stats[f"{mode_key}_losses"] += 1

    # Actualizar métricas Prometheus
    _prom_record_trade(pos.side, result_label)
    _prom_update_state(
        open_pos=len([p for p in _open_pos if p.status == "open"]),
        pnl=_window_pnl,
        btc_mom=0.0,  # se actualiza en el heartbeat con el valor real
    )

    icon = "💰" if pnl > 0 else "❌"
    hold = time.time() - pos.entry_time
    fee_str = f" fee=${fee_usdc:.3f}" if fee_usdc > 0 else ""
    msg  = f"{icon} EXIT {pos.side} @ {price:.4f}{fee_str} | P&L ${pnl:+.4f} | hold={hold:.0f}s | {reason}"
    _linfo(msg)
    # TG solo para fallos técnicos — salidas normales no se notifican
    await _append_csv(pos)

    # Aplicar parámetros hot-reload diferidos (acumulados mientras había posición abierta)
    if _pending_hot_params and not any(p.status == "open" for p in _open_pos):
        applied = _apply_hot_params(_pending_hot_params)
        _pending_hot_params.clear()
        if applied:
            _linfo(f"[CONFIG] Hot-reload diferido aplicado tras cierre:\n" + "\n".join(f"  {u}" for u in applied))


async def check_exits() -> None:
    """Verifica SL/TP para todas las posiciones abiertas.

    Evalúa cada posición y lanza los cierres necesarios en paralelo
    con asyncio.gather para no serializar las órdenes CLOB.
    """
    if not _market:
        return
    now       = time.time()
    secs_left = max(0.0, _market.end_ts - now)

    closes: List[Tuple] = []  # (pos, reason)

    async with _pos_lock:
        snapshot = list(_open_pos)
    for pos in snapshot:
        if pos.status != "open":
            continue
        price = _prices.get(pos.side)
        if price is None:
            continue

        if price > pos.peak_price:
            pos.peak_price = price

        hold_s  = now - pos.entry_time
        tp_full = pos.entry_price * (1 + _P["TP_FULL_PCT"])
        tp_mid  = pos.entry_price * (1 + _P["TP_MID_PCT"])

        # Si BTC se mueve muy fuerte en la dirección de la posición → hold resolución
        # Evita cerrar con TP parcial cuando BTC está en tendencia clara (ej. +0.15%)
        btc_pct, btc_dir = bp.get_momentum(window_s=_P["BTC_WINDOW_S"])
        if abs(btc_pct) >= _P["HOLD_BTC_PCT"] and btc_dir == pos.side:
            gain_pct = (price - pos.entry_price) / pos.entry_price * 100
            print(f"  🔒 HOLD {pos.side}@{price:.4f} +{gain_pct:.1f}% "
                  f"| BTC {btc_dir}{btc_pct:+.3f}% muy fuerte → espera resolucion ({secs_left:.0f}s)")
            continue  # no cerrar, dejar que resuelva

        gain_pct = (price - pos.entry_price) / pos.entry_price * 100

        if price >= tp_full:
            closes.append((pos, f"tp_full +{gain_pct:.1f}%"))
        elif price >= tp_mid and secs_left > _P["HOLD_SECS"]:
            closes.append((pos, f"tp_mid +{gain_pct:.1f}%"))
        elif secs_left <= _P["HOLD_SECS"] and gain_pct >= _P["TP_LAST_15S_PCT"] * 100:
            closes.append((pos, f"tp_15s +{gain_pct:.1f}%"))
        elif secs_left <= _P["HOLD_SECS"] and price >= 0.80:
            pass  # hold a resolución
        elif secs_left <= 30 and price < pos.entry_price * (1 - _P["SL_LAST_30S_PCT"]):
            closes.append((pos, f"sl_30s {gain_pct:.1f}%"))
        elif price <= _P["SL_FLOOR"]:
            closes.append((pos, "sl_floor"))
        elif price <= pos.entry_price - _P["SL_DROP"]:
            closes.append((pos, "sl_drop"))
        elif hold_s >= _P["SL_TIME_S"] and price < pos.entry_price:
            closes.append((pos, "sl_time"))

    if closes:
        await asyncio.gather(*[close_position(p, r) for p, r in closes])


async def _exit_watcher() -> None:
    """Tarea paralela: llama check_exits inmediatamente cuando llega precio nuevo por WS.

    Evita esperar el ciclo de scan (0.5s) para ejecutar SL/TP.
    """
    while True:
        if _price_updated is not None:
            await _price_updated.wait()
            _price_updated.clear()
            if _open_pos:
                await check_exits()
        else:
            await asyncio.sleep(0.1)


async def _wallet_reconciliation_loop() -> None:
    """
    Lee la wallet real de Polymarket (Gamma API) cada 10s durante toda la sesión.

    Fase 1 — Orphaned: token en Gamma no rastreado en _open_pos →
        reconstruye Position sintética, activa TP/SL inmediatamente.

    Fase 2 — Post-sell 60s: tras un SELL exitoso, verifica que los tokens
        desaparecen de Gamma. Si siguen presentes >60s los re-cola como orphaned.

    C5 — Guardas de seguridad:
        · No crea sintéticas si el mercado expira en <30s (inútil)
        · Máximo 2 posiciones sintéticas simultáneas (anti-flood)
        · avgCost validado con rango 0.01-0.99 antes de usar
        · No actúa si hay otra posición en _closing_ids para el mismo token
        · Nunca crea sintética para token ya presente en _open_pos (dedup)
    """
    if DRY_RUN:
        return  # sin wallet real en DRY_RUN

    if not _WALLET_ADDR:
        _lwarn("[RECONCILE] POLYMARKET_ADDRESS no configurado — desactivado")
        return

    _linfo("[RECONCILE] Wallet reconciliation activo — Gamma API cada 10s")

    MAX_SYNTHETIC_POS = 2  # límite de posiciones sintéticas simultáneas

    while True:
        await asyncio.sleep(10)

        if _http_session is None:
            continue

        # ── Fetch posiciones reales de Gamma API ──────────────────────────────
        try:
            async with _http_session.get(
                f"{GAMMA_API}/positions",
                params={"user": _WALLET_ADDR, "sizeThreshold": "0.1"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    continue
                raw = await resp.json(content_type=None)
        except Exception as exc:
            _lwarn(f"[RECONCILE] Error Gamma API: {exc}")
            continue

        # C5: validar que la respuesta es una lista no vacía de dicts
        if not isinstance(raw, list):
            _lwarn(f"[RECONCILE] Respuesta inesperada de Gamma API: {type(raw)}")
            continue

        now = time.time()

        # Indexar por token_id (campo "asset" en Gamma)
        gamma_by_token: Dict[str, dict] = {}
        for gpos in raw:
            if not isinstance(gpos, dict):
                continue
            tid = (gpos.get("asset") or gpos.get("assetId") or "").strip()
            if not tid or len(tid) < 10:  # C5: token_id debe tener longitud razonable
                continue
            try:
                sz = float(gpos.get("size") or 0)
                if sz >= 0.1:
                    gamma_by_token[tid] = gpos
            except (ValueError, TypeError):
                pass

        async with _pos_lock:
            _open_pos_snap = list(_open_pos)

        tracked_tokens = {
            p.token_id for p in _open_pos_snap
            if p.status == "open" and p.token_id
        }

        # Tokens de posiciones sintéticas ya creadas (evita duplicados)
        synthetic_tokens = {
            p.token_id for p in _open_pos_snap
            if p.status == "open" and p.id.startswith("RCNCL_")
        }
        n_synthetic = len(synthetic_tokens)

        # ── Fase 1: Orphaned — en Gamma, no en _open_pos ─────────────────────
        for tid, gpos in gamma_by_token.items():
            if tid in tracked_tokens:
                continue
            # C5: ya hay una sintética para este token
            if tid in synthetic_tokens:
                continue
            # C5: límite de sintéticas simultáneas
            if n_synthetic >= MAX_SYNTHETIC_POS:
                _lwarn(f"[RECONCILE] Límite de {MAX_SYNTHETIC_POS} sintéticas alcanzado — omitiendo {tid[:20]}")
                break
            # C5: token siendo cerrado activamente (fix 1: O(1) lookup via _closing_token_ids)
            if tid in _closing_token_ids:
                continue
            # Ventana post-sell: Gamma puede tardar en actualizar (60s gracia)
            if tid in _reconcile_sell_verify and now - _reconcile_sell_verify[tid] < 60:
                continue

            size_tokens = 0.0
            try:
                size_tokens = float(gpos.get("size") or 0)
            except (ValueError, TypeError):
                pass
            if size_tokens < MIN_SELL_TOKENS:
                continue

            # Solo actuar en el mercado actual
            side: Optional[str] = None
            if _market:
                if tid == _market.up_token_id:
                    side = "UP"
                elif tid == _market.down_token_id:
                    side = "DOWN"
            if side is None:
                continue  # token de mercado anterior o desconocido

            # C5: no crear sintética si el mercado expira en menos de 30s
            if _market and (_market.end_ts - now) < 30:
                _linfo(f"[RECONCILE] Mercado expira en <30s — omitiendo sintética para {tid[:20]}")
                continue

            current_price = _prices.get(side)
            if current_price is None:
                continue

            # C5: avgCost con validación estricta — múltiples campos posibles
            entry_price = current_price  # fallback seguro
            for avg_field in ("avgCost", "avgPrice", "averagePrice"):
                raw_avg = gpos.get(avg_field)
                if raw_avg is None:
                    continue
                try:
                    candidate = float(str(raw_avg).strip())
                    if 0.01 <= candidate <= 0.99:  # debe ser precio de token binario válido
                        entry_price = candidate
                        break
                except (ValueError, TypeError):
                    pass

            _lwarn(
                f"[RECONCILE] Orphaned: {side} token={tid[:20]} "
                f"size={size_tokens:.2f} entry≈{entry_price:.4f} current={current_price:.4f}"
            )
            asyncio.ensure_future(_tg(
                f"⚠️ RECONCILE: posición orphaned\n"
                f"Side: {side} | Token: {tid[:30]}\n"
                f"Size: {size_tokens:.2f} | Precio: {current_price:.4f}\n"
                f"Añadida al tracking con TP/SL activo"
            ))

            synthetic = Position(
                id          = f"RCNCL_{tid[:12]}_{int(now)}",
                side        = side,
                entry_price = entry_price,
                size_usd    = entry_price * size_tokens,
                # C5: entry_time basado en market_start, no en 'now - 90s' arbitrario
                entry_time  = max(now - (_market.end_ts - now), now - 240) if _market else now - 90,
                peak_price  = max(entry_price, current_price),
                token_id    = tid,
                market_slug = _market.slug if _market else "",
                size_tokens = size_tokens,
                status      = "open",
            )
            async with _pos_lock:
                _open_pos.append(synthetic)
            synthetic_tokens.add(tid)
            n_synthetic += 1
            _save_open_positions()
            _linfo(f"[RECONCILE] {synthetic.id} añadida → check_exits() evaluará TP/SL")

        # ── Fase 2: Post-sell verificación (60s) ─────────────────────────────
        for tid, close_ts in list(_reconcile_sell_verify.items()):
            age = now - close_ts

            if tid not in gamma_by_token:
                # Tokens ya fuera de Gamma — SELL confirmado
                _linfo(f"[RECONCILE] Post-sell OK: {tid[:20]} out of Gamma en {age:.0f}s")
                _reconcile_sell_verify.pop(tid, None)
                continue

            if age > 60:
                # Sigue en Gamma >60s después del SELL
                size_tokens = 0.0
                try:
                    size_tokens = float(gamma_by_token[tid].get("size") or 0)
                except (ValueError, TypeError):
                    pass
                if size_tokens >= MIN_SELL_TOKENS:
                    _lwarn(
                        f"[RECONCILE] Post-sell FALLO: {tid[:20]} sigue en Gamma "
                        f"({size_tokens:.2f} tokens, {age:.0f}s) — re-cola como orphaned"
                    )
                    asyncio.ensure_future(_tg(
                        f"⚠️ RECONCILE: SELL no confirmado\n"
                        f"Token: {tid[:30]}\n"
                        f"Size: {size_tokens:.2f} tras {age:.0f}s\n"
                        f"Se re-intentará vender en el próximo ciclo"
                    ))
                    # Al salir de _reconcile_sell_verify la Fase 1 lo detecta como orphaned
                _reconcile_sell_verify.pop(tid, None)


# ─────────────────────────────────────────────────────────────────────────────
#  FIN DE RONDA
# ─────────────────────────────────────────────────────────────────────────────

async def on_round_end(old_market: Market) -> None:
    """Cierra posiciones abiertas al expirar la ronda."""
    async with _pos_lock:
        remaining = [p for p in _open_pos if p.status == "open"]
    if not remaining:
        return

    print(f"\n  [RND] Ronda expiró — cerrando {len(remaining)} posición(es)...")
    # Espera resolución de Polymarket (sondeo cada 1s, máx 20s)
    up_res = dn_res = None
    for _ in range(30):
        await asyncio.sleep(1)
        up_res, dn_res = _fetch_resolution(old_market)
        # Espera hasta que los precios de resolución reflejen la liquidación real
        # (cerca de 0.00 o 1.00), no precios intermedios de mercado (~0.50)
        if up_res is not None and (up_res >= 0.95 or up_res <= 0.05):
            break
        up_res = dn_res = None  # precio intermedio, seguir esperando

    # Cierre en paralelo para no serializar órdenes
    await asyncio.gather(*[
        close_position(pos, "round_end",
                       price_override=(up_res if pos.side == "UP" else dn_res))
        for pos in list(remaining) if pos.status == "open"
    ])

    # Limpia la lista (posiciones ya movidas a _closed)
    async with _pos_lock:
        _open_pos.clear()


# ─────────────────────────────────────────────────────────────────────────────
#  CSV
# ─────────────────────────────────────────────────────────────────────────────

_CSV_FIELDS = [
    "id", "side", "market_slug", "entry_price", "exit_price",
    "size_usd", "pnl", "fee_usd", "exit_reason", "entry_time", "exit_time", "mode",
]


def _rotate_csv_if_needed(path: Path) -> None:
    """Si el CSV supera MAX_TRADES_CSV filas, lo rota conservando solo las últimas MAX_TRADES_CSV."""
    try:
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        if len(rows) > MAX_TRADES_CSV:
            keep = rows[-MAX_TRADES_CSV:]
            import tempfile, os as _os
            dir_ = path.parent
            with tempfile.NamedTemporaryFile("w", dir=dir_, delete=False,
                                             suffix=".tmp", newline="", encoding="utf-8") as tf:
                w = csv.DictWriter(tf, fieldnames=_CSV_FIELDS)
                w.writeheader()
                w.writerows(keep)
                tmp = tf.name
            _os.replace(tmp, path)
    except Exception:
        pass  # Si falla la rotación no es crítico; el archivo simplemente crece


def _append_csv_sync(pos: Position) -> None:
    """Operación síncrona de escritura CSV — ejecutada en thread pool desde _append_csv."""
    path   = Path(TRADES_CSV)
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow({
            "id":           pos.id,
            "side":         pos.side,
            "market_slug":  pos.market_slug,
            "entry_price":  f"{pos.entry_price:.4f}",
            "exit_price":   f"{pos.exit_price:.4f}" if pos.exit_price else "",
            "size_usd":     f"{pos.size_usd:.2f}",
            "pnl":          f"{pos.pnl:+.4f}",
            "fee_usd":      f"{pos.fee_usd:.4f}",
            "exit_reason":  pos.exit_reason or "",
            "entry_time":   f"{pos.entry_time:.3f}",
            "exit_time":    f"{time.time():.3f}",
            "mode":         "DRY" if DRY_RUN else "REAL",
        })


async def _append_csv(pos: Position) -> None:
    """Escribe la fila de trade en CSV de forma no bloqueante (thread pool) y rota si es necesario."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _append_csv_sync, pos)
    # Rotación periódica: solo si el archivo es grande (evitar I/O en cada trade)
    path = Path(TRADES_CSV)
    if path.exists() and path.stat().st_size > 500_000:  # ~500 KB → verificar filas
        await loop.run_in_executor(None, _rotate_csv_if_needed, path)


def _save_open_positions() -> None:
    """Persiste posiciones abiertas a disco para recovery tras reinicio."""
    if DRY_RUN:
        return
    data = []
    for pos in _open_pos:
        if pos.status == "open":
            data.append({
                "id":           pos.id,
                "side":         pos.side,
                "entry_price":  pos.entry_price,
                "size_usd":     pos.size_usd,
                "size_tokens":  pos.size_tokens,
                "entry_time":   pos.entry_time,
                "peak_price":   pos.peak_price,
                "token_id":     pos.token_id,
                "market_slug":  pos.market_slug,
                "market_end_ts": _market.end_ts if _market else 0,
            })
    try:
        _write_json_atomic(OPEN_POS_FILE, data)
    except Exception as exc:
        _lwarn(f"[RECOVERY] No se pudo guardar posiciones abiertas: {exc}")


def _recover_open_positions() -> None:
    """Al arrancar en modo REAL, recupera posiciones abiertas de sesión anterior.

    Solo recupera posiciones cuyo mercado sigue activo (end_ts > now).
    Si el mercado ya cerró, avisa pero no puede hacer nada — la posición
    resolvió sola en Polymarket.
    """
    if DRY_RUN:
        return
    path = Path(OPEN_POS_FILE)
    if not path.exists():
        return
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        now  = time.time()
        recovered = 0
        for d in data:
            market_end_ts = d.get("market_end_ts", 0)
            entry_time    = d.get("entry_time", 0)
            secs_left     = market_end_ts - now
            if market_end_ts > now:
                pos = Position(
                    id          = d["id"],
                    side        = d["side"],
                    entry_price = d["entry_price"],
                    size_usd    = d["size_usd"],
                    size_tokens = d["size_tokens"],
                    entry_time  = d["entry_time"],
                    peak_price  = d.get("peak_price", d["entry_price"]),
                    token_id    = d.get("token_id", ""),
                    market_slug = d.get("market_slug", ""),
                    status      = "open",
                )
                _open_pos.append(pos)
                hold_s = now - entry_time
                _lwarn(f"  [RECOVERY] ⚡ Posición recuperada: {pos.side} @ {pos.entry_price:.4f} "
                       f"| hold={hold_s:.0f}s | {secs_left:.0f}s restantes | token={pos.token_id[:16]}")
                recovered += 1
            else:
                held = now - entry_time
                _lwarn(f"  [RECOVERY] ⚠️  Posición PERDIDA (mercado cerró hace {-secs_left:.0f}s): "
                       f"{d.get('side')} @ {d.get('entry_price')} | hold={held:.0f}s "
                       f"| {d.get('market_slug', '')}")
        if recovered:
            _lwarn(f"  [RECOVERY] {recovered} posición(es) restaurada(s) — SL/TP activo desde ahora")
        path.unlink()   # borrar tras recovery para no re-cargar en próximo arranque
    except Exception as exc:
        _lwarn(f"  [RECOVERY] Error leyendo {OPEN_POS_FILE}: {exc}")


def _load_history_from_csv() -> None:
    """Carga historial de trades del CSV al iniciar (persistencia entre sesiones).

    Reconstruye objetos Position en _closed para que el dashboard muestre
    todos los trades previos desde el primer arranque.
    """
    path = Path(TRADES_CSV)
    if not path.exists():
        return
    try:
        count     = 0
        total_pnl = 0.0
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pnl_bruto = float(row.get("pnl", 0))
                # fee_usd = 0 en trades anteriores al fix — sus PnL están inflados pero
                # se aceptan tal cual para no distorsionar el historial
                pnl      = pnl_bruto
                row_mode = row.get("mode", "DRY")   # trades viejos sin columna → DRY por defecto
                total_pnl += pnl
                if pnl > 0:
                    _stats["wins"] += 1
                elif pnl < 0:
                    _stats["losses"] += 1
                # Stats separados por modo
                mode_key = "dry" if row_mode == "DRY" else "real"
                _stats[f"{mode_key}_pnl"] += pnl
                if pnl > 0:
                    _stats[f"{mode_key}_wins"] += 1
                elif pnl < 0:
                    _stats[f"{mode_key}_losses"] += 1

                # Reconstruye Position para que el dashboard lo muestre
                try:
                    pos = Position(
                        id          = row["id"],
                        side        = row["side"],
                        entry_price = float(row["entry_price"]),
                        size_usd    = float(row["size_usd"]),
                        entry_time  = float(row["entry_time"]),
                        peak_price  = float(row["entry_price"]),
                        status      = "closed",
                        pnl         = pnl,
                        exit_price  = float(row["exit_price"]) if row.get("exit_price") else None,
                        exit_reason = row.get("exit_reason", ""),
                        market_slug = row.get("market_slug", ""),
                    )
                    _closed.append(pos)
                except Exception:
                    pass
                count += 1

        _stats["pnl"] = total_pnl
        if count:
            dw = _stats["dry_wins"];   dl = _stats["dry_losses"]
            rw = _stats["real_wins"];  rl = _stats["real_losses"]
            d_wr = dw/(dw+dl)*100 if (dw+dl) > 0 else 0.0
            r_wr = rw/(rw+rl)*100 if (rw+rl) > 0 else 0.0
            print(f"  [CSV] Historial: {count} trades | "
                  f"DRY: W={dw} L={dl} WR={d_wr:.0f}% P&L=${_stats['dry_pnl']:+.2f} | "
                  f"REAL: W={rw} L={rl} WR={r_wr:.0f}% P&L=${_stats['real_pnl']:+.2f}")
    except Exception as exc:
        print(f"  [CSV] No se pudo cargar historial: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  LOOP PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def _print_banner() -> None:
    mode = "DRY RUN (paper)" if DRY_RUN else "⚠️  REAL MONEY"
    print("\n" + "=" * 62)
    print(f"  CalvinBot 5min — BTC Momentum Sniper   [{mode}]")
    print("=" * 62)
    print(f"  Mercado:     {SLUG_PATTERN}-XXXXXXXXX")
    print(f"  Ventana:     últimos {SNIPER_WINDOW_S}s de cada ronda de 5min")
    print(f"  Entrada:     precio {ENTRY_MIN}-{ENTRY_MAX}")
    print(f"  BTC filter:  >={BTC_MIN_PCT}% en {BTC_WINDOW_S}s  (required={BTC_REQUIRED})")
    print(f"  TP:          +{TP_MID_PCT*100:.0f}% (mid) / +{TP_FULL_PCT*100:.0f}% (full) | hold<=0.80 si <={HOLD_SECS}s")
    print(f"  SL:          -{SL_DROP*100:.0f}c drop / floor {SL_FLOOR} / time {SL_TIME_S}s")
    min_stake_needed = MIN_TOKENS_BUY * ENTRY_MAX
    stake_ok = STAKE_USD >= min_stake_needed or MICRO_TEST
    print(f"  Stake:       ${STAKE_USD} | max pos: {int(_P['MAX_OPEN_POS'])}")
    if not stake_ok:
        print(f"  ⚠️  STAKE INSUFICIENTE: mínimo ${min_stake_needed:.2f} para {MIN_TOKENS_BUY:.0f} tokens @ {ENTRY_MAX:.2f}")
        print(f"     Las compras serán bloqueadas hasta que STAKE_USD >= ${min_stake_needed:.2f}")
    if MICRO_TEST:
        print(f"  ⚠️  MICRO_TEST: posiciones NO usarán TP/SL (tokens < {MIN_TOKENS_BUY:.0f}). Solo resolución final.")
    print("=" * 62 + "\n")


async def sniper_loop() -> None:
    global _market, _scan_num

    _print_banner()

    while True:
        now       = time.time()
        secs_left = (_market.end_ts - now) if _market else -1

        # ── Detecta/renueva mercado ───────────────────────────────────────────
        if _market is None or secs_left < 3:
            old_market = _market
            if old_market and secs_left < 3:
                await on_round_end(old_market)
                _stats["rounds"] += 1
                async with _pos_lock:
                    _open_pos.clear()

            print(f"\n  Buscando próximo mercado 5min...")
            found = False
            for _ in range(15):
                m = detect_market()
                if m:
                    _market = m
                    _prices["UP"] = _prices["DOWN"] = None  # resetea precios
                    _price_history["UP"].clear()
                    _price_history["DOWN"].clear()
                    _round_start_prices["UP"] = _round_start_prices["DOWN"] = None   # I9
                    _round_start_ts["UP"]     = _round_start_ts["DOWN"]     = 0.0   # fix 6
                    found = True
                    break
                await asyncio.sleep(5)

            if not found:
                print("  [ERR] Sin mercado activo — reintentando en 15s")
                await asyncio.sleep(15)
                continue

            secs_left = _market.end_ts - time.time()

        # ── Espera hasta ventana sniper ───────────────────────────────────────
        if secs_left > SNIPER_WINDOW_S:
            wait = secs_left - SNIPER_WINDOW_S
            btc_p = bp.get_btc_price()
            btc_pct, btc_dir = bp.get_momentum(BTC_WINDOW_S)
            btc_str = (f"${btc_p:,.0f} {btc_dir or '─'} {btc_pct:+.3f}%"
                       if btc_p else "BTC: esperando feed...")

            print(f"\r  [{_market.slug[-14:]}] ⏳ {secs_left:.0f}s | {btc_str} | "
                  f"entrando ventana en {wait:.0f}s...", end="", flush=True)
            await asyncio.sleep(2)
            continue

        # ── DENTRO DE VENTANA SNIPER ──────────────────────────────────────────
        _scan_num += 1
        up_p  = _prices.get("UP")
        dn_p  = _prices.get("DOWN")
        btc_p = bp.get_btc_price()
        btc_pct, btc_dir = bp.get_momentum(BTC_WINDOW_S)

        # ── Drawdown: si se alcanzó el límite y ya no hay posiciones abiertas → salir ──
        if _drawdown_hit and not any(p.status == "open" for p in _open_pos):
            print(f"\n  🛑 DRAWDOWN LÍMITE alcanzado (${_window_pnl:+.2f}). "
                  f"Bot pausado. Reinicia mañana a las {TRADING_HOUR_START}h España.")
            break

        # Mostrar stats del modo actual para no mezclar DRY con REAL
        mode_key = "dry" if DRY_RUN else "real"
        mode_lbl = "DRY" if DRY_RUN else "REAL"
        w    = _stats[f"{mode_key}_wins"]
        l    = _stats[f"{mode_key}_losses"]
        wr   = w / (w + l) * 100 if (w + l) > 0 else 0.0
        pnl  = _stats[f"{mode_key}_pnl"]
        skip = _stats["skipped_momentum"]

        # Indicador de horario en la línea de estado
        now_es   = _madrid_now()
        in_hours = _in_trading_hours()
        horario_tag = f"{now_es.hour:02d}:{now_es.minute:02d}h" if in_hours else f"FUERA {now_es.hour:02d}:{now_es.minute:02d}h"

        arrow   = {"UP": "▲", "DOWN": "▼"}.get(btc_dir, "─")
        btc_line = (f"BTC ${btc_p:,.0f} {arrow}{btc_pct:+.3f}%/{BTC_WINDOW_S}s"
                    if btc_p else "BTC: —")
        price_line = (f"UP={up_p:.4f}  DN={dn_p:.4f}"
                      if up_p and dn_p else "precios: cargando...")
        ws_tag = "WS✓" if _ws_connected else "WS✗"

        # Indicadores de conectividad de los otros bots (verde ✓ / rojo ✗ / ? sin datos)
        now_ts = time.time()
        def _bot_tag(key: str, label: str) -> str:
            ts = _bot_last_hb.get(key)
            if ts is None:   return f"{label}?"
            return f"{label}✓" if now_ts - ts < 30 else f"{label}✗"
        bots_line = f"BOTS: {_bot_tag('executor','EXEC')} {_bot_tag('optimizer','OPT')} {_bot_tag('watchdog','WD')}"

        print(f"\n{'─'*62}")
        print(f"  🎯 #{_scan_num:>3} | {secs_left:.0f}s | {price_line} | {ws_tag} | {horario_tag}")
        print(f"       {btc_line} | pos:{len(_open_pos)}/{int(_P['MAX_OPEN_POS'])} | "
              f"[{mode_lbl}] W={w} L={l} WR={wr:.0f}% P&L=${pnl:+.2f} | "
              f"window=${_window_pnl:+.2f} | skip={skip}")
        print(f"       {bots_line}")

        # Comprueba salidas primero (siempre, independiente del horario)
        await check_exits()

        # Evalúa señal (debug=True muestra razón exacta del bloqueo)
        signal, btc_info = evaluate_signal(debug=True)
        if signal:
            await open_position(signal, btc_info)

        # LOG DIAGNÓSTICO cada 10 scans — muestra exactamente qué bloquea
        if _scan_num % 10 == 0:
            _linfo(
                f"[DIAG] btc_connected={bp.is_connected()} "
                f"btc_history={bp.has_enough_history(_P['BTC_WINDOW_S'] * 0.5)} "
                f"prices_UP={_prices.get('UP')} prices_DOWN={_prices.get('DOWN')} "
                f"secs_left={(_market.end_ts - time.time()):.0f}s "
                f"in_trading_hours={_in_trading_hours()} "
                f"entries_paused={_entries_paused} drawdown_hit={_drawdown_hit}"
            )

        await asyncio.sleep(SCAN_INTERVAL_S)


# ─────────────────────────────────────────────────────────────────────────────
#  I10: PNL ALERT LOOP — resumen horario + alerta inmediata si WR < 45%
# ─────────────────────────────────────────────────────────────────────────────

async def _pnl_alert_loop() -> None:
    """
    Envía a Telegram un resumen de PnL cada hora y alerta inmediatamente
    si la win rate de la sesión cae por debajo del 45%.
    Solo cuenta trades REAL (no DRY) para las métricas críticas.
    """
    PNL_ALERT_INTERVAL_S = 3600          # resumen cada 1h
    WR_ALERT_THRESHOLD   = 0.45          # alerta si WR cae por debajo del 45%
    WR_MIN_TRADES        = 10            # mínimo trades para evaluar WR
    WR_COOLDOWN_S        = 1800          # no repetir alerta WR más de 1x cada 30min

    last_hourly_ts  = time.time()
    last_wr_warn_ts = 0.0

    while True:
        await asyncio.sleep(60)
        now = time.time()

        real_wins   = _stats.get("real_wins",   0)
        real_losses = _stats.get("real_losses", 0)
        real_pnl    = _stats.get("real_pnl",    0.0)
        real_trades = real_wins + real_losses

        # ── Alerta inmediata si WR < 45% ────────────────────────────────────
        if real_trades >= WR_MIN_TRADES:
            wr = real_wins / real_trades
            if wr < WR_ALERT_THRESHOLD and (now - last_wr_warn_ts) > WR_COOLDOWN_S:
                last_wr_warn_ts = now
                lt_summary = loss_tracker.get_summary()
                asyncio.ensure_future(_tg(
                    f"⚠️ WIN RATE BAJO: {wr:.0%} ({real_wins}W/{real_losses}L) — "
                    f"sesión PnL ${real_pnl:+.2f} | "
                    f"semana ${lt_summary['weekly_pnl']:+.2f} "
                    f"({lt_summary['weekly_pct_used']:.0f}% límite)"
                ))

        # ── Resumen horario ──────────────────────────────────────────────────
        if (now - last_hourly_ts) >= PNL_ALERT_INTERVAL_S:
            last_hourly_ts = now
            lt_summary = loss_tracker.get_summary()
            wr_str = (
                f"{real_wins / real_trades:.0%} ({real_wins}W/{real_losses}L)"
                if real_trades > 0 else "—"
            )
            asyncio.ensure_future(_tg(
                f"📊 Resumen horario Calvin5\n"
                f"Sesión PnL: ${real_pnl:+.2f} | WR: {wr_str}\n"
                f"Semana:  ${lt_summary['weekly_pnl']:+.2f} "
                f"({lt_summary['weekly_pct_used']:.0f}% de ${lt_summary['weekly_limit']:.0f})\n"
                f"Mes:     ${lt_summary['monthly_pnl']:+.2f} "
                f"({lt_summary['monthly_pct_used']:.0f}% de ${lt_summary['monthly_limit']:.0f})"
            ))


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    """
    Calculador Calvin5 — lanza todas las tareas en paralelo:
      · BTC price feed (Binance)
      · WebSocket de precios Polymarket
      · REST price refresher (backup del WS)
      · Exit watcher (SL/TP reactivo)
      · Sniper loop (generación de señales)
      · Receipt listener (confirmaciones del execution_bot)
      · Config listener (hot-reload del dynamic_optimizer)
      · Heartbeat publisher (latido al watchdog)
    """
    global _price_updated, _redis, _http_session

    _validate_env_vars_calvin5()  # ISSUE #4: Verificación temprana
    _price_updated = asyncio.Event()

    # Arrancar servidor de métricas Prometheus (puerto 9090 por defecto, no-op si no instalado)
    await metrics.start_server()

    # Crear HTTP session singleton (pool de conexiones — evita TCP handshake en cada llamada REST)
    _http_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300),
        timeout=aiohttp.ClientTimeout(total=10),
    )

    # Conectar Redis
    try:
        _redis = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
        await _redis.ping()
        _linfo(f"[REDIS] Conectado a {REDIS_URL}")
    except Exception as exc:
        _lwarn(f"[REDIS] No se pudo conectar ({exc}) — señales desactivadas, solo DRY RUN local")
        _redis = None

    # Cargar estado previo
    _load_window_pnl()
    _recover_open_positions()
    _load_history_from_csv()

    # Inicializar parámetros vivos desde constantes de arranque
    _init_live_params()

    # Servidor Prometheus (no-op si prometheus_client no instalado)
    await metrics.start_server()

    try:
        await asyncio.gather(
            bp.run_btc_poller(),
            run_prices_ws(),
            _rest_price_refresher(),
            _exit_watcher(),
            sniper_loop(),
            _receipt_listener(),
            _config_listener(),
            _control_listener(),
            _calc_heartbeat(),
            _bot_monitor_task(),
            _wallet_reconciliation_loop(),
            _pnl_alert_loop(),
        )
    finally:
        if _http_session and not _http_session.closed:
            await _http_session.close()


if __name__ == "__main__":
    from utils import configure_structlog
    configure_structlog("calvin5", log_file="calvin5.log")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        dw  = _stats["dry_wins"];  dl  = _stats["dry_losses"]
        rw  = _stats["real_wins"]; rl  = _stats["real_losses"]
        d_wr = dw/(dw+dl)*100 if (dw+dl) > 0 else 0.0
        r_wr = rw/(rw+rl)*100 if (rw+rl) > 0 else 0.0
        print(f"\n\n{'='*50}")
        print(f"  Sesión terminada — {_stats['rounds']} rondas")
        print(f"  DRY  — W={dw} L={dl} WR={d_wr:.0f}% P&L=${_stats['dry_pnl']:+.4f}")
        print(f"  REAL — W={rw} L={rl} WR={r_wr:.0f}% P&L=${_stats['real_pnl']:+.4f}")
        print(f"  Skipped (sin momentum BTC): {_stats['skipped_momentum']}")
        print(f"{'='*50}")
