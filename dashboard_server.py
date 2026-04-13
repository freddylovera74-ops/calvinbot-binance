"""
dashboard_server.py — Dashboard tiempo real CalvinBot Binance
Servidor: aiohttp en http://0.0.0.0:PORT/
Rutas:
  GET  /        -> HTML SPA (dark modern UI)
  GET  /api/state -> JSON estado completo
  GET  /ws      -> WebSocket push cada 1s
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

try:
    import aiohttp
    from aiohttp import web
except ImportError:
    print("ERROR: pip install aiohttp"); sys.exit(1)

try:
    import redis.asyncio as aioredis
except ImportError:
    print("ERROR: pip install redis"); sys.exit(1)

try:
    import loss_tracker as _loss_tracker
    _HAS_LOSS_TRACKER = True
except ImportError:
    _HAS_LOSS_TRACKER = False

# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT        = int(os.getenv("DASHBOARD_PORT", "8084"))
HISTORY_LEN = 120   # ~2 min a 1s/ciclo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DASH] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("dashboard")

# ─────────────────────────────────────────────────────────────────────────────
_btc_history: deque = deque(maxlen=HISTORY_LEN)
_pnl_history: deque = deque(maxlen=HISTORY_LEN)
_log_buf:     deque = deque(maxlen=150)
_ws_clients:  Set[web.WebSocketResponse] = set()
_redis_conn:  Optional[aioredis.Redis] = None

_state: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
#  REDIS
# ─────────────────────────────────────────────────────────────────────────────

async def _get_redis() -> Optional[aioredis.Redis]:
    global _redis_conn
    if _redis_conn is None:
        try:
            _redis_conn = aioredis.Redis.from_url(
                REDIS_URL, decode_responses=True, socket_timeout=3
            )
        except Exception:
            pass
    return _redis_conn


async def _reset_redis():
    global _redis_conn
    if _redis_conn:
        try:
            await _redis_conn.aclose()
        except Exception:
            pass
    _redis_conn = None


# ─────────────────────────────────────────────────────────────────────────────
#  LECTURA DE ARCHIVOS
# ─────────────────────────────────────────────────────────────────────────────

def _read_trades_csv(n: int = 50) -> List[dict]:
    """
    Lee binance_trades.csv — columnas:
    timestamp, event, position_id, symbol, side,
    entry_price, exit_price, qty_btc, size_usd, pnl_usd, reason
    Solo se incluyen filas con event=CLOSE.
    """
    path = BASE_DIR / "binance_trades.csv"
    if not path.exists():
        return []
    try:
        trades = []
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    if row.get("event", "CLOSE") != "CLOSE":
                        continue
                    # timestamp es ISO string del cierre
                    ts_raw = row.get("timestamp", "") or ""
                    try:
                        exit_ts = datetime.fromisoformat(ts_raw).timestamp()
                    except Exception:
                        exit_ts = 0.0
                    pnl = float(row.get("pnl_usd", 0) or 0)
                    trades.append({
                        "id":          row.get("position_id", ""),
                        "symbol":      row.get("symbol", "BTCUSDT"),
                        "entry_price": float(row.get("entry_price", 0) or 0),
                        "exit_price":  float(row.get("exit_price",  0) or 0),
                        "qty":         float(row.get("qty_btc",     0) or 0),
                        "size_usd":    float(row.get("size_usd",    0) or 0),
                        "pnl":         pnl,
                        "exit_reason": row.get("reason", ""),
                        "entry_time":  0.0,   # no disponible en CSV
                        "exit_time":   exit_ts,
                        "mode":        row.get("mode", "DRY"),
                    })
                except (ValueError, KeyError):
                    pass
        trades.sort(key=lambda t: t["exit_time"])
        return trades[-n:]
    except Exception:
        return []


def _compute_stats(trades: List[dict]) -> dict:
    wins   = sum(1 for t in trades if t["pnl"] > 0)
    losses = sum(1 for t in trades if t["pnl"] <= 0)
    total  = wins + losses
    pnl    = sum(t["pnl"] for t in trades)
    win_rate = (wins / total * 100) if total else 0.0
    drawdown = sum(t["pnl"] for t in trades if t["pnl"] < 0)
    avg_win  = (sum(t["pnl"] for t in trades if t["pnl"] > 0) / wins) if wins else 0
    avg_loss = (sum(t["pnl"] for t in trades if t["pnl"] <= 0) / losses) if losses else 0
    return {
        "wins": wins, "losses": losses, "total": total,
        "pnl": round(pnl, 4),
        "win_rate": round(win_rate, 1),
        "drawdown": round(drawdown, 4),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  RECOLECTOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

async def collect_state() -> dict:
    now = time.time()
    redis_ok = False
    strategy = {}
    executor = {}
    watchdog = {}

    try:
        r = await _get_redis()
        if r:
            await r.ping()
            redis_ok = True
            raw_s = await r.get("state:strategy:latest")
            raw_e = await r.get("state:executor:latest")
            raw_w = await r.get("state:watchdog:latest")
            if raw_s:
                strategy = json.loads(raw_s)
            if raw_e:
                executor = json.loads(raw_e)
            if raw_w:
                watchdog = json.loads(raw_w)
    except Exception as ex:
        log.debug(f"Redis error: {ex}")
        await _reset_redis()

    btc = strategy.get("btc_price")
    mom = strategy.get("btc_momentum")
    session_pnl = strategy.get("session_pnl", 0.0)

    if btc:
        _btc_history.append({"t": now, "v": btc})
    _pnl_history.append({"t": now, "v": session_pnl})

    all_trades = _read_trades_csv(5000)
    recent     = all_trades[-50:]
    stats      = _compute_stats(all_trades)

    # Cumulative PnL curve from CSV
    cum = 0.0
    pnl_curve = []
    for t in all_trades:
        cum += t["pnl"]
        pnl_curve.append({"t": t["exit_time"], "v": round(cum, 4)})

    # Bot alive check (< 20s since last heartbeat)
    def _alive(data: dict) -> bool:
        ts_raw = data.get("timestamp")
        if not ts_raw:
            return False
        try:
            # ISO string (strategy) o float string (executor)
            ts = float(ts_raw)
        except (ValueError, TypeError):
            try:
                ts = datetime.fromisoformat(str(ts_raw)).timestamp()
            except Exception:
                return False
        return (now - ts) < 20

    strategy_alive = _alive(strategy)
    executor_alive = _alive(executor)
    watchdog_alive = _alive(watchdog)
    optimizer_alive = watchdog.get("optimizer_ok", False) if watchdog else False

    open_positions = strategy.get("positions_data", [])
    params         = strategy.get("params", {})

    return {
        "ts":          now,
        "redis_ok":    redis_ok,
        "dry_run":     strategy.get("dry_run", True),
        "symbol":      strategy.get("symbol", "BTC/USDT"),

        # Price & momentum
        "btc_price":    btc,
        "btc_momentum": mom,
        "btc_direction": strategy.get("btc_direction", "—"),
        "btc_history":   list(_btc_history),

        # PnL
        "session_pnl":  session_pnl,
        "global_pnl":   round(pnl_curve[-1]["v"], 4) if pnl_curve else 0.0,
        "pnl_history":  list(_pnl_history),
        "pnl_curve":    pnl_curve[-200:],

        # Stats
        "stats":       stats,

        # Positions
        "open_positions": open_positions,
        "entries_paused": strategy.get("entries_paused", False),

        # Trades
        "recent_trades": recent,

        # Bot status
        "bots": {
            "strategy": {"alive": strategy_alive, "data": {
                k: v for k, v in strategy.items()
                if k not in ("positions_data", "params", "btc_history")
            }},
            "executor":  {"alive": executor_alive,  "data": {**executor, "balance_usdt": executor.get("balance_usdt", "")}},
            "watchdog":  {"alive": watchdog_alive,  "data": watchdog},
            "optimizer": {"alive": optimizer_alive, "data": {}},
        },

        # Params
        "params": params,

        # Loss tracker semanal/mensual
        "loss_limits": _get_loss_limits(),

        # Logs
        "logs": list(_log_buf),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  SUBSCRIPTOR DE LOGS EN REDIS
# ─────────────────────────────────────────────────────────────────────────────

def _get_loss_limits() -> dict:
    """Devuelve resumen de loss_tracker. No-op si el módulo no está disponible."""
    if not _HAS_LOSS_TRACKER:
        return {}
    try:
        summary = _loss_tracker.get_summary()
        status, msg = _loss_tracker.check()
        return {
            "week_key":          summary.get("week_key", ""),
            "month_key":         summary.get("month_key", ""),
            "weekly_pnl":        summary.get("weekly_pnl", 0.0),
            "monthly_pnl":       summary.get("monthly_pnl", 0.0),
            "weekly_limit":      summary.get("weekly_limit", 150.0),
            "monthly_limit":     summary.get("monthly_limit", 400.0),
            "weekly_pct":        round(summary.get("weekly_pct_used", 0.0), 1),
            "monthly_pct":       round(summary.get("monthly_pct_used", 0.0), 1),
            "status":            status,
            "msg":               msg,
        }
    except Exception:
        return {}


async def log_subscriber():
    """Suscribe a logs:unified y health:heartbeats para el live log stream."""
    while True:
        try:
            r = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe("logs:unified", "health:heartbeats")
            log.info("Log subscriber connected")
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                try:
                    data = json.loads(msg["data"])
                    channel = msg["channel"]

                    if channel == "health:heartbeats":
                        bot  = data.get("bot", "?")
                        stat = data.get("status", "?")
                        txt  = f"[HB] {bot} → {stat}"
                    else:
                        level = data.get("level", "INFO")
                        src   = data.get("source", "")
                        txt   = data.get("message", str(data))
                        txt   = f"[{level}] {src}: {txt}" if src else f"[{level}] {txt}"

                    ts_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
                    _log_buf.append({"ts": ts_str, "msg": txt[:200]})
                except Exception:
                    pass
        except Exception as ex:
            log.warning(f"Log subscriber error: {ex} — retrying in 5s")
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
#  LOOP DE BROADCAST
# ─────────────────────────────────────────────────────────────────────────────

async def broadcast_loop():
    while True:
        try:
            state = await collect_state()
            _state.update(state)
            payload = json.dumps(state, default=str)

            dead = set()
            for ws in list(_ws_clients):
                try:
                    await ws.send_str(payload)
                except Exception:
                    dead.add(ws)
            _ws_clients -= dead
        except Exception as ex:
            log.error(f"Broadcast error: {ex}")
        await asyncio.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
#  HANDLERS HTTP / WS
# ─────────────────────────────────────────────────────────────────────────────

async def handle_ws(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    _ws_clients.add(ws)
    log.info(f"WS client connected ({len(_ws_clients)} total)")
    try:
        if _state:
            await ws.send_str(json.dumps(_state, default=str))
        async for _ in ws:
            pass
    finally:
        _ws_clients.discard(ws)
        log.info(f"WS client disconnected ({len(_ws_clients)} remaining)")
    return ws


async def handle_api_state(request):
    return web.json_response(_state if _state else await collect_state(), dumps=lambda o: json.dumps(o, default=str))


async def handle_index(request):
    return web.Response(text=_HTML, content_type="text/html")


# ─────────────────────────────────────────────────────────────────────────────
#  HTML / CSS / JS  (SPA embebida)
# ─────────────────────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CalvinBTC · Trading Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg:       #0d1117;
    --surface:  #161b22;
    --border:   #30363d;
    --text:     #e6edf3;
    --muted:    #8b949e;
    --green:    #3fb950;
    --red:      #f85149;
    --yellow:   #d29922;
    --blue:     #58a6ff;
    --purple:   #bc8cff;
    --orange:   #e3b341;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; font-size: 13px; }
  header {
    display: flex; align-items: center; gap: 16px;
    padding: 12px 20px; background: var(--surface);
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 100;
  }
  header h1 { font-size: 16px; font-weight: 600; letter-spacing: .5px; }
  .badge { padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
  .badge-live   { background: #1a3a1a; color: var(--green);  border: 1px solid var(--green); }
  .badge-dry    { background: #3a2a0a; color: var(--orange); border: 1px solid var(--orange); }
  .badge-paused { background: #3a1a1a; color: var(--red);    border: 1px solid var(--red); }
  .ts { margin-left: auto; color: var(--muted); font-size: 11px; }

  main { padding: 16px 20px; display: flex; flex-direction: column; gap: 16px; max-width: 1600px; margin: 0 auto; }

  /* KPI row */
  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }
  .kpi {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 14px 16px;
  }
  .kpi-label   { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .5px; margin-bottom: 6px; }
  .kpi-value   { font-size: 22px; font-weight: 700; line-height: 1; }
  .kpi-sub     { color: var(--muted); font-size: 11px; margin-top: 4px; }
  .kpi-explain { color: var(--muted); font-size: 10px; margin-top: 6px; padding-top: 6px; border-top: 1px solid var(--border); opacity: 0.75; line-height: 1.4; }
  .pos { color: var(--green); }
  .neg { color: var(--red); }
  .neu { color: var(--text); }

  /* Bot status strip */
  .bots-row { display: flex; gap: 10px; flex-wrap: wrap; }
  .bot-pill {
    display: flex; align-items: center; gap: 6px;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 6px 12px; font-size: 12px;
  }
  .dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .dot-on  { background: var(--green); box-shadow: 0 0 6px var(--green); }
  .dot-off { background: var(--red); }

  /* Charts row */
  .charts-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  @media (max-width: 900px) { .charts-row { grid-template-columns: 1fr; } }
  .chart-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 16px;
  }
  .card-title { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .5px; margin-bottom: 12px; }
  .chart-wrap { position: relative; height: 160px; }

  /* Tables */
  .section { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
  .section-header { padding: 12px 16px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; }
  .section-title { font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: .5px; color: var(--muted); }
  .count-badge { background: var(--border); border-radius: 99px; padding: 1px 7px; font-size: 11px; color: var(--text); }
  table { width: 100%; border-collapse: collapse; }
  th { padding: 8px 14px; text-align: left; font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .4px; border-bottom: 1px solid var(--border); font-weight: 500; }
  td { padding: 9px 14px; border-bottom: 1px solid #1c2128; font-size: 12px; white-space: nowrap; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #1c2128; }
  .empty-row td { text-align: center; color: var(--muted); padding: 24px; }

  /* Reason pill */
  .reason { padding: 2px 7px; border-radius: 4px; font-size: 10px; font-weight: 600; }
  .reason-sl       { background: #3a1a1a; color: var(--red); }
  .reason-tp       { background: #1a3a1a; color: var(--green); }
  .reason-tp_partial { background: #1a2a3a; color: var(--blue); }
  .reason-time     { background: #2a2a1a; color: var(--yellow); }
  .reason-other    { background: var(--border); color: var(--muted); }

  /* Bottom row */
  .bottom-row { display: grid; grid-template-columns: 1fr 320px; gap: 12px; }
  @media (max-width: 1100px) { .bottom-row { grid-template-columns: 1fr; } }

  /* Log stream */
  .log-box {
    height: 220px; overflow-y: auto; padding: 8px 12px;
    font-family: 'Consolas', 'Courier New', monospace; font-size: 11px;
    line-height: 1.6; color: var(--muted);
  }
  .log-entry-ERROR { color: var(--red); }
  .log-entry-WARNING { color: var(--yellow); }
  .log-entry-INFO { color: #7ac0f5; }
  .log-entry-HB   { color: #5a6e7a; }
  .log-ts { color: #404850; margin-right: 6px; }

  /* Params panel */
  .params-grid { display: flex; flex-direction: column; gap: 0; }
  .param-row { display: flex; justify-content: space-between; padding: 7px 16px; border-bottom: 1px solid #1c2128; font-size: 12px; }
  .param-row:last-child { border-bottom: none; }
  .param-key { color: var(--muted); }
  .param-val { font-weight: 600; color: var(--blue); font-family: monospace; }

  /* Scrollbar */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
</style>
</head>
<body>
<header>
  <h1>CalvinBTC · Trading System</h1>
  <span id="mode-badge" class="badge badge-dry">DRY RUN</span>
  <span id="pause-badge" class="badge badge-paused" style="display:none">ENTRIES PAUSED</span>
  <span id="redis-badge" class="badge" style="background:#1a1a2a;color:var(--purple);border:1px solid var(--purple)">Redis ●</span>
  <span class="ts" id="last-update">—</span>
</header>

<main>
  <!-- KPI row -->
  <div class="kpi-row">

    <div class="kpi" title="Precio actual de Bitcoin en dólares. El bot compra y vende BTC automáticamente.">
      <div class="kpi-label">Precio BTC</div>
      <div class="kpi-value neu" id="kpi-btc">—</div>
      <div class="kpi-sub" id="kpi-mom">mom: —</div>
      <div class="kpi-explain">Precio en tiempo real · mom = velocidad de subida/bajada</div>
    </div>

    <div class="kpi" title="Ganancia o pérdida desde que el bot arrancó hoy. Verde = ganando, Rojo = perdiendo.">
      <div class="kpi-label">PnL de Hoy</div>
      <div class="kpi-value" id="kpi-pnl">—</div>
      <div class="kpi-sub" id="kpi-drawdown">drawdown: —</div>
      <div class="kpi-explain">Lo ganado/perdido hoy · drawdown = mayor racha de pérdidas</div>
    </div>

    <div class="kpi" title="Suma de todas las ganancias y pérdidas desde el primer día. Es el resultado real total del bot.">
      <div class="kpi-label">PnL Total (siempre)</div>
      <div class="kpi-value" id="kpi-global-pnl">—</div>
      <div class="kpi-sub" id="kpi-global-trades">todos los trades</div>
      <div class="kpi-explain">Resultado acumulado histórico · verde = bot rentable</div>
    </div>

    <div class="kpi" title="De cada 100 operaciones, cuántas terminaron ganando. Por encima del 60% es bueno. Por encima del 70% es excelente.">
      <div class="kpi-label">% Aciertos</div>
      <div class="kpi-value neu" id="kpi-wr">—</div>
      <div class="kpi-sub" id="kpi-trades">0 trades</div>
      <div class="kpi-explain">% de operaciones ganadoras · +60% es bueno, +70% excelente</div>
    </div>

    <div class="kpi" title="Cuánto gana el bot cuando acierta vs cuánto pierde cuando falla. R:R = ratio riesgo/beneficio. Con R:R 0.6 necesitas ganar el 62% de las veces para ser rentable.">
      <div class="kpi-label">Ganancia / Pérdida media</div>
      <div class="kpi-value neu" id="kpi-avgwl">—</div>
      <div class="kpi-sub" id="kpi-rr">R:R —</div>
      <div class="kpi-explain">Promedio por operación · R:R = cuánto ganas por cada $ que arriesgas</div>
    </div>

    <div class="kpi" title="Operaciones abiertas ahora mismo. El bot tiene dinero en juego esperando a que el precio llegue al objetivo.">
      <div class="kpi-label">Posiciones Abiertas</div>
      <div class="kpi-value neu" id="kpi-openpos">0</div>
      <div class="kpi-sub" id="kpi-unrealized">unrealized: —</div>
      <div class="kpi-explain">Operaciones en curso · unrealized = ganancia/pérdida aún no cerrada</div>
    </div>

    <div class="kpi" title="Tu saldo disponible en Binance. El bot usa una parte fija (stake) en cada operación.">
      <div class="kpi-label">Saldo en Binance</div>
      <div class="kpi-value neu" id="kpi-balance">—</div>
      <div class="kpi-sub" id="kpi-stake">stake: —</div>
      <div class="kpi-explain">Dinero disponible · stake = monto usado por operación</div>
    </div>

    <div class="kpi" title="Límite de pérdida semanal. Si el bot pierde más de este monto en 7 días, se detiene automáticamente para proteger el capital.">
      <div class="kpi-label">Límite Semanal</div>
      <div class="kpi-value neu" id="kpi-weekly">—</div>
      <div class="kpi-sub" id="kpi-weekly-sub">— / —</div>
      <div class="kpi-explain">Freno de seguridad semanal · se detiene si supera el límite</div>
    </div>

    <div class="kpi" title="Límite de pérdida mensual. Protección a largo plazo para no perder más de un monto fijo en 30 días.">
      <div class="kpi-label">Límite Mensual</div>
      <div class="kpi-value neu" id="kpi-monthly">—</div>
      <div class="kpi-sub" id="kpi-monthly-sub">— / —</div>
      <div class="kpi-explain">Freno de seguridad mensual · protección del capital a largo plazo</div>
    </div>

  </div>

  <!-- Bot status -->
  <div class="bots-row" id="bots-row">
    <div class="bot-pill"><span class="dot dot-off" id="dot-redis"></span> Redis</div>
    <div class="bot-pill"><span class="dot dot-off" id="dot-strategy"></span> Signal Engine</div>
    <div class="bot-pill"><span class="dot dot-off" id="dot-executor"></span> Trade Executor</div>
    <div class="bot-pill"><span class="dot dot-off" id="dot-optimizer"></span> Param Optimizer</div>
    <div class="bot-pill"><span class="dot dot-off" id="dot-watchdog"></span> Watchdog</div>
  </div>

  <!-- Charts -->
  <div class="charts-row">
    <div class="chart-card">
      <div class="card-title">BTC Price (2 min)</div>
      <div class="chart-wrap"><canvas id="chart-btc"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="card-title">Cumulative PnL (USDT)</div>
      <div class="chart-wrap"><canvas id="chart-pnl"></canvas></div>
    </div>
  </div>

  <!-- Open positions -->
  <div class="section">
    <div class="section-header">
      <span class="section-title">Open Positions</span>
      <span class="count-badge" id="open-count">0</span>
    </div>
    <table>
      <thead><tr>
        <th>ID</th><th>Entry Price</th><th>Qty (BTC)</th>
        <th>TP</th><th>SL</th><th>Unrealized PnL</th><th>Holding</th>
      </tr></thead>
      <tbody id="open-tbody"><tr class="empty-row"><td colspan="7">No open positions</td></tr></tbody>
    </table>
  </div>

  <!-- Recent trades -->
  <div class="section">
    <div class="section-header">
      <span class="section-title">Recent Trades</span>
      <span class="count-badge" id="trades-count">0</span>
    </div>
    <table>
      <thead><tr>
        <th>#</th><th>Entry</th><th>Exit</th><th>Qty</th>
        <th>PnL (USDT)</th><th>Reason</th><th>Hold</th><th>Mode</th>
      </tr></thead>
      <tbody id="trades-tbody"><tr class="empty-row"><td colspan="8">No trades yet</td></tr></tbody>
    </table>
  </div>

  <!-- Bottom: logs + params -->
  <div class="bottom-row">
    <div class="section">
      <div class="section-header"><span class="section-title">Live Log Stream</span></div>
      <div class="log-box" id="log-box"></div>
    </div>
    <div class="section">
      <div class="section-header"><span class="section-title">Strategy Parameters</span></div>
      <div class="params-grid" id="params-grid">
        <div class="param-row"><span class="param-key">—</span><span class="param-val">—</span></div>
      </div>
    </div>
  </div>
</main>

<script>
// ── Chart.js init ────────────────────────────────────────────────────────────
const chartDefaults = {
  responsive: true, maintainAspectRatio: false,
  animation: false,
  plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
  scales: {
    x: { display: false },
    y: { grid: { color: '#1c2128' }, ticks: { color: '#8b949e', font: { size: 10 } } }
  }
};

const btcCtx = document.getElementById('chart-btc').getContext('2d');
const btcChart = new Chart(btcCtx, {
  type: 'line',
  data: { labels: [], datasets: [{ data: [], borderColor: '#58a6ff', borderWidth: 1.5, pointRadius: 0, fill: true, backgroundColor: 'rgba(88,166,255,.07)', tension: 0.3 }] },
  options: { ...chartDefaults }
});

const pnlCtx = document.getElementById('chart-pnl').getContext('2d');
const pnlChart = new Chart(pnlCtx, {
  type: 'line',
  data: { labels: [], datasets: [{ data: [], borderWidth: 1.5, pointRadius: 0, fill: true, tension: 0.2 }] },
  options: { ...chartDefaults }
});

// ── Helpers ──────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt  = (v, d=2) => v == null ? '—' : Number(v).toFixed(d);
const fmtP = v => v == null ? '—' : (v >= 0 ? '+' : '') + Number(v).toFixed(2);
const ts2s = ts => {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString('es', { hour12: false });
};
const hold = secs => {
  if (!secs || secs < 0) return '—';
  if (secs < 60) return secs.toFixed(0) + 's';
  return (secs / 60).toFixed(1) + 'm';
};
const colorClass = v => v > 0 ? 'pos' : v < 0 ? 'neg' : 'neu';

function reasonClass(r) {
  if (!r) return 'reason-other';
  r = r.toLowerCase();
  if (r.includes('stop') || r === 'sl') return 'reason-sl';
  if (r === 'tp_partial') return 'reason-tp_partial';
  if (r.includes('tp') || r.includes('profit')) return 'reason-tp';
  if (r.includes('time')) return 'reason-time';
  return 'reason-other';
}

// ── State update ─────────────────────────────────────────────────────────────
let lastLogCount = 0;

function applyState(s) {
  const now = Date.now() / 1000;

  // Header
  $('last-update').textContent = 'Updated ' + new Date().toLocaleTimeString('es', { hour12: false });
  const modeBadge = $('mode-badge');
  modeBadge.textContent = s.dry_run ? 'DRY RUN' : 'LIVE';
  modeBadge.className = 'badge ' + (s.dry_run ? 'badge-dry' : 'badge-live');
  $('pause-badge').style.display = s.entries_paused ? 'inline' : 'none';

  const rBadge = $('redis-badge');
  rBadge.style.color = s.redis_ok ? 'var(--green)' : 'var(--red)';
  rBadge.textContent = s.redis_ok ? 'Redis ●' : 'Redis ✕';

  // Dots
  $('dot-redis').className     = 'dot ' + (s.redis_ok ? 'dot-on' : 'dot-off');
  $('dot-strategy').className  = 'dot ' + (s.bots?.strategy?.alive  ? 'dot-on' : 'dot-off');
  $('dot-executor').className  = 'dot ' + (s.bots?.executor?.alive  ? 'dot-on' : 'dot-off');
  $('dot-optimizer').className = 'dot ' + (s.bots?.optimizer?.alive ? 'dot-on' : 'dot-off');
  $('dot-watchdog').className  = 'dot ' + (s.bots?.watchdog?.alive  ? 'dot-on' : 'dot-off');

  // KPIs
  const btc = s.btc_price;
  $('kpi-btc').textContent = btc ? '$' + Number(btc).toLocaleString('en', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : '—';
  $('kpi-btc').className = 'kpi-value neu';

  const mom = s.btc_momentum;
  const dir = s.btc_direction || '—';
  $('kpi-mom').textContent = mom != null ? `mom: ${fmtP(mom * 100)}% ${dir}` : 'mom: —';

  const spnl = s.session_pnl ?? 0;
  $('kpi-pnl').textContent  = fmtP(spnl) + ' USDT';
  $('kpi-pnl').className    = 'kpi-value ' + colorClass(spnl);
  $('kpi-drawdown').textContent = 'drawdown: ' + fmt(s.stats?.drawdown ?? 0) + ' USDT';

  const gpnl = s.global_pnl ?? 0;
  $('kpi-global-pnl').textContent = fmtP(gpnl) + ' USDT';
  $('kpi-global-pnl').className   = 'kpi-value ' + colorClass(gpnl);
  $('kpi-global-trades').textContent = (s.stats?.total ?? 0) + ' trades históricos';

  const st = s.stats || {};
  $('kpi-wr').textContent    = st.win_rate != null ? st.win_rate + '%' : '—';
  $('kpi-trades').textContent = (st.total ?? 0) + ' trades (' + (st.wins ?? 0) + 'W/' + (st.losses ?? 0) + 'L)';

  const aw = st.avg_win ?? 0, al = st.avg_loss ?? 0;
  $('kpi-avgwl').textContent = `+${fmt(aw)} / ${fmt(al)}`;
  const rr = al !== 0 ? (aw / Math.abs(al)).toFixed(2) : '—';
  $('kpi-rr').textContent    = 'R:R ' + rr;

  const openPos = s.open_positions || [];
  $('kpi-openpos').textContent = openPos.length;
  const totalUnreal = openPos.reduce((acc, p) => {
    const unreal = btc && p.entry_price ? (btc - p.entry_price) * (p.qty_base || 0) : 0;
    return acc + unreal;
  }, 0);
  $('kpi-unrealized').textContent = 'unrealized: ' + fmtP(totalUnreal) + ' USDT';

  const bal = parseFloat(s.bots?.executor?.data?.balance_usdt || '0');
  $('kpi-balance').textContent = bal > 0 ? '$' + Number(bal).toLocaleString('en', {minimumFractionDigits:2, maximumFractionDigits:2}) + ' USDT' : '—';
  const params = s.params || {};
  $('kpi-stake').textContent = 'stake: $' + fmt(params.STAKE_USD ?? 0) + ' | ' + (s.symbol || 'BTC/USDT');

  // Charts — BTC
  if (s.btc_history && s.btc_history.length) {
    btcChart.data.labels   = s.btc_history.map(p => ts2s(p.t));
    btcChart.data.datasets[0].data = s.btc_history.map(p => p.v);
    btcChart.update('none');
  }

  // Charts — PnL
  if (s.pnl_curve && s.pnl_curve.length) {
    const last = s.pnl_curve[s.pnl_curve.length - 1]?.v ?? 0;
    const pnlColor = last >= 0 ? '#3fb950' : '#f85149';
    pnlChart.data.labels   = s.pnl_curve.map(p => ts2s(p.t));
    pnlChart.data.datasets[0].data = s.pnl_curve.map(p => p.v);
    pnlChart.data.datasets[0].borderColor = pnlColor;
    pnlChart.data.datasets[0].backgroundColor = last >= 0 ? 'rgba(63,185,80,.07)' : 'rgba(248,81,73,.07)';
    pnlChart.update('none');
  }

  // Open positions table
  $('open-count').textContent = openPos.length;
  const obody = $('open-tbody');
  if (!openPos.length) {
    obody.innerHTML = '<tr class="empty-row"><td colspan="7">No open positions</td></tr>';
  } else {
    obody.innerHTML = openPos.map(p => {
      const unreal = btc && p.entry_price ? (btc - p.entry_price) * (p.qty_base || 0) : 0;
      const heldSec = p.opened_at ? (now - p.opened_at) : null;
      const uClass = colorClass(unreal);
      return `<tr>
        <td style="color:var(--muted);font-family:monospace">${(p.position_id || '').slice(-8)}</td>
        <td>$${Number(p.entry_price || 0).toLocaleString('en', {minimumFractionDigits:2})}</td>
        <td>${fmt(p.qty_base, 6)}</td>
        <td style="color:var(--green)">$${Number(p.tp_price || 0).toLocaleString('en', {minimumFractionDigits:2})}</td>
        <td style="color:var(--red)">$${Number(p.sl_price || 0).toLocaleString('en', {minimumFractionDigits:2})}</td>
        <td class="${uClass}" style="font-weight:600">${fmtP(unreal)} USDT</td>
        <td style="color:var(--muted)">${hold(heldSec)}</td>
      </tr>`;
    }).join('');
  }

  // Recent trades table
  const trades = (s.recent_trades || []).slice().reverse().slice(0, 25);
  $('trades-count').textContent = s.stats?.total ?? 0;
  const tbody = $('trades-tbody');
  if (!trades.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="8">No trades yet</td></tr>';
  } else {
    tbody.innerHTML = trades.map((t, i) => {
      const pnlClass = colorClass(t.pnl);
      const heldSec = t.entry_time && t.exit_time ? t.exit_time - t.entry_time : null;
      const rc = reasonClass(t.exit_reason);
      return `<tr>
        <td style="color:var(--muted)">${i + 1}</td>
        <td>$${Number(t.entry_price || 0).toLocaleString('en', {minimumFractionDigits:2})}</td>
        <td>$${Number(t.exit_price  || 0).toLocaleString('en', {minimumFractionDigits:2})}</td>
        <td style="font-family:monospace">${fmt(t.qty, 6)}</td>
        <td class="${pnlClass}" style="font-weight:600">${fmtP(t.pnl)}</td>
        <td><span class="reason ${rc}">${t.exit_reason || '?'}</span></td>
        <td style="color:var(--muted)">${hold(heldSec)}</td>
        <td style="color:${t.mode === 'LIVE' ? 'var(--green)' : 'var(--yellow)'};font-size:10px">${t.mode}</td>
      </tr>`;
    }).join('');
  }

  // Loss limits (weekly / monthly)
  const ll = s.loss_limits || {};
  if (ll.weekly_pnl !== undefined) {
    const wPnl = ll.weekly_pnl ?? 0;
    const mPnl = ll.monthly_pnl ?? 0;
    const wPct = ll.weekly_pct ?? 0;
    const mPct = ll.monthly_pct ?? 0;
    const wStatus = ll.status === 'breached' ? 'neg' : wPct >= 75 ? 'neg' : wPct >= 50 ? 'kpi-value' : 'pos';
    const mStatus = ll.status === 'breached' ? 'neg' : mPct >= 75 ? 'neg' : mPct >= 50 ? 'kpi-value' : 'pos';
    $('kpi-weekly').className   = 'kpi-value ' + wStatus;
    $('kpi-weekly').textContent = fmtP(wPnl) + ' USDT';
    $('kpi-weekly-sub').textContent = wPct.toFixed(0) + '% de $' + fmt(ll.weekly_limit ?? 150, 0);
    $('kpi-monthly').className   = 'kpi-value ' + mStatus;
    $('kpi-monthly').textContent = fmtP(mPnl) + ' USDT';
    $('kpi-monthly-sub').textContent = mPct.toFixed(0) + '% de $' + fmt(ll.monthly_limit ?? 400, 0);
  }

  // Params panel
  const pg = $('params-grid');
  if (params && Object.keys(params).length) {
    pg.innerHTML = Object.entries(params).map(([k, v]) =>
      `<div class="param-row"><span class="param-key">${k}</span><span class="param-val">${v}</span></div>`
    ).join('');
  }

  // Logs
  const logs = s.logs || [];
  if (logs.length !== lastLogCount) {
    lastLogCount = logs.length;
    const box = $('log-box');
    const wasBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
    box.innerHTML = logs.slice(-80).map(l => {
      const lvl = l.msg.startsWith('[HB]') ? 'HB' : (l.msg.match(/\[(ERROR|WARNING|INFO)\]/) || ['', 'INFO'])[1];
      return `<div class="log-entry-${lvl}"><span class="log-ts">${l.ts}</span>${escHtml(l.msg)}</div>`;
    }).join('');
    if (wasBottom) box.scrollTop = box.scrollHeight;
  }
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── WebSocket ────────────────────────────────────────────────────────────────
let ws, wsRetry = 1000;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen    = () => { wsRetry = 1000; };
  ws.onmessage = e => { try { applyState(JSON.parse(e.data)); } catch(_) {} };
  ws.onclose   = () => { setTimeout(connect, wsRetry); wsRetry = Math.min(wsRetry * 1.5, 15000); };
  ws.onerror   = () => ws.close();
}

connect();
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────────────────
#  APP SETUP
# ─────────────────────────────────────────────────────────────────────────────

async def on_startup(app):
    asyncio.create_task(broadcast_loop())
    asyncio.create_task(log_subscriber())
    log.info(f"Dashboard started on http://0.0.0.0:{PORT}/")


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",          handle_index)
    app.router.add_get("/api/state", handle_api_state)
    app.router.add_get("/ws",        handle_ws)
    app.on_startup.append(on_startup)
    return app


if __name__ == "__main__":
    app = make_app()
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)
