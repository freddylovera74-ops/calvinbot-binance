"""
dashboard_server.py — Dashboard tiempo real CalvinBot (reescritura completa)

Servidor: aiohttp en http://localhost:8081/
Rutas:
  GET  /          -> HTML SPA
  GET  /api/state -> JSON estado completo
  GET  /ws        -> WebSocket push cada 2s
  POST /api/stop  -> Detiene todos los bots
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
    print("ERROR: pip install aiohttp")
    sys.exit(1)

try:
    import redis.asyncio as aioredis
except ImportError:
    print("ERROR: pip install redis")
    sys.exit(1)

# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT      = int(os.getenv("DASHBOARD_PORT", "8081"))
HISTORY_LEN = 90  # puntos de precio a mantener (~180s a 2s/ciclo)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DASH] %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("dashboard")

# ──────────────────────────────────────────────────────────────────────────────
#  ESTADO GLOBAL
# ──────────────────────────────────────────────────────────────────────────────

_up_history:   deque = deque(maxlen=HISTORY_LEN)
_down_history: deque = deque(maxlen=HISTORY_LEN)
_log_buf:      deque = deque(maxlen=200)
_ws_clients:   Set[web.WebSocketResponse] = set()

# Tailing de archivos de log — fallback cuando logs:unified está silencioso
_log_file_pos: Dict[str, int] = {}   # {filename: byte_offset}

_state: dict = {
    "ts": 0.0,
    "redis_ok": False,
    "dry_run": True,
    "btc_price": None,
    "btc_momentum": None,
    "up_price": None,
    "down_price": None,
    "up_history": [],
    "down_history": [],
    "market_slug": None,
    "market_end_ts": None,
    "window_pnl": 0.0,
    "stats": {"wins": 0, "losses": 0, "pnl": 0.0},
    "stake_usd": 5.0,
    "open_positions": [],
    "recent_trades": [],
    "bots": {
        "calculator": {"alive": False, "last_ts": 0},
        "executor":   {"alive": False, "last_ts": 0},
        "watchdog":   {"alive": False, "last_ts": 0},
        "optimizer":  {"alive": False, "last_ts": 0},
    },
    "logs": [],
}


# ──────────────────────────────────────────────────────────────────────────────
#  RECOLECTOR DE DATOS
# ──────────────────────────────────────────────────────────────────────────────

class Collector:
    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None

    async def _r(self) -> Optional[aioredis.Redis]:
        if self._redis is None:
            try:
                self._redis = aioredis.Redis.from_url(
                    REDIS_URL, decode_responses=True, socket_timeout=3
                )
            except Exception:
                pass
        return self._redis

    async def collect(self) -> dict:
        now = time.time()
        redis_ok = False
        calc = {}

        try:
            r = await self._r()
            if r:
                await r.ping()
                redis_ok = True
                raw = await r.get("state:calculator:latest")
                if raw:
                    calc = json.loads(raw)
        except Exception as e:
            log.debug(f"Redis error: {e}")
            self._redis = None

        window_pnl    = _read_window_pnl()
        recent_trades = _read_trades_csv(30)   # últimos 30 para la tabla

        # Timestamp puede ser ISO string o float
        raw_ts = calc.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                from datetime import datetime as _dt
                calc_ts = _dt.fromisoformat(raw_ts).timestamp()
            except Exception:
                calc_ts = now
        else:
            calc_ts = float(raw_ts) if raw_ts else now

        prices = calc.get("prices", {})
        up_p   = prices.get("UP")
        down_p = prices.get("DOWN")

        if up_p is not None:
            _up_history.append(up_p)
        if down_p is not None:
            _down_history.append(down_p)

        # Stats totales directamente desde Redis (fuente de verdad del bot)
        redis_stats = calc.get("stats", {})
        open_pos    = calc.get("open_positions", [])
        params      = calc.get("params", {})

        # Totales acumulados del bot (toda la sesión + histórico cargado al inicio)
        total_wins   = redis_stats.get("wins",   0)
        total_losses = redis_stats.get("losses", 0)
        total_pnl    = redis_stats.get("pnl",    0.0)

        # Drawdown: suma de todos los PnL negativos del CSV completo
        all_trades = _read_trades_csv(5000)
        drawdown = sum(t["pnl"] for t in all_trades if t["pnl"] < 0)

        # Estado de bots por log mtime (fallback si pubsub no llegó)
        bots_state = dict(_state["bots"])
        for bot_name, log_file in [
            ("calculator", "calvin5.log"),
            ("executor",   "executor.log"),
            ("watchdog",   "watchdog.log"),
            ("optimizer",  "dynamic_optimizer.log"),
        ]:
            mtime = _log_mtime(BASE_DIR / log_file)
            cur   = bots_state[bot_name]
            if mtime > cur.get("last_ts", 0):
                bots_state[bot_name] = {**cur, "last_ts": mtime}

        # Calcular alive (activo si latido < 30s)
        for bot_name in bots_state:
            last = bots_state[bot_name].get("last_ts", 0)
            bots_state[bot_name]["alive"] = (now - last) < 30

        return {
            "ts":           now,
            "redis_ok":     redis_ok,
            "dry_run":      calc.get("dry_run", True),
            "btc_price":    calc.get("btc_price"),
            "btc_momentum": calc.get("btc_momentum"),
            "up_price":     up_p,
            "down_price":   down_p,
            "up_history":   list(_up_history),
            "down_history": list(_down_history),
            "market_slug":  calc.get("market_slug"),
            "market_end_ts": calc.get("market_end_ts"),
            "window_pnl":   window_pnl,
            "stats": {
                "wins":   total_wins,
                "losses": total_losses,
                "pnl":    total_pnl,
                "total":  total_wins + total_losses,
            },
            "drawdown":     drawdown,
            "stake_usd":    float(params.get("STAKE_USD", 5.0)),
            "open_positions": open_pos,
            "recent_trades":  recent_trades,
            "bots":           bots_state,
            "logs":           list(_log_buf),
        }


def _read_window_pnl() -> float:
    path = BASE_DIR / "calvin5_window.json"
    try:
        if path.exists():
            d = json.loads(path.read_text(encoding="utf-8"))
            if d.get("date") == _today_madrid():
                return float(d.get("pnl", 0.0))
    except Exception:
        pass
    return 0.0


def _read_trades_csv(n: int = 20) -> List[dict]:
    path = BASE_DIR / "calvin5_trades.csv"
    if not path.exists():
        return []
    try:
        trades = []
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    et = float(row.get("entry_time", 0) or 0)
                    xt = float(row.get("exit_time",  0) or 0)
                    pnl = float(row.get("pnl", 0) or 0)
                    trades.append({
                        "id":          row.get("id", ""),
                        "side":        row.get("side", ""),
                        "entry_price": float(row.get("entry_price", 0) or 0),
                        "exit_price":  float(row.get("exit_price",  0) or 0),
                        "size_usd":    float(row.get("size_usd",    0) or 0),
                        "pnl":         pnl,
                        "exit_reason": row.get("exit_reason", ""),
                        "entry_time":  et,
                        "exit_time":   xt,
                        "mode":        row.get("mode", "DRY"),
                    })
                except (ValueError, KeyError):
                    pass
        trades.sort(key=lambda t: t["entry_time"])
        return trades[-n:]
    except Exception:
        return []


def _log_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime if path.exists() else 0.0
    except Exception:
        return 0.0


# Mapa de archivos .log → nombre de bot para el panel de logs
_LOG_FILES = {
    "calvin5.log":           "calvin5",
    "executor.log":          "executor",
    "watchdog.log":          "watchdog",
    "dynamic_optimizer.log": "optimizer",
}
# Patrones a ignorar (spam que no aporta al dashboard)
_LOG_SKIP = ("DEBUG", "[HB]", "HB-STREAM", "WD-STREAM", "socket.timeout")


def _tail_log_files() -> None:
    """Lee líneas nuevas de cada .log desde la última posición y las añade a _log_buf."""
    now = time.time()
    for fname, bot in _LOG_FILES.items():
        path = BASE_DIR / fname
        if not path.exists():
            continue
        try:
            size = path.stat().st_size
            last_pos = _log_file_pos.get(fname, -1)

            # Primera vez: posicionarse al final para no inundar de histórico
            if last_pos == -1:
                _log_file_pos[fname] = size
                continue

            # Sin cambios
            if size <= last_pos:
                continue

            with open(path, "rb") as f:
                f.seek(last_pos)
                new_bytes = f.read(size - last_pos)
                _log_file_pos[fname] = f.tell()

            lines = new_bytes.decode("utf-8", errors="replace").splitlines()
            for raw in lines:
                line = raw.strip()
                if not line:
                    continue
                # Filtrar spam
                if any(skip in line for skip in _LOG_SKIP):
                    continue
                # Detectar nivel
                level = "INFO"
                for lvl in ("CRITICAL", "ERROR", "WARNING", "DEBUG"):
                    if lvl in line:
                        level = lvl
                        break
                # Limpiar prefijo de timestamp si existe
                msg = line
                parts = line.split(None, 3)
                if len(parts) >= 3 and len(parts[0]) == 10 and parts[0][4] == "-":
                    # "2026-03-26 10:30:00 LEVEL msg..."
                    msg = parts[-1] if len(parts) == 4 else line
                _log_buf.append({
                    "bot":   bot,
                    "level": level,
                    "msg":   msg[:200],
                    "ts":    now,
                })
        except Exception:
            pass


def _today_madrid() -> str:
    try:
        from utils import madrid_today_str
        return madrid_today_str()
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _ts_to_date_madrid(ts: float) -> str:
    try:
        from utils import madrid_now
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        from zoneinfo import ZoneInfo
        dt = dt.astimezone(ZoneInfo("Europe/Madrid"))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


# ──────────────────────────────────────────────────────────────────────────────
#  TAREAS DE FONDO
# ──────────────────────────────────────────────────────────────────────────────

collector = Collector()


async def task_collect():
    """Recolecta datos cada 2s y actualiza _state."""
    global _state
    while True:
        try:
            # Tail log files como fuente de logs (fallback a logs:unified)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _tail_log_files)
            new = await collector.collect()
            new["logs"] = list(_log_buf)   # merged: pub/sub + file tail
            _state.update(new)
        except Exception as e:
            log.warning(f"collect error: {e}")
        await asyncio.sleep(2)


async def task_pubsub():
    """Suscribe a canales Redis para heartbeats y logs."""
    while True:
        try:
            r = await aioredis.from_url(REDIS_URL, decode_responses=True)
            ps = r.pubsub()
            await ps.subscribe("health:heartbeats", "execution:logs", "logs:unified")
            log.info("Suscrito a Redis pub/sub")
            async for msg in ps.listen():
                if msg["type"] != "message":
                    continue
                try:
                    data = json.loads(msg["data"])
                except Exception:
                    continue
                now = time.time()
                ch  = msg["channel"]

                if ch == "health:heartbeats":
                    bot = data.get("bot", "")
                    if bot in _state["bots"]:
                        _state["bots"][bot].update({
                            "last_ts": now,
                            "alive":   True,
                            **{k: v for k, v in data.items() if k not in ("bot",)},
                        })

                elif ch == "execution:logs":
                    if data.get("status") == "FILLED":
                        entry = {
                            "bot":   "executor",
                            "level": "INFO",
                            "msg":   f"FILL {data.get('side','?')} {data.get('market_slug','?')} @ {data.get('price','?')}",
                            "ts":    now,
                        }
                        _log_buf.append(entry)

                elif ch == "logs:unified":
                    _log_buf.append({
                        "bot":   data.get("bot", "?"),
                        "level": data.get("level", "INFO"),
                        "msg":   data.get("msg", ""),
                        "ts":    now,
                    })
                    _state["logs"] = list(_log_buf)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning(f"pubsub error: {e} — reconectando en 5s")
            await asyncio.sleep(5)


async def task_broadcast():
    """Envía _state a todos los clientes WebSocket cada 2s."""
    while True:
        await asyncio.sleep(2)
        if not _ws_clients:
            continue
        try:
            payload = json.dumps(_state, default=str)
        except Exception:
            continue
        dead = set()
        for ws in list(_ws_clients):
            try:
                await ws.send_str(payload)
            except Exception:
                dead.add(ws)
        _ws_clients.difference_update(dead)


# ──────────────────────────────────────────────────────────────────────────────
#  HANDLERS HTTP
# ──────────────────────────────────────────────────────────────────────────────

async def handle_index(req: web.Request) -> web.Response:
    return web.Response(text=HTML, content_type="text/html", charset="utf-8")


async def handle_api_state(req: web.Request) -> web.Response:
    return web.json_response(_state, dumps=lambda o: json.dumps(o, default=str))


async def handle_ws(req: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(req)
    _ws_clients.add(ws)
    try:
        await ws.send_str(json.dumps(_state, default=str))
        async for msg in ws:
            if msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
    except Exception:
        pass
    finally:
        _ws_clients.discard(ws)
    return ws


async def handle_stop(req: web.Request) -> web.Response:
    stopped = []
    try:
        r = await aioredis.from_url(REDIS_URL, decode_responses=True)
        cmd = {
            "source":    "dashboard",
            "command":   "CLOSE_ALL",
            "priority":  "SUPREME",
            "reason":    "Stop manual desde Dashboard",
            "timestamp": datetime.utcnow().isoformat(),
        }
        await r.publish("emergency:commands", json.dumps(cmd))
        await r.publish("signals:control", json.dumps({"command": "PAUSE"}))
        stopped.append("redis:CLOSE_ALL")
    except Exception as e:
        log.warning(f"CLOSE_ALL error: {e}")

    pids_file = BASE_DIR / "calvinbot_pids.json"
    if pids_file.exists():
        try:
            import psutil
            pids = json.loads(pids_file.read_text(encoding="utf-8"))
            for name, pid in reversed(list(pids.items())):
                if name == "dashboard":
                    continue
                try:
                    proc = psutil.Process(pid)
                    proc.terminate()
                    stopped.append(f"{name}:{pid}")
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"Kill PIDs error: {e}")

    return web.json_response({"ok": True, "stopped": stopped})


# ──────────────────────────────────────────────────────────────────────────────
#  HTML / CSS / JS
# ──────────────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>CalvinBot Dashboard</title>
<style>
:root {
  --bg:      #0a0b0f;
  --card:    #12141a;
  --border:  #1e2028;
  --green:   #00d48a;
  --red:     #ff4757;
  --yellow:  #ffd32a;
  --blue:    #3d8ef8;
  --text:    #e2e8f0;
  --muted:   #64748b;
  --label:   #94a3b8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, monospace; font-size: 13px; }

/* ── HEADER ── */
#header {
  display: flex; align-items: center; gap: 20px;
  padding: 10px 20px; background: var(--card);
  border-bottom: 1px solid var(--border);
  flex-wrap: wrap;
}
#header .logo { font-size: 15px; font-weight: 700; color: var(--green); letter-spacing: 1px; }
#header .sep { color: var(--border); }
.hval { font-weight: 600; }
.hlabel { color: var(--muted); font-size: 11px; margin-right: 3px; }
.badge {
  padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700;
  background: var(--border); letter-spacing: 0.5px;
}
.badge.dry  { background: #2d3748; color: var(--yellow); }
.badge.real { background: #1a3a1a; color: var(--green); }
.dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
.dot.on  { background: var(--green); box-shadow: 0 0 6px var(--green); }
.dot.off { background: var(--red); }
#clock { margin-left: auto; color: var(--muted); font-size: 12px; }

/* ── LAYOUT ── */
#main { padding: 14px 18px; display: flex; flex-direction: column; gap: 14px; }

/* ── METRICS ROW ── */
#metrics { display: grid; grid-template-columns: repeat(7, 1fr); gap: 10px; }
.metric-card {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 12px 14px;
}
.metric-card .label { font-size: 10px; font-weight: 700; color: var(--muted); letter-spacing: 1px; text-transform: uppercase; }
.metric-card .value { font-size: 22px; font-weight: 700; margin: 4px 0 2px; line-height: 1; }
.metric-card .sub   { font-size: 11px; color: var(--muted); }
.positive { color: var(--green); }
.negative { color: var(--red); }
.neutral  { color: var(--text); }

/* ── PRICES ROW ── */
#prices { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.price-card {
  background: var(--card); border-radius: 8px; padding: 14px 16px;
  border: 1.5px solid var(--border);
  position: relative; overflow: hidden;
}
.price-card.up-card   { border-color: var(--green); }
.price-card.down-card { border-color: var(--red); }

.price-header { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
.price-dot { width: 10px; height: 10px; border-radius: 50%; }
.up-card   .price-dot { background: var(--green); box-shadow: 0 0 8px var(--green); }
.down-card .price-dot { background: var(--red);   box-shadow: 0 0 8px var(--red); }
.price-label { font-size: 11px; font-weight: 700; letter-spacing: 2px; }
.up-card   .price-label { color: var(--green); }
.down-card .price-label { color: var(--red); }
.price-direction { font-size: 11px; color: var(--muted); margin-left: auto; }

.price-main { font-size: 36px; font-weight: 700; font-variant-numeric: tabular-nums; }
.up-card   .price-main { color: var(--green); }
.down-card .price-main { color: var(--red); }

.sparkline-wrap { margin-top: 10px; height: 50px; }
svg.spark { width: 100%; height: 50px; }
.spark-line-up   { stroke: var(--green); fill: none; stroke-width: 1.5; }
.spark-fill-up   { fill: url(#grad-up); stroke: none; }
.spark-line-down { stroke: var(--red);  fill: none; stroke-width: 1.5; }
.spark-fill-down { fill: url(#grad-down); stroke: none; }

.price-footer { display: flex; gap: 16px; margin-top: 8px; font-size: 11px; color: var(--muted); }
.price-footer span b { color: var(--text); }

/* ── MARKET INFO ── */
#market-bar {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 10px 16px;
  display: flex; gap: 24px; align-items: center; flex-wrap: wrap;
}
.mbar-item { display: flex; flex-direction: column; gap: 2px; }
.mbar-label { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; }
.mbar-val   { font-size: 13px; font-weight: 600; }
#timer-bar { flex: 1; min-width: 120px; }
#timer-progress {
  height: 4px; background: var(--border); border-radius: 2px; margin-top: 4px;
}
#timer-fill { height: 100%; border-radius: 2px; background: var(--green); transition: width 1s linear; }

/* ── OPEN POSITIONS ── */
#open-section { display: block; }
.open-empty {
  text-align: center; color: var(--muted); padding: 18px;
  font-size: 12px; letter-spacing: 0.5px;
}
.open-count-badge {
  display: inline-flex; align-items: center; justify-content: center;
  min-width: 18px; height: 18px; border-radius: 9px;
  background: var(--green); color: #000; font-size: 10px; font-weight: 700;
  padding: 0 5px; margin-left: 6px; vertical-align: middle;
}
.open-count-badge.zero { background: var(--border); color: var(--muted); }
.pnl-bar {
  display: inline-block; height: 4px; border-radius: 2px;
  margin-left: 6px; vertical-align: middle; min-width: 4px;
}
.status-live { color: var(--green); font-size: 10px; font-weight: 700; letter-spacing: 1px; }
.status-hold { color: var(--yellow); font-size: 10px; font-weight: 700; letter-spacing: 1px; }
.section-title {
  font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;
  color: var(--muted); margin-bottom: 8px;
}

/* ── TABLES ── */
.table-wrap {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; overflow: hidden;
}
table { width: 100%; border-collapse: collapse; }
thead th {
  padding: 8px 12px; text-align: left;
  font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.8px;
  color: var(--muted); background: #0d0f14; border-bottom: 1px solid var(--border);
}
tbody td { padding: 8px 12px; border-bottom: 1px solid #1a1c23; }
tbody tr:last-child td { border-bottom: none; }
tbody tr:hover { background: #15171f; }
.side-up   { color: var(--green); font-weight: 700; }
.side-down { color: var(--red);   font-weight: 700; }
.reason    { font-size: 11px; color: var(--muted); }
.tp   { color: var(--green); }
.sl   { color: var(--red); }
.hold { color: var(--yellow); }

/* ── BOTTOM ROW ── */
#bottom { display: grid; grid-template-columns: 1fr 340px; gap: 10px; }

/* ── LOGS ── */
#logs-panel {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; display: flex; flex-direction: column; max-height: 280px;
}
#logs-header {
  padding: 8px 14px; border-bottom: 1px solid var(--border);
  font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--muted);
  display: flex; align-items: center; justify-content: space-between;
}
#logs-body {
  flex: 1; overflow-y: auto; padding: 6px 0; font-size: 11px; font-family: monospace;
}
.log-line { padding: 2px 14px; line-height: 1.5; }
.log-line:hover { background: #15171f; }
.log-time { color: var(--muted); margin-right: 6px; }
.log-bot  { color: var(--blue); margin-right: 6px; font-weight: 600; }
.log-INFO    { color: var(--text); }
.log-WARNING { color: var(--yellow); }
.log-ERROR   { color: var(--red); font-weight: 600; }
.log-CRITICAL{ color: var(--red); font-weight: 700; }

/* ── BOT STATUS ── */
#bots-panel {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 8px; padding: 14px 16px;
}
.bot-row { display: flex; align-items: center; gap: 10px; padding: 7px 0; border-bottom: 1px solid var(--border); }
.bot-row:last-child { border-bottom: none; }
.bot-dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
.bot-dot.on  { background: var(--green); box-shadow: 0 0 5px var(--green); }
.bot-dot.off { background: var(--red); }
.bot-name { font-weight: 600; min-width: 80px; }
.bot-age  { color: var(--muted); font-size: 11px; margin-left: auto; }

/* ── SCROLLBAR ── */
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* ── RESPONSIVE ── */
@media (max-width: 1300px) {
  #metrics { grid-template-columns: repeat(4, 1fr); }
}
@media (max-width: 1100px) {
  #metrics { grid-template-columns: repeat(3, 1fr); }
}
@media (max-width: 800px) {
  #metrics { grid-template-columns: repeat(2, 1fr); }
  #prices  { grid-template-columns: 1fr; }
  #bottom  { grid-template-columns: 1fr; }
}
</style>
</head>
<body>

<!-- HEADER -->
<div id="header">
  <span class="logo">CALVINBOT</span>
  <span class="sep">|</span>
  <span class="hlabel">BTC</span><span id="h-btc" class="hval">---</span>
  <span id="h-mom" style="font-size:12px;"></span>
  <span class="sep">|</span>
  <span class="dot" id="h-redis-dot"></span>
  <span id="h-redis" style="font-size:11px;">Redis</span>
  <span class="sep">|</span>
  <span id="h-mode" class="badge">---</span>
  <span id="clock"></span>
</div>

<!-- MAIN -->
<div id="main">

  <!-- METRICS -->
  <div id="metrics">
    <div class="metric-card">
      <div class="label">P&L Hoy</div>
      <div class="value" id="m-pnl">$0.00</div>
      <div class="sub" id="m-pnl-sub">ventana activa</div>
    </div>
    <div class="metric-card">
      <div class="label">Win Rate Total</div>
      <div class="value" id="m-wr">--%</div>
      <div class="sub" id="m-wr-sub">0 wins / 0 losses</div>
    </div>
    <div class="metric-card">
      <div class="label">Trades Totales</div>
      <div class="value" id="m-trades">0</div>
      <div class="sub" id="m-trades-sub">0 wins / 0 losses</div>
    </div>
    <div class="metric-card">
      <div class="label">Stake USD</div>
      <div class="value" id="m-stake">$5.00</div>
      <div class="sub">por operación (LP)</div>
    </div>
    <div class="metric-card">
      <div class="label">Drawdown</div>
      <div class="value" id="m-dd">$0.00</div>
      <div class="sub">límite $60</div>
    </div>
    <div class="metric-card">
      <div class="label">Sharpe</div>
      <div class="value" id="m-sharpe">---</div>
      <div class="sub">proxy últimos trades</div>
    </div>
    <div class="metric-card">
      <div class="label">Profit Neto</div>
      <div class="value" id="m-net-profit">$0.00</div>
      <div class="sub" id="m-net-profit-sub">cerradas + abiertas</div>
    </div>
  </div>

  <!-- MARKET BAR -->
  <div id="market-bar">
    <div class="mbar-item">
      <span class="mbar-label">Mercado</span>
      <span class="mbar-val" id="mb-slug">---</span>
    </div>
    <div class="mbar-item" id="timer-bar">
      <span class="mbar-label">Tiempo restante</span>
      <span class="mbar-val" id="mb-timer">--:--</span>
      <div id="timer-progress"><div id="timer-fill" style="width:0%"></div></div>
    </div>
    <div class="mbar-item">
      <span class="mbar-label">BTC Momentum</span>
      <span class="mbar-val" id="mb-mom">---</span>
    </div>
    <div class="mbar-item">
      <span class="mbar-label">Posiciones abiertas</span>
      <span class="mbar-val" id="mb-openpos">0</span>
    </div>
  </div>

  <!-- PRICES -->
  <div id="prices">
    <!-- UP -->
    <div class="price-card up-card">
      <div class="price-header">
        <div class="price-dot"></div>
        <span class="price-label">▲ UP — YES</span>
        <span class="price-direction" id="up-dir"></span>
      </div>
      <div class="price-main" id="up-price">---</div>
      <div class="sparkline-wrap">
        <svg class="spark" id="spark-up" viewBox="0 0 400 50" preserveAspectRatio="none">
          <defs>
            <linearGradient id="grad-up" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="#00d48a" stop-opacity="0.25"/>
              <stop offset="100%" stop-color="#00d48a" stop-opacity="0"/>
            </linearGradient>
          </defs>
          <path class="spark-fill-up" id="spark-fill-up" d=""/>
          <path class="spark-line-up"  id="spark-line-up"  d=""/>
        </svg>
      </div>
      <div class="price-footer">
        <span>Min: <b id="up-min">---</b></span>
        <span>Max: <b id="up-max">---</b></span>
        <span>Puntos: <b id="up-pts">0</b></span>
      </div>
    </div>

    <!-- DOWN -->
    <div class="price-card down-card">
      <div class="price-header">
        <div class="price-dot"></div>
        <span class="price-label">▼ DOWN — NO</span>
        <span class="price-direction" id="down-dir"></span>
      </div>
      <div class="price-main" id="down-price">---</div>
      <div class="sparkline-wrap">
        <svg class="spark" id="spark-down" viewBox="0 0 400 50" preserveAspectRatio="none">
          <defs>
            <linearGradient id="grad-down" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="#ff4757" stop-opacity="0.25"/>
              <stop offset="100%" stop-color="#ff4757" stop-opacity="0"/>
            </linearGradient>
          </defs>
          <path class="spark-fill-down" id="spark-fill-down" d=""/>
          <path class="spark-line-down"  id="spark-line-down"  d=""/>
        </svg>
      </div>
      <div class="price-footer">
        <span>Min: <b id="down-min">---</b></span>
        <span>Max: <b id="down-max">---</b></span>
        <span>Puntos: <b id="down-pts">0</b></span>
      </div>
    </div>
  </div>

  <!-- OPEN POSITIONS -->
  <div id="open-section">
    <div class="section-title">
      Posiciones Abiertas
      <span class="open-count-badge zero" id="open-count-badge">0</span>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Lado</th><th>Mercado</th><th>Entrada</th>
            <th>Precio Actual</th><th>Peak</th><th>Tamaño</th>
            <th>PnL Est.</th><th>PnL %</th><th>Tiempo</th><th>Status</th>
          </tr>
        </thead>
        <tbody id="open-tbody">
          <tr><td colspan="10" class="open-empty">Sin posiciones abiertas</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- RECENT TRADES -->
  <div>
    <div class="section-title">Trades Recientes</div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Lado</th><th>Mercado</th><th>Entrada</th><th>Salida</th>
            <th>Tamaño</th><th>PnL</th><th>Razón</th><th>Duración</th><th>Modo</th>
          </tr>
        </thead>
        <tbody id="trades-tbody"><tr><td colspan="9" style="text-align:center;color:var(--muted);padding:20px">Cargando trades...</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- BOTTOM: LOGS + BOTS -->
  <div id="bottom">
    <div>
      <div class="section-title">Log en Tiempo Real</div>
      <div id="logs-panel">
        <div id="logs-header">
          <span>Actividad</span>
          <span id="log-count" style="color:var(--muted)">0 líneas</span>
        </div>
        <div id="logs-body"></div>
      </div>
    </div>

    <div>
      <div class="section-title">Estado de Bots</div>
      <div id="bots-panel">
        <div class="bot-row" id="br-calculator">
          <div class="bot-dot off" id="bd-calculator"></div>
          <span class="bot-name">Calculator</span>
          <span class="bot-age" id="ba-calculator">---</span>
        </div>
        <div class="bot-row" id="br-executor">
          <div class="bot-dot off" id="bd-executor"></div>
          <span class="bot-name">Executor</span>
          <span class="bot-age" id="ba-executor">---</span>
        </div>
        <div class="bot-row" id="br-watchdog">
          <div class="bot-dot off" id="bd-watchdog"></div>
          <span class="bot-name">Watchdog</span>
          <span class="bot-age" id="ba-watchdog">---</span>
        </div>
        <div class="bot-row" id="br-optimizer">
          <div class="bot-dot off" id="bd-optimizer"></div>
          <span class="bot-name">Optimizer</span>
          <span class="bot-age" id="ba-optimizer">---</span>
        </div>
        <div style="margin-top:14px;padding-top:10px;border-top:1px solid var(--border);">
          <button id="btn-stop" onclick="stopBots()" style="
            width:100%;padding:8px;border:1px solid var(--red);background:transparent;
            color:var(--red);border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;
            letter-spacing:1px;
          ">STOP ALL BOTS</button>
        </div>
      </div>
    </div>
  </div>

</div><!-- /main -->

<script>
// ── SPARKLINE ──────────────────────────────────────────────────────────────
function buildSparkPath(data, W, H, pad) {
  if (!data || data.length < 2) return { line: '', fill: '' };
  const mn = Math.min(...data);
  const mx = Math.max(...data);
  const rng = mx - mn || 0.01;
  const pts = data.map((v, i) => {
    const x = pad + (i / (data.length - 1)) * (W - pad * 2);
    const y = H - pad - ((v - mn) / rng) * (H - pad * 2);
    return [x, y];
  });
  const line = 'M ' + pts.map(p => p[0].toFixed(1) + ',' + p[1].toFixed(1)).join(' L ');
  const fill = line + ` L ${pts[pts.length-1][0].toFixed(1)},${H} L ${pts[0][0].toFixed(1)},${H} Z`;
  return { line, fill };
}

function updateSpark(id, fillId, data) {
  const el = document.getElementById(id);
  const fl = document.getElementById(fillId);
  if (!el || !fl) return;
  const { line, fill } = buildSparkPath(data, 400, 50, 2);
  el.setAttribute('d', line);
  fl.setAttribute('d', fill);
}

// ── FORMATO ────────────────────────────────────────────────────────────────
const fmt2 = v => (v != null ? v.toFixed(2) : '---');
const fmt4 = v => (v != null ? v.toFixed(4) : '---');
const fmtPct = v => (v != null ? (v * 100).toFixed(1) + '%' : '---');
const fmtUSD = v => (v != null ? (v >= 0 ? '+$' : '-$') + Math.abs(v).toFixed(2) : '$0.00');

function ageSec(ts) {
  if (!ts) return '---';
  const s = Math.floor(Date.now() / 1000 - ts);
  if (s < 60)  return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ago';
}

function fmtDuration(secs) {
  if (!secs || secs < 0) return '---';
  const m = Math.floor(secs / 60), s = Math.floor(secs % 60);
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function reasonClass(r) {
  if (!r) return '';
  const lo = r.toLowerCase();
  if (lo.includes('tp')) return 'tp';
  if (lo.includes('sl')) return 'sl';
  if (lo.includes('hold')) return 'hold';
  return '';
}

function sharpeProxy(trades) {
  if (!trades || trades.length < 5) return '---';
  const pnls = trades.slice(-20).map(t => t.pnl);
  const mean = pnls.reduce((a,b) => a+b, 0) / pnls.length;
  const std  = Math.sqrt(pnls.map(p => (p-mean)**2).reduce((a,b)=>a+b,0) / pnls.length);
  if (std < 0.001) return '---';
  return (mean / std).toFixed(2);
}

// ── CLOCK ─────────────────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  document.getElementById('clock').textContent =
    now.toLocaleTimeString('es-ES', { timeZone: 'Europe/Madrid', hour12: false });
}
setInterval(updateClock, 1000);
updateClock();

// ── TIMER ─────────────────────────────────────────────────────────────────
let _endTs = null;
function updateTimer() {
  if (!_endTs) return;
  const left = _endTs - Date.now() / 1000;
  const total = 300;
  const mm = Math.max(0, Math.floor(left / 60));
  const ss = Math.max(0, Math.floor(left % 60));
  document.getElementById('mb-timer').textContent =
    String(mm).padStart(2,'0') + ':' + String(ss).padStart(2,'0');
  const pct = Math.max(0, Math.min(100, (left / total) * 100));
  const fill = document.getElementById('timer-fill');
  fill.style.width = pct + '%';
  fill.style.background = left < 30 ? 'var(--red)' : left < 60 ? 'var(--yellow)' : 'var(--green)';
}
setInterval(updateTimer, 500);

// ── RENDER ─────────────────────────────────────────────────────────────────
let _prevUpPrice = null, _prevDownPrice = null;

function render(s) {
  // Header
  const btcOk = s.btc_price != null;
  document.getElementById('h-btc').textContent = btcOk
    ? '$' + s.btc_price.toLocaleString('en-US', {maximumFractionDigits: 0})
    : '---';

  const mom = s.btc_momentum;
  const momEl = document.getElementById('h-mom');
  if (mom != null) {
    const sign = mom >= 0 ? '▲' : '▼';
    momEl.textContent = sign + ' ' + Math.abs(mom).toFixed(4) + '%';
    momEl.style.color = mom >= 0 ? 'var(--green)' : 'var(--red)';
  } else {
    momEl.textContent = '';
  }

  const rdot = document.getElementById('h-redis-dot');
  rdot.className = 'dot ' + (s.redis_ok ? 'on' : 'off');
  document.getElementById('h-redis').textContent = s.redis_ok ? 'Redis OK' : 'Redis OFF';

  const modeEl = document.getElementById('h-mode');
  modeEl.textContent = s.dry_run ? 'DRY RUN' : 'REAL MONEY';
  modeEl.className = 'badge ' + (s.dry_run ? 'dry' : 'real');

  // Metrics
  const pnl = s.window_pnl || 0;
  const pnlEl = document.getElementById('m-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);
  pnlEl.className = 'value ' + (pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral');

  // Stats totales del bot (directos desde Redis, toda la sesión)
  const st = s.stats || {};
  const wins   = st.wins   || 0;
  const losses = st.losses || 0;
  const total  = st.total  || (wins + losses);
  const wr = total > 0 ? (wins / total * 100).toFixed(1) + '%' : '--%';

  const wrEl = document.getElementById('m-wr');
  wrEl.textContent = wr;
  wrEl.className = 'value ' + (total > 0 ? (wins/total >= 0.5 ? 'positive' : 'negative') : 'neutral');
  document.getElementById('m-wr-sub').textContent = wins + ' wins / ' + losses + ' losses';

  const trEl = document.getElementById('m-trades');
  trEl.textContent = total;
  document.getElementById('m-trades-sub').textContent = wins + ' wins / ' + losses + ' losses';

  document.getElementById('m-stake').textContent = '$' + (s.stake_usd || 5).toFixed(2);

  // Drawdown desde el servidor (calculado sobre CSV completo)
  const ddVal = s.drawdown || 0;
  const ddEl = document.getElementById('m-dd');
  ddEl.textContent = '$' + Math.abs(ddVal).toFixed(2);
  ddEl.className = 'value ' + (Math.abs(ddVal) > 30 ? 'negative' : Math.abs(ddVal) > 15 ? '' : 'neutral');

  document.getElementById('m-sharpe').textContent = sharpeProxy(s.recent_trades);

  // Profit Neto = PnL cerradas (stats.pnl) + PnL estimado posiciones abiertas
  const closedPnl = (s.stats && s.stats.pnl != null) ? s.stats.pnl : 0;
  let openPnlSum = 0;
  const openPosForNet = s.open_positions || [];
  for (const p of openPosForNet) {
    const curP = p.side === 'UP' ? (s.up_price || null) : (s.down_price || null);
    if (curP != null && p.entry_price != null) {
      openPnlSum += (curP - p.entry_price) * (p.size_tokens || 0);
    }
  }
  const netProfit = closedPnl + openPnlSum;
  const npEl = document.getElementById('m-net-profit');
  npEl.textContent = (netProfit >= 0 ? '+$' : '-$') + Math.abs(netProfit).toFixed(2);
  npEl.className = 'value ' + (netProfit > 0 ? 'positive' : netProfit < 0 ? 'negative' : 'neutral');
  const npSub = document.getElementById('m-net-profit-sub');
  const openPnlStr = openPnlSum >= 0 ? '+$' + openPnlSum.toFixed(2) : '-$' + Math.abs(openPnlSum).toFixed(2);
  npSub.textContent = 'cerradas $' + closedPnl.toFixed(2) + ' | abiertas ' + openPnlStr;

  // Market bar
  const slug = s.market_slug || '---';
  document.getElementById('mb-slug').textContent = slug;
  _endTs = s.market_end_ts;
  updateTimer();

  const momVal = s.btc_momentum;
  const momBEl = document.getElementById('mb-mom');
  if (momVal != null) {
    momBEl.textContent = (momVal >= 0 ? '▲ +' : '▼ ') + momVal.toFixed(4) + '%';
    momBEl.style.color = momVal >= 0 ? 'var(--green)' : 'var(--red)';
  } else {
    momBEl.textContent = '---';
    momBEl.style.color = 'var(--muted)';
  }

  const openPos = s.open_positions || [];
  document.getElementById('mb-openpos').textContent = openPos.length;

  // Price UP
  const upP = s.up_price;
  const upEl = document.getElementById('up-price');
  if (upP != null) {
    upEl.textContent = upP.toFixed(4);
    const dir = _prevUpPrice != null
      ? (upP > _prevUpPrice ? '▲' : upP < _prevUpPrice ? '▼' : '—')
      : '—';
    document.getElementById('up-dir').textContent = dir;
    _prevUpPrice = upP;
  } else {
    upEl.textContent = '---';
  }

  const upH = s.up_history || [];
  updateSpark('spark-line-up', 'spark-fill-up', upH);
  if (upH.length > 0) {
    document.getElementById('up-min').textContent = Math.min(...upH).toFixed(4);
    document.getElementById('up-max').textContent = Math.max(...upH).toFixed(4);
  }
  document.getElementById('up-pts').textContent = upH.length;

  // Price DOWN
  const dnP = s.down_price;
  const dnEl = document.getElementById('down-price');
  if (dnP != null) {
    dnEl.textContent = dnP.toFixed(4);
    const dir2 = _prevDownPrice != null
      ? (dnP > _prevDownPrice ? '▲' : dnP < _prevDownPrice ? '▼' : '—')
      : '—';
    document.getElementById('down-dir').textContent = dir2;
    _prevDownPrice = dnP;
  } else {
    dnEl.textContent = '---';
  }

  const dnH = s.down_history || [];
  updateSpark('spark-line-down', 'spark-fill-down', dnH);
  if (dnH.length > 0) {
    document.getElementById('down-min').textContent = Math.min(...dnH).toFixed(4);
    document.getElementById('down-max').textContent = Math.max(...dnH).toFixed(4);
  }
  document.getElementById('down-pts').textContent = dnH.length;

  // Open positions — siempre visible, actualización en tiempo real
  const badge = document.getElementById('open-count-badge');
  badge.textContent = openPos.length;
  badge.className = 'open-count-badge' + (openPos.length > 0 ? '' : ' zero');

  if (openPos.length > 0) {
    const rows = openPos.map(p => {
      const currentP = p.side === 'UP' ? upP : dnP;
      const pnlEst = currentP != null && p.entry_price != null
        ? (currentP - p.entry_price) * (p.size_tokens || 0)
        : null;
      const pnlPct = currentP != null && p.entry_price != null && p.entry_price > 0
        ? ((currentP - p.entry_price) / p.entry_price * 100)
        : null;
      const pnlStr   = pnlEst  != null ? (pnlEst  >= 0 ? '+$' : '-$') + Math.abs(pnlEst).toFixed(2)  : '---';
      const pnlPctStr= pnlPct  != null ? (pnlPct  >= 0 ? '+' : '')   + pnlPct.toFixed(1) + '%'       : '---';
      const pnlClass = pnlEst  != null ? (pnlEst  >= 0 ? 'positive' : 'negative') : '';
      const barW  = pnlPct != null ? Math.min(60, Math.abs(pnlPct) * 4) : 0;
      const barCol= pnlEst != null && pnlEst >= 0 ? 'var(--green)' : 'var(--red)';
      const age   = p.entry_time ? fmtDuration(Date.now()/1000 - p.entry_time) : '---';
      const peakStr = p.peak_price != null ? p.peak_price.toFixed(4) : '---';
      const slug  = (p.market_slug || '---').replace(/btc-/i,'').replace(/-\d+$/,'');
      const statusCls = (p.status || '').toLowerCase().includes('hold') ? 'status-hold' : 'status-live';
      const statusLabel = p.status || 'LIVE';
      return `<tr>
        <td class="side-${(p.side||'').toLowerCase()}">${p.side||'?'}</td>
        <td style="font-size:11px;color:var(--muted)" title="${p.market_slug||''}">${slug}</td>
        <td>${(p.entry_price||0).toFixed(4)}</td>
        <td>${currentP != null ? currentP.toFixed(4) : '<span style="color:var(--muted)">---</span>'}</td>
        <td style="color:var(--muted)">${peakStr}</td>
        <td>$${(p.size_usd||0).toFixed(2)}</td>
        <td class="${pnlClass}">${pnlStr}<span class="pnl-bar" style="width:${barW}px;background:${barCol}"></span></td>
        <td class="${pnlClass}">${pnlPctStr}</td>
        <td style="color:var(--muted)">${age}</td>
        <td class="${statusCls}">${statusLabel}</td>
      </tr>`;
    }).join('');
    document.getElementById('open-tbody').innerHTML = rows;
  } else {
    document.getElementById('open-tbody').innerHTML =
      '<tr><td colspan="10" class="open-empty">Sin posiciones abiertas</td></tr>';
  }

  // Recent trades
  const trades = (s.recent_trades || []).slice().reverse();
  if (trades.length > 0) {
    const rows = trades.map(t => {
      const dur = t.exit_time && t.entry_time ? fmtDuration(t.exit_time - t.entry_time) : '---';
      const pnlSign = t.pnl >= 0 ? '+' : '';
      const rc = reasonClass(t.exit_reason);
      return `<tr>
        <td class="side-${(t.side||'').toLowerCase()}">${t.side||'?'}</td>
        <td style="font-size:10px;color:var(--muted)">${(t.id||'').split('_').slice(0,1).join('')}</td>
        <td>${t.entry_price.toFixed(4)}</td>
        <td>${t.exit_price.toFixed(4)}</td>
        <td>$${(t.size_usd||0).toFixed(2)}</td>
        <td class="${t.pnl>=0?'positive':'negative'}">${pnlSign}$${Math.abs(t.pnl).toFixed(2)}</td>
        <td class="reason ${rc}">${t.exit_reason||'---'}</td>
        <td>${dur}</td>
        <td><span style="font-size:10px;color:var(--muted)">${t.mode||'DRY'}</span></td>
      </tr>`;
    }).join('');
    document.getElementById('trades-tbody').innerHTML = rows;
  }

  // Bots
  const bots = s.bots || {};
  for (const [name, info] of Object.entries(bots)) {
    const dot = document.getElementById('bd-' + name);
    const age = document.getElementById('ba-' + name);
    if (!dot || !age) continue;
    const alive = info.alive;
    dot.className = 'bot-dot ' + (alive ? 'on' : 'off');
    age.textContent = info.last_ts ? ageSec(info.last_ts) : '---';
  }

  // Logs
  const logs = s.logs || [];
  const logsBody = document.getElementById('logs-body');
  document.getElementById('log-count').textContent = logs.length + ' líneas';
  const atBottom = logsBody.scrollHeight - logsBody.scrollTop - logsBody.clientHeight < 40;
  const html = logs.slice(-80).reverse().map(l => {
    const t = l.ts ? new Date(l.ts * 1000).toLocaleTimeString('es-ES', {hour12:false}) : '';
    const lvl = l.level || 'INFO';
    const msg = (l.msg || '').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    return `<div class="log-line log-${lvl}">
      <span class="log-time">${t}</span>
      <span class="log-bot">[${(l.bot||'?').toUpperCase()}]</span>
      <span>${msg}</span>
    </div>`;
  }).join('');
  logsBody.innerHTML = html;
}

// ── WEBSOCKET ──────────────────────────────────────────────────────────────
let ws, reconnectTimer;

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    console.log('WS conectado');
    clearTimeout(reconnectTimer);
  };

  ws.onmessage = (ev) => {
    try {
      render(JSON.parse(ev.data));
    } catch(e) { console.error('parse error', e); }
  };

  ws.onclose = () => {
    console.log('WS desconectado — reconectando en 3s');
    reconnectTimer = setTimeout(connect, 3000);
  };

  ws.onerror = () => ws.close();
}

connect();

// ── STOP ──────────────────────────────────────────────────────────────────
async function stopBots() {
  if (!confirm('¿Detener todos los bots?')) return;
  const btn = document.getElementById('btn-stop');
  btn.disabled = true;
  btn.textContent = 'DETENIENDO...';
  try {
    const r = await fetch('/api/stop', { method: 'POST' });
    const d = await r.json();
    btn.textContent = 'DETENIDO ✓';
    btn.style.color = 'var(--muted)';
  } catch(e) {
    btn.textContent = 'ERROR';
    btn.disabled = false;
  }
}
</script>
</body>
</html>
"""


# ──────────────────────────────────────────────────────────────────────────────
#  STARTUP
# ──────────────────────────────────────────────────────────────────────────────

async def on_startup(app: web.Application):
    asyncio.create_task(task_collect())
    asyncio.create_task(task_pubsub())
    asyncio.create_task(task_broadcast())
    log.info(f"Dashboard iniciado en http://localhost:{PORT}/")


def main():
    app = web.Application()
    app.router.add_get("/",          handle_index)
    app.router.add_get("/api/state", handle_api_state)
    app.router.add_get("/ws",        handle_ws)
    app.router.add_post("/api/stop", handle_stop)
    app.on_startup.append(on_startup)
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)


if __name__ == "__main__":
    main()
