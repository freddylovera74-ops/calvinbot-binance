"""
watchdog_binance.py — Supervisor independiente para CalvinBot Binance

Monitoriza cada 20s:
  - strategy_binance (heartbeat Redis state:strategy:latest)
  - execution_bot    (heartbeat Redis state:executor:latest)
  - Límites de pérdida (loss_tracker)
  - Log activity (strategy_binance.log mtime)

Acciones:
  - WARNING  → Telegram si bot lleva >60s offline
  - CRITICAL → Telegram + reinicio automático con systemctl
  - BREACHED → Pausa executor + Telegram CRÍTICA
  - Max 3 reinicios por hora (protección contra crash loop)
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

import loss_tracker
from utils import send_telegram_async, in_trading_hours

# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
REDIS_URL  = os.getenv("REDIS_URL", "redis://localhost:6379")

CHECK_INTERVAL_S   = int(os.getenv("WD_CHECK_S",          "20"))
HEARTBEAT_MAX_S    = int(os.getenv("WD_HEARTBEAT_MAX_S",   "30"))   # muerto si > 30s
WARN_OFFLINE_S     = int(os.getenv("WD_WARN_OFFLINE_S",    "60"))   # alerta si > 60s
RESTART_OFFLINE_S  = int(os.getenv("WD_RESTART_OFFLINE_S", "120"))  # reiniciar si > 120s
MAX_RESTARTS_PER_H = int(os.getenv("WD_MAX_RESTARTS_H",    "3"))
DRAWDOWN_WARN      = float(os.getenv("WD_DRAWDOWN_WARN",   "40.0"))
DRAWDOWN_KILL      = float(os.getenv("WD_DRAWDOWN_KILL",   "80.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WD] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(BASE_DIR / "watchdog_binance.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("watchdog")

# ─────────────────────────────────────────────────────────────────────────────
#  ESTADO INTERNO
# ─────────────────────────────────────────────────────────────────────────────

_restart_times = []   # timestamps de reinicios en esta hora


def _restarts_this_hour() -> int:
    cutoff = time.time() - 3600
    _restart_times[:] = [t for t in _restart_times if t > cutoff]
    return len(_restart_times)


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _bot_alive(data: dict, max_age_s: int = HEARTBEAT_MAX_S) -> Tuple[bool, float]:
    """Devuelve (alive: bool, age_s: float)."""
    ts_raw = data.get("timestamp")
    if not ts_raw:
        return False, 9999
    try:
        ts = float(ts_raw)
    except (TypeError, ValueError):
        try:
            ts = datetime.fromisoformat(str(ts_raw)).timestamp()
        except Exception:
            return False, 9999
    age = time.time() - ts
    return age < max_age_s, round(age, 1)


async def _get_redis_state(r: aioredis.Redis, key: str) -> dict:
    try:
        raw = await r.get(key)
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _restart_services(*services: str) -> bool:
    """Reinicia servicios via systemctl. Retorna True si tuvo éxito."""
    if _restarts_this_hour() >= MAX_RESTARTS_PER_H:
        log.error(f"Límite de {MAX_RESTARTS_PER_H} reinicios/hora alcanzado — NO reiniciando")
        return False
    try:
        for svc in services:
            subprocess.run(["systemctl", "restart", svc], timeout=30, check=True)
            log.warning(f"systemctl restart {svc} — OK")
        _restart_times.append(time.time())
        return True
    except Exception as ex:
        log.error(f"Error reiniciando servicios: {ex}")
        return False


def _pause_executor(r_sync) -> None:
    """Publica comando PAUSE al executor via Redis."""
    try:
        import redis as sync_redis
        rc = sync_redis.Redis.from_url(REDIS_URL, decode_responses=True)
        rc.publish("emergency:commands", json.dumps({
            "command": "PAUSE",
            "reason":  "watchdog_drawdown_kill",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
        log.warning("Comando PAUSE enviado al executor")
    except Exception as ex:
        log.error(f"Error enviando PAUSE: {ex}")


# ─────────────────────────────────────────────────────────────────────────────
#  ESTADO DE SEGUIMIENTO
# ─────────────────────────────────────────────────────────────────────────────

class BotState:
    def __init__(self, name: str):
        self.name = name
        self.offline_since: float = 0.0      # 0 = está online
        self.warned_offline = False
        self.warned_critical = False

    def mark_alive(self):
        if self.offline_since > 0:
            downtime = round(time.time() - self.offline_since)
            log.info(f"{self.name} volvió online (estuvo offline {downtime}s)")
        self.offline_since = 0.0
        self.warned_offline = False
        self.warned_critical = False

    def mark_dead(self):
        if self.offline_since == 0.0:
            self.offline_since = time.time()

    def offline_seconds(self) -> float:
        if self.offline_since == 0.0:
            return 0.0
        return time.time() - self.offline_since


# ─────────────────────────────────────────────────────────────────────────────
#  CICLO PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

async def check_cycle(r: aioredis.Redis, strategy_st: BotState, executor_st: BotState):
    now = time.time()

    # ── 1. Leer heartbeats ──────────────────────────────────────────────────
    strat_data = await _get_redis_state(r, "state:strategy:latest")
    exec_data  = await _get_redis_state(r, "state:executor:latest")

    strat_alive, strat_age = _bot_alive(strat_data)
    exec_alive,  exec_age  = _bot_alive(exec_data, max_age_s=60)

    # ── 2. Actualizar estado ────────────────────────────────────────────────
    if strat_alive:
        strategy_st.mark_alive()
    else:
        strategy_st.mark_dead()

    if exec_alive:
        executor_st.mark_alive()
    else:
        executor_st.mark_dead()

    strat_off = strategy_st.offline_seconds()
    exec_off  = executor_st.offline_seconds()

    # ── 3. Alertas de bots offline ──────────────────────────────────────────
    for st, off_s, name in [
        (strategy_st, strat_off, "calvinbot-strategy"),
        (executor_st, exec_off,  "calvinbot-executor"),
    ]:
        if off_s > WARN_OFFLINE_S and not st.warned_offline:
            st.warned_offline = True
            log.warning(f"{name} offline desde hace {off_s:.0f}s")
            await send_telegram_async(
                f"{name} lleva {off_s:.0f}s offline",
                level="WARNING", prefix_label="Watchdog"
            )

        if off_s > RESTART_OFFLINE_S and not st.warned_critical:
            st.warned_critical = True
            log.error(f"{name} offline {off_s:.0f}s — reiniciando")
            restarted = _restart_services(name)
            msg = (
                f"{name} reiniciado automáticamente\n"
                f"Offline: {off_s:.0f}s | Reinicios esta hora: {_restarts_this_hour()}"
                if restarted else
                f"FALLO al reiniciar {name} (límite de reinicios alcanzado)"
            )
            await send_telegram_async(
                msg,
                level="CRITICAL" if not restarted else "WARNING",
                prefix_label="Watchdog"
            )

    # ── 4. Ambos offline simultáneamente ───────────────────────────────────
    if strat_off > 30 and exec_off > 30 and in_trading_hours():
        log.error("AMBOS bots offline — reinicio de emergencia")
        _restart_services("calvinbot-strategy", "calvinbot-executor")
        await send_telegram_async(
            f"AMBOS BOTS OFFLINE\nReinicio de emergencia ejecutado\n"
            f"Strategy: {strat_off:.0f}s | Executor: {exec_off:.0f}s",
            level="CRITICAL", prefix_label="Watchdog"
        )

    # ── 5. Límites de pérdida ───────────────────────────────────────────────
    try:
        # loss_tracker.check() devuelve Tuple[str, str] → (status, message)
        lt_status, lt_msg = loss_tracker.check()

        session_pnl = float(strat_data.get("session_pnl", 0.0) or 0.0)
        drawdown    = abs(min(session_pnl, 0.0))

        if lt_status == loss_tracker.STATUS_BREACHED:
            log.error(f"LOSS LIMIT BREACHED — {lt_msg}")
            _pause_executor(None)
            await send_telegram_async(
                f"LIMITE DE PERDIDA SUPERADO\n{lt_msg}\n"
                f"Session PnL: ${session_pnl:.2f}",
                level="CRITICAL", prefix_label="Watchdog"
            )

        elif lt_status == loss_tracker.STATUS_WARNING:
            log.warning(f"Loss limit WARNING — {lt_msg}")

        if drawdown >= DRAWDOWN_KILL:
            log.error(f"DRAWDOWN KILL — sesion PnL: ${session_pnl:.2f}")
            _pause_executor(None)
            await send_telegram_async(
                f"DRAWDOWN KILL activado\nPerdida sesion: ${session_pnl:.2f}\n"
                f"Limite: ${DRAWDOWN_KILL}",
                level="CRITICAL", prefix_label="Watchdog"
            )
        elif drawdown >= DRAWDOWN_WARN:
            log.warning(f"Drawdown WARNING — ${session_pnl:.2f}")

    except Exception as ex:
        log.error(f"Error verificando loss limits: {ex}")

    # ── 6. Heartbeat del watchdog al Redis ─────────────────────────────────
    try:
        await r.setex("state:watchdog:latest", 60, json.dumps({
            "bot":         "watchdog_binance",
            "status":      "online",
            "strategy_ok": strat_alive,
            "executor_ok": exec_alive,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }))
    except Exception:
        pass

    log.debug(
        f"strategy={'OK' if strat_alive else f'OFF {strat_off:.0f}s'} | "
        f"executor={'OK' if exec_alive else f'OFF {exec_off:.0f}s'}"
    )


async def main():
    log.info("=" * 55)
    log.info("  Watchdog Binance — iniciando")
    log.info(f"  Check: {CHECK_INTERVAL_S}s | Restart threshold: {RESTART_OFFLINE_S}s")
    log.info(f"  Max reinicios/h: {MAX_RESTARTS_PER_H} | Drawdown kill: ${DRAWDOWN_KILL}")
    log.info("=" * 55)

    r = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await r.ping()
        log.info(f"Redis conectado: {REDIS_URL}")
    except Exception as ex:
        log.error(f"Redis no disponible: {ex}")
        sys.exit(1)

    await send_telegram_async(
        "Watchdog Binance iniciado\n"
        f"Supervisando strategy + executor\n"
        f"Restart threshold: {RESTART_OFFLINE_S}s | Max restarts/h: {MAX_RESTARTS_PER_H}",
        level="INFO", prefix_label="Watchdog"
    )

    strategy_st = BotState("strategy_binance")
    executor_st = BotState("executor")

    while True:
        try:
            await check_cycle(r, strategy_st, executor_st)
        except Exception as ex:
            log.error(f"Error en ciclo watchdog: {ex}")
        await asyncio.sleep(CHECK_INTERVAL_S)

if __name__ == "__main__":
    asyncio.run(main())
