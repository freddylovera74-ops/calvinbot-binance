"""
watchdog.py — Supervisor independiente para calvin5.py

Monitoriza:
  1. Heartbeat: verifica que calvin5.log se actualiza cada 5 min (ciclo del bot).
     Si pasan >7 min sin actividad → Kill Switch.
  2. Errores: busca patrones de error críticos en calvin5.log (Timeout, Connection,
     RateLimit, SELL FALLIDO, etc.). Si detecta ≥3 errores en 5 min → alerta/kill.
  3. Capital: lee calvin5_window.json para detectar drawdown excesivo.
     Si el PnL del tramo cae >$60 o hay una caída brusca de >$20 en un ciclo → alerta.
  4. Kill Switch: mata el proceso calvin5.py y envía alerta de emergencia a Telegram.

Variables de entorno necesarias (añadir al .env):
  TG_TOKEN         — Token del bot de Telegram (ya presente en calvin5.py)
  TG_CHAT_ID       — Chat ID de Telegram (ya presente en calvin5.py)
  WATCHDOG_CHECK_S — Intervalo de verificación en segundos (default: 30)
  WD_DRAWDOWN_KILL — Umbral de drawdown para kill switch en $ (default: 60)
  WD_ERROR_WINDOW  — Ventana de tiempo para contar errores en segundos (default: 300)
  WD_ERROR_THRESH  — Número de errores críticos para disparar alerta (default: 3)

Dependencias extra:
  pip install requests python-dotenv psutil

Uso:
  python watchdog.py
  python watchdog.py --dry-run   # modo simulación, no mata el proceso
"""

import json
import logging
import os
import re
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import psutil
import requests
import redis
from dotenv import load_dotenv
import loss_tracker  # C4: límites de pérdida globales semanales/mensuales

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN (extraída de calvin5.py y de variables de entorno)
# ─────────────────────────────────────────────────────────────────────────────

# Archivos de estado del bot principal
BOT_SCRIPT       = "calvin5.py"
LOG_FILE         = "calvin5.log"
OPEN_POS_FILE    = "calvin5_open.json"
WINDOW_PNL_FILE  = "calvin5_window.json"

# APIs de Polymarket (fuente de verdad para verificación cruzada)
GAMMA_API        = "https://gamma-api.polymarket.com"
CLOB_API         = "https://clob.polymarket.com"
WALLET_ADDR      = os.getenv("POLYMARKET_ADDRESS", "")

# Redis — canal de comandos de emergencia al Execution Bot
REDIS_URL             = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CH_EMERGENCY    = "emergency:commands"    # Watchdog → Execution Bot
REDIS_CH_STATE        = "state:calculator"      # Calvin5 → Watchdog (auditoría)
REDIS_CH_HEARTBEATS   = "health:heartbeats"     # latidos de todos los bots
_redis_client: Optional[redis.Redis] = None     # inicializado en run_watchdog()

# Auto-healer: rastrea cuándo el executor se puso en pausa para auto-resumir si es recuperable
_executor_paused_since: float = 0.0   # 0.0 = no estaba pausado en el último ciclo

# Razones de pausa RECUPERABLES (auto-resume tras AUTO_RESUME_AFTER_S)
# Estas indican un fallo transitorio de mercado, no un fallo de infraestructura o seguridad
_RECOVERABLE_PAUSE_PATTERNS = (
    "sell fallido definitivo",
    "sell fallido",
    "slippage",
    "fok sin fill",
    "circuitbreaker [red]",
    "circuit breaker",
    "sin liquidez",
    "couldn't be fully filled",
)
# Razones NO recuperables (no auto-resume, solo alerta — requieren revisión manual)
_CRITICAL_PAUSE_PATTERNS = (
    "insufficient funds",
    "rate limit",
    "invalid signature",
    "circuitbreaker [critico]",
    "kill switch",
)
AUTO_RESUME_AFTER_S = 120   # 2 minutos pausado por razón recuperable → auto-resume

# Telegram
TG_TOKEN         = os.getenv("TG_TOKEN", "")
TG_CHAT_ID       = os.getenv("TG_CHAT_ID", "")

# Parámetros del watchdog (configurables por .env)
CHECK_INTERVAL_S = int(os.getenv("WATCHDOG_CHECK_S", "30"))      # cada cuántos segundos verifica
HEARTBEAT_MAX_S  = int(os.getenv("WD_HEARTBEAT_MAX_S", "120"))   # 120s sin log → kill (calvin5 escribe al log cada 5s)
DRAWDOWN_KILL    = float(os.getenv("WD_DRAWDOWN_KILL", "60.0"))  # $60 → kill (igual que SESSION_DRAWDOWN_LIMIT)
DRAWDOWN_WARN    = float(os.getenv("WD_DRAWDOWN_WARN", "40.0"))  # $40 → advertencia
ERROR_WINDOW_S   = int(os.getenv("WD_ERROR_WINDOW", "300"))      # ventana de 5 min para contar errores
ERROR_THRESH     = int(os.getenv("WD_ERROR_THRESH", "2"))        # N errores FATALES → kill (solo patrones CRITICAL)
WARN_THRESH      = int(os.getenv("WD_WARN_THRESH", "10"))        # N advertencias en ventana → aviso Telegram
CYCLE_DROP_WARN  = float(os.getenv("WD_CYCLE_DROP_WARN", "20.0")) # caída brusca de PnL en 1 ciclo

# Modo simulación: no mata el proceso (útil para probar el watchdog)
DRY_RUN = "--dry-run" in sys.argv or os.getenv("DRY_RUN", "true").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING DEL WATCHDOG
# ─────────────────────────────────────────────────────────────────────────────

_log = logging.getLogger("watchdog")
_log.setLevel(logging.DEBUG)

_fh = logging.FileHandler("watchdog.log", encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s",
                                    datefmt="%Y-%m-%d %H:%M:%S"))
_log.addHandler(_fh)

_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(logging.Formatter("%(asctime)s [WD] %(levelname)s %(message)s",
                                    datefmt="%H:%M:%S"))
_log.addHandler(_sh)


from utils import madrid_now as _madrid_now, in_trading_hours as _in_trading_hours, madrid_today_str as _madrid_today_str, configure_structlog  # noqa: E402
configure_structlog("watchdog", log_file="watchdog.log")


def _log_unified(msg: str, level: str = "INFO") -> None:
    """Publica el mensaje al canal logs:unified para el Dashboard (sync)."""
    if _redis_client is None:
        return
    try:
        _redis_client.publish("logs:unified", json.dumps({
            "bot":       "watchdog",
            "level":     level,
            "msg":       msg[:300],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
    except Exception as e:
        # ISSUE #5: Fire-and-forget logging a Redis — ignorable si falla
        pass


def _info(msg: str) -> None:
    _log.info(msg)
    _log_unified(msg, "INFO")


def _warn(msg: str) -> None:
    _log.warning(msg)
    _log_unified(msg, "WARN")


def _err(msg: str) -> None:
    _log.error(msg)
    _log_unified(msg, "ERROR")


# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────

def _send_telegram(msg: str, level: str = "INFO") -> bool:
    """Envía mensaje a Telegram. Devuelve True si tuvo éxito."""
    if not TG_TOKEN or not TG_CHAT_ID:
        _warn("Telegram no configurado (TG_TOKEN / TG_CHAT_ID vacíos)")
        return False

    prefix = {
        "WARNING":  "⚠️  [Calvin5 WATCHDOG] ADVERTENCIA",
        "CRITICAL": "🚨 [Calvin5 WATCHDOG] EMERGENCIA — KILL SWITCH ACTIVADO",
        "INFO":     "ℹ️  [Calvin5 WATCHDOG]",
    }.get(level, "ℹ️  [Calvin5 WATCHDOG]")

    full_msg = f"{prefix}\n\n{msg}\n\n⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"

    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": TG_CHAT_ID, "text": full_msg},
            timeout=10,
        )
        if resp.status_code == 200:
            _info(f"Telegram [{level}] enviado OK")
            return True
        _warn(f"Telegram error HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as exc:
        _warn(f"Telegram fallo de conexión: {exc}")
    return False


# ─────────────────────────────────────────────────────────────────────────────
#  DETECCIÓN Y KILL DEL PROCESO
# ─────────────────────────────────────────────────────────────────────────────

def _find_bot_process() -> Optional[psutil.Process]:
    """Busca el proceso Python que ejecuta calvin5.py."""
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            # Busca "calvin5.py" en la línea de comando del proceso
            if any(BOT_SCRIPT in str(arg) for arg in cmdline):
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def _kill_bot(reason: str) -> bool:
    """Mata el proceso del bot principal. Devuelve True si lo mató."""
    proc = _find_bot_process()
    if proc is None:
        _warn("Kill Switch solicitado pero el proceso no se encontró (¿ya estaba caído?)")
        return False

    pid = proc.pid
    _err(f"KILL SWITCH — matando PID {pid} ({BOT_SCRIPT}). Razón: {reason}")

    if DRY_RUN:
        _warn(f"[DRY-RUN] No se mataría el proceso PID {pid}. Simulando kill.")
        return True

    try:
        # Intento suave primero (SIGTERM → cierre limpio)
        proc.terminate()
        time.sleep(3)
        if proc.is_running():
            # Forzar si sigue vivo
            proc.kill()
            _err(f"SIGKILL enviado a PID {pid}")
        _err(f"Proceso PID {pid} terminado correctamente")
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
        _err(f"Error al matar PID {pid}: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  MÓDULO 1: HEARTBEAT — verificación de actividad del log
# ─────────────────────────────────────────────────────────────────────────────

def check_heartbeat() -> tuple[bool, float]:
    """
    Verifica que calvin5.log se ha actualizado en los últimos HEARTBEAT_MAX_S segundos.
    Devuelve (ok, segundos_desde_ultima_actividad).
    """
    log_path = Path(LOG_FILE)
    if not log_path.exists():
        _warn(f"Log file {LOG_FILE} no existe — bot posiblemente nunca arrancó")
        return False, float("inf")

    mtime = log_path.stat().st_mtime
    elapsed = time.time() - mtime

    # Fuera de horario Madrid el bot está intencionalmente inactivo — no es fallo
    if not _in_trading_hours():
        _info(f"Fuera de horario Madrid ({_madrid_now().hour}h) — heartbeat inactivo es normal")
        return True, elapsed

    if elapsed > HEARTBEAT_MAX_S:
        _err(f"HEARTBEAT FALLO: {elapsed:.0f}s sin actividad en {LOG_FILE} (máx {HEARTBEAT_MAX_S}s)")
        return False, elapsed

    _info(f"Heartbeat OK — última actividad hace {elapsed:.0f}s")

    # ── Verificar latido del ExecutionBot vía Redis ────────────────────────────
    if _redis_client is not None:
        try:
            raw = _redis_client.get("state:calculator:latest")
            # El heartbeat del executor se publica en health:heartbeats pero no se persiste
            # como key. Usamos executor.log como proxy de actividad del executor.
            exec_log = Path("executor.log")
            if exec_log.exists():
                exec_elapsed = time.time() - exec_log.stat().st_mtime
                if exec_elapsed > 30:
                    _warn(f"[EXECUTOR] Sin actividad en executor.log desde hace {exec_elapsed:.0f}s — verificar estado")
        except Exception:
            pass

    return True, elapsed


# ─────────────────────────────────────────────────────────────────────────────
#  MÓDULO 2: ANÁLISIS DE ERRORES en el log
# ─────────────────────────────────────────────────────────────────────────────

# ── Patrones FATALES: el bot no puede recuperarse solo → kill switch ──────────
# Solo errores que indican colapso total e irrecuperable del proceso.
# NO incluir errores que el bot reintenta y resuelve por sí mismo.
CRITICAL_ERROR_PATTERNS = [
    re.compile(r"DRAWDOWN LÍMITE", re.I),            # bot ya se pausó a sí mismo
    re.compile(r"RateLimit|Too Many Requests", re.I),# ban de API, no se recupera solo
    re.compile(r"Traceback \(most recent call", re.I),# excepción no capturada → crash
    re.compile(r"Exception in thread", re.I),        # crash de thread
]

# ── Patrones de AVISO: el bot puede recuperarse, solo notificar ───────────────
# Incluye errores transitorios normales en operación real de Polymarket.
WARNING_ERROR_PATTERNS = [
    # SELL FALLIDO DEFINITIVO: el bot reintenta a los 15s, no es fatal inmediato
    re.compile(r"SELL FALLIDO DEFINITIVO", re.I),
    # Reintentos de SELL: normales por lag de API de Polymarket
    re.compile(r"SELL intento \d+/\d+ FALLIDO", re.I),
    # Gamma API 404: normal cuando no hay mercado activo entre rondas
    re.compile(r"RECONCILE.*Gamma API error", re.I),
    # Timeouts y conexión: transitorios, el bot tiene reintentos internos
    re.compile(r"Timeout|TimeoutError|ReadTimeout|ConnectTimeout", re.I),
    re.compile(r"ConnectionError|ConnectionRefused|ConnectionReset", re.I),
    # Balance/allowance: lag de API de Polymarket, se resuelve solo
    re.compile(r"balance insuficiente|allowance insuficiente|lag de API", re.I),
]

# Cola deslizante para contar errores recientes (timestamp, pattern_match)
_recent_errors: deque = deque(maxlen=100)
_last_log_size: int = 0
_last_log_pos: int = 0


def _parse_log_errors() -> tuple[int, int]:
    """
    Lee las líneas nuevas del log y cuenta errores críticos y advertencias
    en la ventana temporal ERROR_WINDOW_S.
    Devuelve (errores_críticos_recientes, advertencias_recientes).
    """
    global _last_log_pos

    log_path = Path(LOG_FILE)
    if not log_path.exists():
        return 0, 0

    now = time.time()
    new_lines = []

    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            f.seek(_last_log_pos)
            new_lines = f.readlines()
            _last_log_pos = f.tell()
    except Exception as exc:
        _warn(f"Error leyendo log: {exc}")
        return 0, 0

    for line in new_lines:
        line = line.strip()
        if not line:
            continue
        for pat in CRITICAL_ERROR_PATTERNS:
            if pat.search(line):
                _recent_errors.append((now, "CRITICAL", line[:120]))
                _info(f"Error crítico detectado en log: {line[:100]}")
                break

    # Contar errores dentro de la ventana temporal
    cutoff = now - ERROR_WINDOW_S
    critical_count = sum(1 for ts, lvl, _ in _recent_errors if ts > cutoff and lvl == "CRITICAL")
    warn_count = sum(1 for ts, lvl, _ in _recent_errors if ts > cutoff and lvl == "WARNING")

    return critical_count, warn_count


def check_errors() -> tuple[str, int]:
    """
    Analiza errores del log. Devuelve ('ok'|'warning'|'critical', count).
    """
    critical, warnings = _parse_log_errors()

    # Solo kill si hay errores VERDADERAMENTE fatales (crash, ban de API, drawdown hit)
    if critical >= ERROR_THRESH:
        _err(f"ALERTA FATAL: {critical} errores irrecuperables en los últimos {ERROR_WINDOW_S}s")
        return "critical", critical

    # Advertencia si hay muchos errores recuperables acumulados (no kill)
    if warnings >= WARN_THRESH:
        _warn(f"Muchas advertencias: {warnings} en {ERROR_WINDOW_S}s (bot operando con dificultades)")
        return "warning", warnings

    return "ok", 0


# ─────────────────────────────────────────────────────────────────────────────
#  MÓDULO 3: CAPITAL — monitorización de drawdown y exposición
# ─────────────────────────────────────────────────────────────────────────────

_prev_window_pnl: Optional[float] = None


def _read_window_pnl() -> Optional[float]:
    """
    Lee el PnL del tramo horario.
    ISSUE #10: Primero intenta Redis (fuente autoritativa), luego JSON (fallback).
    """
    # ISSUE #10: Intentar Redis primero (fuente única de verdad)
    if _redis_client is not None:
        try:
            key = f"pnl:window:{_madrid_today_str()}"
            pnl_redis = _redis_client.get(key)
            if pnl_redis:
                pnl_val = float(pnl_redis)
                _info(f"[PnL] Leyendo desde Redis: ${pnl_val:+.2f}")
                return pnl_val
        except Exception as exc:
            _warn(f"[PnL] Error leyendo desde Redis: {exc} — cayendo a JSON")
    
    # Fallback: JSON (compatible con bots anteriores)
    path = Path(WINDOW_PNL_FILE)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        today = _madrid_today_str()
        if data.get("date") == today:
            pnl_val = float(data.get("pnl", 0.0))
            # ISSUE #10: Si Redis disponible, sincronizar JSON → Redis
            if _redis_client is not None:
                try:
                    key = f"pnl:window:{today}"
                    _redis_client.setex(key, 86400 + 3600, str(pnl_val))
                    _info(f"[PnL] Sincronizado JSON → Redis para ${pnl_val:+.2f}")
                except Exception:
                    pass  # Sincronización fallida no es crítico
            return pnl_val
    except Exception as exc:
        _warn(f"[PnL] Error leyendo {WINDOW_PNL_FILE}: {exc}")
    return None


def _read_open_positions() -> list:
    """Lee las posiciones abiertas desde calvin5_open.json (solo lectura)."""
    path = Path(OPEN_POS_FILE)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as exc:
        _warn(f"Error leyendo {OPEN_POS_FILE}: {exc}")
        return []


def _fetch_wallet_balance() -> Optional[float]:
    """Consulta el balance USDC de la wallet en Polymarket (solo lectura)."""
    if not WALLET_ADDR:
        return None
    try:
        resp = requests.get(
            f"{CLOB_API}/balance-allowance",
            params={"asset_type": "COLLATERAL"},
            headers={"POLY_ADDRESS": WALLET_ADDR},
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            raw = float(data.get("balance", 0) or 0)
            # Polymarket devuelve 6 decimales (USDC)
            return raw / 1e6 if raw > 1000 else raw
    except Exception as exc:
        _warn(f"Error consultando balance en CLOB: {exc}")
    return None


def check_capital() -> tuple[str, str]:
    """
    Verifica el estado del capital. Devuelve ('ok'|'warning'|'critical', mensaje).
    """
    global _prev_window_pnl

    # Solo verificar drawdown durante horario activo
    if not _in_trading_hours():
        return "ok", "Fuera de horario Madrid — sin verificación de capital"

    window_pnl = _read_window_pnl()
    status = "ok"
    messages = []

    if window_pnl is not None:
        # Drawdown crítico del tramo — mismo umbral que SESSION_DRAWDOWN_LIMIT del bot
        if window_pnl < -DRAWDOWN_KILL:
            msg = f"Drawdown crítico de sesión: ${window_pnl:+.2f} (límite -${DRAWDOWN_KILL:.0f})"
            _err(msg)
            messages.append(msg)
            status = "critical"

        elif window_pnl < -DRAWDOWN_WARN:
            msg = f"Drawdown de advertencia: ${window_pnl:+.2f} (aviso a -${DRAWDOWN_WARN:.0f})"
            _warn(msg)
            messages.append(msg)
            if status == "ok":
                status = "warning"

        # Caída brusca entre ciclos de verificación
        if _prev_window_pnl is not None:
            drop = _prev_window_pnl - window_pnl
            if drop > CYCLE_DROP_WARN:
                msg = f"Caída brusca de PnL: -${drop:.2f} en un ciclo (de ${_prev_window_pnl:+.2f} → ${window_pnl:+.2f})"
                _warn(msg)
                messages.append(msg)
                if status == "ok":
                    status = "warning"

        _prev_window_pnl = window_pnl
        _info(f"Capital OK — PnL sesión: ${window_pnl:+.2f}")

    # Posiciones abiertas — verificación de exposición
    open_pos = _read_open_positions()
    if len(open_pos) > 3:
        msg = f"Exposición inusual: {len(open_pos)} posiciones abiertas simultáneas (máx normal: 1)"
        _warn(msg)
        messages.append(msg)
        if status == "ok":
            status = "warning"

    return status, "; ".join(messages) if messages else "Capital OK"


# ─────────────────────────────────────────────────────────────────────────────
#  MÓDULO 4: ESTADO DEL PROCESO
# ─────────────────────────────────────────────────────────────────────────────

def check_process() -> bool:
    """True si el proceso calvin5.py está corriendo."""
    proc = _find_bot_process()
    if proc is None:
        _warn(f"Proceso {BOT_SCRIPT} NO encontrado en el sistema")
        return False
    _info(f"Proceso {BOT_SCRIPT} activo — PID {proc.pid}")
    return True


# ─────────────────────────────────────────────────────────────────────────────
#  BUCLE PRINCIPAL DEL WATCHDOG
# ─────────────────────────────────────────────────────────────────────────────

# Cooldown para no enviar múltiples alertas del mismo tipo en poco tiempo
_last_alert: dict = {}
ALERT_COOLDOWN_S = 300  # 5 min entre alertas del mismo tipo


def _can_alert(key: str) -> bool:
    now = time.time()
    last = _last_alert.get(key, 0)
    if now - last > ALERT_COOLDOWN_S:
        _last_alert[key] = now
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
#  COMANDOS DE EMERGENCIA → EXECUTION BOT (autoridad suprema del Watchdog)
# ─────────────────────────────────────────────────────────────────────────────

def _send_emergency_command(command: str, reason: str, extra: dict = None) -> bool:
    """
    Publica un comando de emergencia al canal Redis que escucha el Execution Bot.
    El Execution Bot obedece EXCLUSIVAMENTE a Calvin5 (órdenes normales) y al
    Watchdog (órdenes de control). Las órdenes del Watchdog tienen prioridad suprema.

    Comandos disponibles:
      CLOSE_ALL   — cerrar todas las posiciones abiertas inmediatamente
      PAUSE       — bloquear nuevas órdenes de Calvin5 (sigue escuchando Watchdog)
      RESUME      — desbloquear (solo Watchdog puede reanudar tras un PAUSE)
    """
    global _redis_client
    if _redis_client is None:
        _warn(f"[REDIS] Sin conexión Redis — comando {command} no enviado al executor")
        return False

    payload = {
        "source":    "watchdog",
        "command":   command,
        "reason":    reason,
        "priority":  "SUPREME",          # señal de autoridad suprema
        "timestamp": datetime.utcnow().isoformat(),
        **(extra or {}),
    }
    try:
        count = _redis_client.publish(REDIS_CH_EMERGENCY, json.dumps(payload))
        _err(f"[REDIS] Comando de emergencia '{command}' publicado "
             f"({count} executor(s) notificados) — razón: {reason}")
        return True
    except Exception as exc:
        _err(f"[REDIS] Error publicando comando de emergencia: {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  MÓDULO 5: VERIFICACIÓN CRUZADA Calvin5 vs Polymarket
#  (detecta "alucinaciones algorítmicas": el bot cree algo distinto a la realidad)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_polymarket_positions() -> list:
    """Consulta las posiciones reales en la wallet desde Polymarket (fuente de verdad)."""
    if not WALLET_ADDR:
        return []
    try:
        resp = requests.get(
            f"{GAMMA_API}/positions",
            params={"user": WALLET_ADDR, "sizeThreshold": "0.01"},
            timeout=8,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data if isinstance(data, list) else data.get("positions", [])
    except Exception as exc:
        _warn(f"[CROSS-CHECK] Error consultando posiciones en Polymarket: {exc}")
    return []


def _read_calvin5_state_from_redis() -> Optional[dict]:
    """Lee el estado interno que Calvin5 publica en Redis (key state:calculator:latest)."""
    global _redis_client
    if _redis_client is None:
        return None
    try:
        raw = _redis_client.get("state:calculator:latest")
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return None


# Grace period v3:
#   _cross_last_open_count / _cross_last_change_ts — rastrean cuándo cambió n_calvin
#   _cross_discrepancy_ts — timestamp de la PRIMERA vez que se ve Calvin>0 y Poly=0
#   Solo se dispara CRÍTICO si la discrepancia persiste >120s continuos, sin importar
#   cuánto tiempo lleve abierta la posición.
_cross_last_open_count: int   = -1   # -1 = nunca visto
_cross_last_change_ts:  float = 0.0
_cross_discrepancy_ts:  float = 0.0  # 0.0 = sin discrepancia activa


def check_cross_reality() -> tuple[str, str]:
    """
    Compara el estado interno de Calvin5 con la realidad de Polymarket.
    Detecta alucinaciones: el bot reporta posiciones que no existen en el exchange,
    o hay tokens en la wallet que el bot no está trackeando.

    Lógica de grace period:
      - Cada vez que se DETECTA por primera vez que Calvin>0 y Poly=0, se registra
        el timestamp (_cross_discrepancy_ts). Solo se dispara CRÍTICO si esa discrepancia
        se mantiene durante más de CROSS_DISCREPANCY_KILL_S segundos consecutivos.
      - Si la discrepancia desaparece (Poly vuelve a tener posiciones o Calvin llega a 0),
        el contador se resetea.
      - También hay grace period de 90s por cambio reciente de posición (apertura/cierre).

    Devuelve ('ok'|'warning'|'critical', mensaje).
    """
    global _cross_last_open_count, _cross_last_change_ts, _cross_discrepancy_ts

    # Segundos que la discrepancia debe PERSISTIR antes de disparar kill (2 minutos)
    CROSS_DISCREPANCY_KILL_S = 120

    if DRY_RUN:
        return "ok", "DRY_RUN activo — no hay posiciones reales en Polymarket, sin verificación cruzada"

    if not _in_trading_hours():
        return "ok", "Fuera de horario Madrid — sin verificación cruzada"

    calvin_state = _read_calvin5_state_from_redis()
    if calvin_state is None:
        return "ok", "Estado de Calvin5 no disponible en Redis (sin verificación cruzada)"

    poly_positions = _fetch_polymarket_positions()

    calvin_open  = calvin_state.get("open_positions", [])
    messages = []
    status   = "ok"
    now      = time.time()

    # Rastrear cambios en el conteo de posiciones abiertas (para grace por apertura/cierre reciente)
    n_calvin = len([p for p in calvin_open if p.get("status") == "open"])
    if n_calvin != _cross_last_open_count:
        _cross_last_change_ts  = now
        _cross_last_open_count = n_calvin

    secs_since_change = now - _cross_last_change_ts if _cross_last_change_ts > 0 else 9999

    # ① Calvin5 reporta posición abierta pero Polymarket no tiene tokens
    if poly_positions is not None:
        n_poly = len(poly_positions)
        if n_calvin > 0 and n_poly == 0:
            # Grace A: apertura/cierre reciente (Gamma API tarda en propagar)
            if secs_since_change < 90:
                _cross_discrepancy_ts = 0.0  # resetear — el cambio reciente explica la diferencia
                _info(
                    f"[CROSS] Discrepancia pero grace por cambio reciente "
                    f"({secs_since_change:.0f}s < 90s) — ignorando"
                )
                return "ok", f"Grace period: cambio reciente ({secs_since_change:.0f}s)"

            # Grace B: primera detección de discrepancia — iniciar contador
            if _cross_discrepancy_ts == 0.0:
                _cross_discrepancy_ts = now
                _info(
                    f"[CROSS] Primera detección de discrepancia Calvin={n_calvin} Poly=0 "
                    f"— grace period de {CROSS_DISCREPANCY_KILL_S}s iniciado"
                )
                return "ok", f"Grace period: primera detección, esperando {CROSS_DISCREPANCY_KILL_S}s"

            # Grace B en curso: comprobar si ya pasó el tiempo límite
            secs_discrepancy = now - _cross_discrepancy_ts
            if secs_discrepancy < CROSS_DISCREPANCY_KILL_S:
                _info(
                    f"[CROSS] Discrepancia Calvin={n_calvin} Poly=0 persistiendo "
                    f"{secs_discrepancy:.0f}s/{CROSS_DISCREPANCY_KILL_S}s — esperando"
                )
                return "ok", f"Grace period: discrepancia {secs_discrepancy:.0f}s/{CROSS_DISCREPANCY_KILL_S}s"

            # Discrepancia persistente superó el umbral → CRÍTICO
            _cross_discrepancy_ts = 0.0  # resetear para el próximo ciclo
            msg = (
                f"ALUCINACIÓN: Calvin5 reporta {n_calvin} posición(es) abierta(s) "
                f"pero Polymarket muestra 0 tokens en wallet "
                f"(discrepancia persistente {secs_discrepancy:.0f}s > {CROSS_DISCREPANCY_KILL_S}s)"
            )
            _err(msg)
            messages.append(msg)
            status = "critical"

        else:
            # Poly tiene posiciones o Calvin=0 — discrepancia resuelta, resetear contador
            if _cross_discrepancy_ts > 0.0:
                _info("[CROSS] Discrepancia resuelta — reseteando grace period")
            _cross_discrepancy_ts = 0.0

    # ② Polymarket tiene tokens que Calvin5 no está trackeando (tokens huérfanos)
    if poly_positions and not calvin_open:
        msg = (f"TOKENS HUÉRFANOS: Polymarket muestra {len(poly_positions)} posición(es) "
               f"pero Calvin5 no reporta ninguna — posibles pérdidas no controladas")
        _warn(msg)
        messages.append(msg)
        if status == "ok":
            status = "warning"

    return status, "; ".join(messages) if messages else "Realidad OK — Calvin5 alineado con Polymarket"


def _execute_kill_switch(reason: str) -> None:
    """
    Secuencia de apagado de emergencia (autoridad suprema del Watchdog):
      1. Envía CLOSE_ALL al Execution Bot → cierra posiciones abiertas limpiamente
      2. Envía PAUSE al Execution Bot → bloquea nuevas órdenes de Calvin5
      3. Envía alerta Telegram
      4. Como último recurso: mata el proceso de Calvin5

    El Execution Bot sigue vivo para ejecutar el CLOSE_ALL antes de que Calvin5 muera.
    """
    _err(f"KILL SWITCH ACTIVADO: {reason}")

    # Paso 1: pedir cierre limpio de posiciones al Execution Bot
    closed = _send_emergency_command(
        "CLOSE_ALL",
        reason=f"Watchdog kill switch: {reason}",
    )
    if closed:
        _info("[KILL SWITCH] CLOSE_ALL enviado al Execution Bot — esperando 5s para liquidación")
        time.sleep(5)

    # Paso 2: pausar el Execution Bot (deja de aceptar órdenes de Calvin5)
    _send_emergency_command("PAUSE", reason="Watchdog bloqueó nuevas órdenes tras kill switch")

    # Paso 3: notificar al operador
    msg = (
        f"Bot calvin5.py ha sido detenido forzosamente.\n\n"
        f"Razón: {reason}\n\n"
        f"• CLOSE_ALL enviado al Execution Bot: {'Sí' if closed else 'No (sin Redis)'}\n"
        f"• Execution Bot: PAUSADO (no acepta nuevas órdenes de Calvin5)\n\n"
        f"Acción requerida: Revisar logs en {LOG_FILE} y reiniciar manualmente "
        f"cuando sea seguro. Para reanudar: python watchdog.py --resume"
    )
    _send_telegram(msg, level="CRITICAL")

    # Paso 4: matar el proceso de Calvin5 (calculador)
    _kill_bot(reason)


_streams_available: bool = True  # Se pone a False si Redis < 5.0 (XADD no disponible)


def _send_watchdog_heartbeat_to_redis() -> None:
    """Publica heartbeat del watchdog a Redis Stream (persistente, robusto)."""
    global _streams_available
    if _redis_client is None or not _streams_available:
        return

    try:
        hb_data = {
            "timestamp":  str(time.time()),
            "uptime_s":   "0",
            "status":     "monitoring",
            "cpu_check":  "ok",
        }
        _redis_client.xadd(
            "heartbeats:watchdog",
            hb_data,
            maxlen=100,
            approximate=True
        )
        _info("[HB-STREAM] Heartbeat del watchdog publicado a Redis Stream")
    except Exception as exc:
        if "unknown command" in str(exc).lower():
            _streams_available = False
            _warn("[HB-STREAM] Redis Streams no disponibles (Redis < 5.0) — desactivado. Heartbeat continúa por pub/sub.")
        else:
            _warn(f"[HB-STREAM] Error publicando heartbeat: {exc}")


def _init_redis() -> None:
    """Conecta al broker Redis. Sin Redis el watchdog sigue funcionando en modo degradado."""
    global _redis_client
    try:
        _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=3)
        _redis_client.ping()
        _info(f"[REDIS] Conectado a {REDIS_URL}")
    except Exception as exc:
        _warn(f"[REDIS] No disponible ({exc}) — operando sin Redis (sin comandos de emergencia)")
        _redis_client = None


# ISSUE #4: Validación temprana de config (watchdog necesita Redis y Telegram)
def _validate_watchdog_config() -> None:
    """Verifica que la configuración es válida."""
    if not TG_TOKEN or not TG_CHAT_ID:
        _warn("⚠️  Telegram no configurado (TG_TOKEN / TG_CHAT_ID) — alertas por Telegram deshabilitadas")
    _info(f"✅ Configuración válida — operando en modo {'DRY-RUN' if DRY_RUN else 'REAL'}")


def run_watchdog() -> None:
    """Bucle principal del watchdog. Corre indefinidamente."""

    # Modo --resume: desbloquea el Execution Bot y sale
    if "--resume" in sys.argv:
        _init_redis()
        _send_emergency_command("RESUME", reason="Operador reanudó el sistema manualmente")
        _send_telegram("Sistema reanudado por el operador. Execution Bot desbloqueado.", level="INFO")
        _info("RESUME enviado. El sistema está desbloqueado.")
        return

    global _executor_paused_since
    _validate_watchdog_config()  # ISSUE #4: Verificación temprana
    _init_redis()

    _info("=" * 60)
    _info(f"Watchdog iniciado — supervisando {BOT_SCRIPT}")
    _info(f"  Heartbeat máx: {HEARTBEAT_MAX_S}s | Intervalo check: {CHECK_INTERVAL_S}s")
    _info(f"  Drawdown kill: ${DRAWDOWN_KILL} | Error thresh: {ERROR_THRESH} en {ERROR_WINDOW_S}s")
    _info(f"  Redis: {'OK' if _redis_client else 'No disponible'}")
    if DRY_RUN:
        _info("  MODO DRY-RUN: no se matará el proceso real")
    _info("=" * 60)

    consecutive_process_misses = 0

    try:
        while True:
            try:
                # ── Publicar heartbeat a Redis Stream (para que executor lo vea) ─────────────
                _send_watchdog_heartbeat_to_redis()

                _info(f"--- Ciclo de verificación {datetime.utcnow().strftime('%H:%M:%S')} UTC ---")

                # ── Verificar proceso ──────────────────────────────────────────
                process_ok = check_process()
                if not process_ok:
                    consecutive_process_misses += 1
                    if consecutive_process_misses >= 2:
                        reason = f"Proceso {BOT_SCRIPT} no encontrado en {consecutive_process_misses} verificaciones consecutivas"
                        if _can_alert("process_dead"):
                            _send_telegram(
                                f"El proceso {BOT_SCRIPT} no está corriendo.\n"
                                f"El watchdog no puede matarlo si ya está muerto.\n"
                                f"Verifica el sistema y reinicia manualmente.",
                                level="CRITICAL"
                            )
                    else:
                        _warn(f"Proceso no encontrado (intento {consecutive_process_misses}/2)")
                else:
                    consecutive_process_misses = 0

                # ── Heartbeat ──────────────────────────────────────────────────
                heartbeat_ok, elapsed = check_heartbeat()
                if not heartbeat_ok and process_ok:
                    reason = f"Bot sin actividad {elapsed:.0f}s (máx {HEARTBEAT_MAX_S}s) — posible cuelgue"
                    if _can_alert("heartbeat"):
                        _execute_kill_switch(reason)
                    continue  # siguiente ciclo tras el kill

                elif not heartbeat_ok and elapsed > HEARTBEAT_MAX_S * 0.75:
                    # Advertencia temprana antes de alcanzar el límite
                    warn_msg = f"Bot tardando: {elapsed:.0f}s sin log (umbral kill: {HEARTBEAT_MAX_S}s)"
                    _warn(warn_msg)
                    if _can_alert("heartbeat_warn"):
                        _send_telegram(warn_msg, level="WARNING")

                # ── Errores en log ────────────────────────────────────────────
                error_status, error_count = check_errors()
                if error_status == "critical":
                    last_errors = [line for _, _, line in list(_recent_errors)[-5:]]
                    reason = (
                        f"{error_count} errores críticos en los últimos {ERROR_WINDOW_S}s.\n"
                        f"Últimos errores:\n" + "\n".join(f"  • {e}" for e in last_errors)
                    )
                    if _can_alert("errors_critical"):
                        _execute_kill_switch(reason)
                    continue

                elif error_status == "warning":
                    if _can_alert("errors_warn"):
                        _send_telegram(
                            f"Errores detectados en calvin5.py: {error_count} en {ERROR_WINDOW_S}s.\n"
                            f"El bot sigue corriendo — monitorizando.",
                            level="WARNING"
                        )

                # ── Capital / Drawdown ────────────────────────────────────────
                capital_status, capital_msg = check_capital()
                if capital_status == "critical":
                    reason = f"Drawdown crítico detectado: {capital_msg}"
                    if _can_alert("capital_critical"):
                        _execute_kill_switch(reason)
                    continue

                elif capital_status == "warning":
                    if _can_alert("capital_warn"):
                        _send_telegram(capital_msg, level="WARNING")

                # ── C4: Límites globales semanales/mensuales ──────────────────
                try:
                    lt_status, lt_msg = loss_tracker.check()
                    if lt_status == loss_tracker.STATUS_BREACHED:
                        reason = f"Límite global de pérdidas superado: {lt_msg}"
                        if _can_alert("loss_limit_breached"):
                            _err(f"[LOSS_LIMIT] {reason}")
                            _execute_kill_switch(reason)
                        continue
                    elif lt_status == loss_tracker.STATUS_WARNING:
                        if _can_alert("loss_limit_warn"):
                            _warn(f"[LOSS_LIMIT] {lt_msg}")
                            _send_telegram(f"Aviso límite pérdidas: {lt_msg}", level="WARNING")
                except Exception as lt_exc:
                    _warn(f"[LOSS_LIMIT] Error verificando límites: {lt_exc}")

                # ── Verificación cruzada Calvin5 ↔ Polymarket ─────────────────
                # Cada ciclo (30s); con cooldown de alerta no genera spam
                if True:
                    cross_status, cross_msg = check_cross_reality()
                    if cross_status == "critical":
                        reason = f"Alucinación algorítmica detectada: {cross_msg}"
                        if _can_alert("cross_reality"):
                            # Órdenes de emergencia al Executor, luego kill Calvin5
                            _execute_kill_switch(reason)
                        continue
                    elif cross_status == "warning":
                        if _can_alert("cross_warn"):
                            _send_telegram(cross_msg, level="WARNING")

                # ── Verificar ExecutionBot: heartbeat + auto-resume ────────────
                # Lee state:executor:latest (TTL=90s). Si está pausado por razón
                # recuperable durante >AUTO_RESUME_AFTER_S → envía RESUME automático.
                if _redis_client is not None:
                    try:
                        exec_raw = _redis_client.get("state:executor:latest")
                        if exec_raw is None:
                            _warn("[EXECUTOR] Heartbeat ausente (key TTL expirada) — executor posiblemente colgado")
                            if _can_alert("executor_missing"):
                                _send_telegram(
                                    "ExecutionBot sin heartbeat >90s — key expirada.\n"
                                    "Revisar executor.log.",
                                    level="WARNING",
                                )
                            _executor_paused_since = 0.0
                        else:
                            exec_state    = json.loads(exec_raw)
                            exec_ts       = float(exec_state.get("timestamp", "0"))
                            exec_age_s    = time.time() - exec_ts
                            is_paused     = exec_state.get("paused", "false") == "true"
                            pause_reason  = exec_state.get("pause_reason", "").lower()

                            # ── Heartbeat viejo ──────────────────────────────
                            if exec_age_s > 90:
                                _err(f"[EXECUTOR] Heartbeat con {exec_age_s:.0f}s — posible cuelgue")
                                if _can_alert("executor_stuck"):
                                    _send_telegram(
                                        f"ExecutionBot sin heartbeat hace {exec_age_s:.0f}s.\n"
                                        "Revisar executor.log.",
                                        level="WARNING",
                                    )

                            # ── Auto-healer: executor pausado ─────────────────
                            elif is_paused:
                                now = time.time()

                                # Primera vez que lo vemos pausado → registrar timestamp
                                if _executor_paused_since == 0.0:
                                    _executor_paused_since = now
                                    _warn(f"[AUTO-HEAL] Executor PAUSADO — razón: {pause_reason[:80]}")

                                paused_s = now - _executor_paused_since

                                # Determinar si la razón es recuperable o crítica
                                is_critical = any(p in pause_reason for p in _CRITICAL_PAUSE_PATTERNS)
                                is_recoverable = any(p in pause_reason for p in _RECOVERABLE_PAUSE_PATTERNS)

                                if is_critical:
                                    # No auto-resumir — razón grave, alertar y dejar al operador
                                    if _can_alert("executor_critical_pause"):
                                        _err(f"[AUTO-HEAL] Executor pausado por razón CRITICA: {pause_reason[:80]}")
                                        _send_telegram(
                                            f"ExecutionBot PAUSADO por razón critica:\n{pause_reason[:120]}\n\n"
                                            "NO se hara auto-resume. Revisa y ejecuta:\n"
                                            "python watchdog.py --resume",
                                            level="CRITICAL",
                                        )

                                elif paused_s >= AUTO_RESUME_AFTER_S:
                                    # Pausado por razón recuperable y ya pasó el tiempo → auto-resume
                                    _err(f"[AUTO-HEAL] Executor pausado {paused_s:.0f}s por razón recuperable — enviando RESUME automático")
                                    resumed = _send_emergency_command(
                                        "RESUME",
                                        reason=f"Auto-resume watchdog: {pause_reason[:80]} ({paused_s:.0f}s pausado)",
                                    )
                                    if resumed:
                                        _executor_paused_since = 0.0
                                        _send_telegram(
                                            f"Auto-resume aplicado al ExecutionBot.\n"
                                            f"Estaba pausado {paused_s:.0f}s por: {pause_reason[:80]}",
                                            level="INFO",
                                        )
                                else:
                                    secs_left = AUTO_RESUME_AFTER_S - paused_s
                                    _info(f"[AUTO-HEAL] Executor pausado {paused_s:.0f}s — auto-resume en {secs_left:.0f}s si sigue pausado")

                            else:
                                # Executor activo y no pausado
                                if _executor_paused_since > 0.0:
                                    _info("[AUTO-HEAL] Executor ya no está pausado — reset contador")
                                _executor_paused_since = 0.0
                                _info(f"[EXECUTOR] OK — heartbeat hace {exec_age_s:.0f}s | paused=False")

                    except Exception as exc:
                        _warn(f"[EXECUTOR] Error verificando heartbeat: {exc}")

                _info(f"Todo OK — próxima verificación en {CHECK_INTERVAL_S}s")

            except KeyboardInterrupt:
                _info("Watchdog detenido por el usuario (Ctrl+C)")
                break
            except Exception as exc:
                _err(f"Error interno del watchdog: {exc}")

            time.sleep(CHECK_INTERVAL_S)
    finally:
        # Cleanup al terminar
        if _redis_client is not None:
            try:
                _redis_client.close()  # ISSUE #3: Cierre Redis para liberar conexión
                _info("Redis cerrado.")
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from utils import configure_structlog
    configure_structlog("watchdog", log_file="watchdog.log")
    # Inicializar posición en el log (leer desde el final, no desde el inicio)
    log_path = Path(LOG_FILE)
    if log_path.exists():
        _last_log_pos = log_path.stat().st_size
        _info(f"Iniciando lectura de {LOG_FILE} desde posición {_last_log_pos}")

    run_watchdog()
