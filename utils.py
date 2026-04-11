"""
utils.py — Utilidades compartidas por todos los bots de CalvinBot.

Incluye:
  - madrid_now / in_trading_hours / madrid_today_str
  - write_json_atomic
  - send_telegram_sync / send_telegram_async
  - configure_structlog — logging JSON estructurado opcional

Elimina duplicación de _madrid_now(), _in_trading_hours() y helpers de
Telegram presentes en calvin5, execution_bot, watchdog, dynamic_optimizer
y dashboard_server.

⚠️  ISSUE #9: Biblioteca PURA sin importaciones circulares
    NO IMPORTAR: execution_bot, calvin5, watchdog, dynamic_optimizer
    Usar solo en módulos especializados para evitar cyclic imports.

Uso:
    from utils import madrid_now, in_trading_hours, send_telegram_sync, write_json_atomic
"""

import json
import os
import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

# ── Zona horaria Madrid ────────────────────────────────────────────────────────

def madrid_now() -> datetime:
    """Hora actual en Madrid (Europe/Madrid, maneja CET/CEST automáticamente)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Madrid"))
    except Exception:
        try:
            import pytz
            return datetime.now(pytz.timezone("Europe/Madrid"))
        except Exception:
            # Fallback manual: calcula DST sin dependencias externas
            utc = datetime.now(timezone.utc)
            y = utc.year
            last_sun_mar = 31 - (datetime(y, 4, 1, tzinfo=timezone.utc).weekday() + 1) % 7
            last_sun_oct = 31 - (datetime(y, 11, 1, tzinfo=timezone.utc).weekday() + 1) % 7
            dst_start = datetime(y, 3, last_sun_mar, 1, tzinfo=timezone.utc)
            dst_end   = datetime(y, 10, last_sun_oct, 1, tzinfo=timezone.utc)
            offset    = 2 if dst_start <= utc < dst_end else 1
            return (utc + timedelta(hours=offset)).replace(tzinfo=None)


def in_trading_hours(hour_start: int = 10, hour_end: int = 22) -> bool:
    """
    True si la hora actual de Madrid está dentro del horario operativo.
    Soporta rangos nocturnos que cruzan medianoche (ej. start=23, end=20):
      - start < end  → rango normal:   start <= hora < end
      - start >= end → rango nocturno: hora >= start OR hora < end
    """
    h = madrid_now().hour
    if hour_start < hour_end:
        return hour_start <= h < hour_end
    else:
        # Cruza medianoche: ej. 23→20 = activo de 23:00 a 19:59
        return h >= hour_start or h < hour_end


def madrid_today_str() -> str:
    """Fecha de hoy en Madrid, formato YYYY-MM-DD."""
    return madrid_now().strftime("%Y-%m-%d")


# ── I/O atómica ───────────────────────────────────────────────────────────────

def write_json_atomic(path: str | Path, data: Any) -> None:
    """
    Escribe `data` como JSON en `path` de forma atómica:
    write → tmp → os.replace (atómico en NTFS/ext4).
    Garantiza que el fichero destino nunca queda en estado parcial/corrupto
    si el proceso muere durante la escritura.
    """
    p = Path(path)
    dir_ = p.parent
    tmp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=dir_, delete=False, suffix=".tmp", encoding="utf-8"
        ) as f:
            json.dump(data, f, ensure_ascii=False)
            tmp_name = f.name
        os.replace(tmp_name, p)
    except Exception:
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except Exception:
                pass
        raise


# ── Telegram (síncrono) ───────────────────────────────────────────────────────

def send_telegram_sync(
    msg: str,
    level: str = "INFO",
    prefix_label: str = "CalvinBot",
    token: str | None = None,
    chat_id: str | None = None,
) -> bool:
    """
    Envía un mensaje a Telegram de forma síncrona (usando requests).
    Devuelve True si el envío tuvo éxito.

    Args:
        msg:          Texto del mensaje.
        level:        'INFO' | 'WARNING' | 'CRITICAL'
        prefix_label: Identificador del bot que aparece en el prefijo.
        token:        TG_TOKEN (si None, lee de env TG_TOKEN).
        chat_id:      TG_CHAT_ID (si None, lee de env TG_CHAT_ID).
    """
    import requests  # import local para no forzar dependencia si no se usa

    _token   = token   or os.getenv("TG_TOKEN", "")
    _chat_id = chat_id or os.getenv("TG_CHAT_ID", "")
    if not _token or not _chat_id:
        return False

    icons = {"WARNING": "⚠️", "CRITICAL": "🚨", "INFO": "ℹ️"}
    icon = icons.get(level, "ℹ️")
    full_msg = (
        f"{icon} [{prefix_label}] {level}\n\n"
        f"{msg}\n\n"
        f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )
    try:
        url  = f"https://api.telegram.org/bot{_token}/sendMessage"
        resp = requests.post(
            url,
            json={"chat_id": _chat_id, "text": full_msg},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


async def send_telegram_async(
    msg: str,
    level: str = "INFO",
    prefix_label: str = "CalvinBot",
    token: str | None = None,
    chat_id: str | None = None,
) -> bool:
    """
    Versión async de send_telegram_sync (usa aiohttp).
    Pensada para uso dentro de corrutinas asyncio (calvin5, execution_bot, dashboard).
    """
    import aiohttp  # import local

    _token   = token   or os.getenv("TG_TOKEN", "")
    _chat_id = chat_id or os.getenv("TG_CHAT_ID", "")
    if not _token or not _chat_id:
        return False

    icons = {"WARNING": "⚠️", "CRITICAL": "🚨", "INFO": "ℹ️"}
    icon = icons.get(level, "ℹ️")
    full_msg = (
        f"{icon} [{prefix_label}] {level}\n\n"
        f"{msg}\n\n"
        f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )
    try:
        url = f"https://api.telegram.org/bot{_token}/sendMessage"
        async with aiohttp.ClientSession() as s:
            resp = await s.post(
                url,
                json={"chat_id": _chat_id, "text": full_msg},
                timeout=aiohttp.ClientTimeout(total=8),
            )
            return resp.status == 200
    except Exception:
        return False


# ── Structured logging (structlog + stdlib fallback) ──────────────────────────

def configure_structlog(
    bot_name: str,
    log_file: str | None = None,
    json_logs: bool | None = None,
) -> None:
    """
    Configura structlog para emitir logs JSON estructurados.
    Si structlog no está instalado, es un no-op (logging estándar sigue funcionando).

    Args:
        bot_name:  Nombre del bot que aparecerá en cada línea de log.
        log_file:  Ruta del fichero de log (si None, solo stdout).
        json_logs: True = JSON, False = texto legible, None = auto (JSON si STRUCTLOG_JSON=1).
    """
    try:
        import structlog
        import logging

        use_json = json_logs if json_logs is not None else (
            os.getenv("STRUCTLOG_JSON", "0") == "1"
        )

        # Configura stdlib logging para que structlog lo use como backend
        handlers: list = [logging.StreamHandler()]
        if log_file:
            handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
        logging.basicConfig(
            format="%(message)s",
            level=logging.DEBUG,
            handlers=handlers,
            force=True,
        )

        shared_processors = [
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
        ]

        if use_json:
            renderer: Any = structlog.processors.JSONRenderer()
        else:
            renderer = structlog.dev.ConsoleRenderer(colors=False)

        structlog.configure(
            processors=shared_processors + [renderer],
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
            cache_logger_on_first_use=True,
        )

        # Bind bot_name a todos los futuros log events
        structlog.contextvars.bind_contextvars(bot=bot_name)

    except ImportError:
        pass  # structlog no instalado — logging estándar sigue activo
