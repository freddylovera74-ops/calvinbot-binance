"""
loss_tracker.py — Límites de pérdida globales semanales y mensuales para CalvinBot.

El SESSION_DRAWDOWN_LIMIT de calvin5 se resetea diariamente, lo que permite
perder el mismo importe cada día indefinidamente. Este módulo añade contadores
acumulativos que NO se resetean hasta fin de semana/mes.

Cuando se supera un límite → el bot se detiene completamente hasta intervención manual.

Variables de entorno:
    WEEKLY_LOSS_LIMIT   — Pérdida máxima acumulada en 7 días (default: $150)
    MONTHLY_LOSS_LIMIT  — Pérdida máxima acumulada en el mes calendario (default: $400)

Archivo de persistencia: calvin5_loss_limits.json
    Formato:
    {
        "weekly":  {"week_key": "2026-W13", "cumulative_pnl": -45.20},
        "monthly": {"month_key": "2026-03",  "cumulative_pnl": -112.80}
    }

Integración:
    · calvin5.py: llamar loss_tracker.record_pnl(pnl) tras cada cierre
    · watchdog.py: llamar loss_tracker.check() en cada ciclo de monitorización
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

WEEKLY_LOSS_LIMIT  = float(os.getenv("WEEKLY_LOSS_LIMIT",  "150.0"))
MONTHLY_LOSS_LIMIT = float(os.getenv("MONTHLY_LOSS_LIMIT", "400.0"))
LOSS_FILE          = "binance_loss_limits.json"

# Estados posibles
STATUS_OK       = "ok"
STATUS_WARNING  = "warning"   # >75% del límite consumido
STATUS_BREACHED = "breached"  # límite superado → detener bot


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS DE FECHA
# ─────────────────────────────────────────────────────────────────────────────

def _week_key() -> str:
    """Clave ISO de semana actual, ej. '2026-W13'."""
    now = datetime.now(timezone.utc)
    return f"{now.isocalendar()[0]}-W{now.isocalendar()[1]:02d}"


def _month_key() -> str:
    """Clave de mes actual, ej. '2026-03'."""
    now = datetime.now(timezone.utc)
    return f"{now.year}-{now.month:02d}"


# ─────────────────────────────────────────────────────────────────────────────
#  PERSISTENCIA
# ─────────────────────────────────────────────────────────────────────────────

def _load() -> dict:
    """Carga el estado desde disco. Devuelve estructura vacía si no existe."""
    path = Path(LOSS_FILE)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def _save(data: dict) -> None:
    """Escribe el estado a disco de forma atómica."""
    import tempfile
    path = Path(LOSS_FILE)
    try:
        with tempfile.NamedTemporaryFile(
            "w", dir=path.parent, delete=False, suffix=".tmp", encoding="utf-8"
        ) as tf:
            json.dump(data, tf, indent=2)
            tmp = tf.name
        os.replace(tmp, path)
    except Exception:
        pass  # Si falla la persistencia no bloquear el bot


# ─────────────────────────────────────────────────────────────────────────────
#  API PÚBLICA
# ─────────────────────────────────────────────────────────────────────────────

def record_pnl(pnl: float) -> None:
    """
    Registra el PnL de un trade cerrado en los acumuladores semanales y mensuales.
    Llamar después de cada close_position() en calvin5.py.
    Solo acumula pérdidas negativas hacia los límites (las ganancias reducen el contador).
    """
    data = _load()
    wk   = _week_key()
    mo   = _month_key()

    # Inicializar o resetear si estamos en una nueva semana/mes
    if data.get("weekly", {}).get("week_key") != wk:
        data["weekly"] = {"week_key": wk, "cumulative_pnl": 0.0, "updated_at": time.time()}
    if data.get("monthly", {}).get("month_key") != mo:
        data["monthly"] = {"month_key": mo, "cumulative_pnl": 0.0, "updated_at": time.time()}

    data["weekly"]["cumulative_pnl"]  = round(data["weekly"]["cumulative_pnl"]  + pnl, 6)
    data["monthly"]["cumulative_pnl"] = round(data["monthly"]["cumulative_pnl"] + pnl, 6)
    data["weekly"]["updated_at"]      = time.time()
    data["monthly"]["updated_at"]     = time.time()

    _save(data)


def check() -> Tuple[str, str]:
    """
    Verifica si se han superado los límites semanales o mensuales.

    Devuelve (status, mensaje):
        ("ok",       "Límites OK — semana $-45.20 / mes $-112.80")
        ("warning",  "Semana: $-120.50 / $150 (80%)")
        ("breached", "LÍMITE SEMANAL SUPERADO: $-162.30 > $150 — BOT DEBE DETENERSE")
    """
    data = _load()
    wk   = _week_key()
    mo   = _month_key()

    weekly_pnl  = data.get("weekly",  {}).get("cumulative_pnl", 0.0) \
                  if data.get("weekly",  {}).get("week_key")  == wk else 0.0
    monthly_pnl = data.get("monthly", {}).get("cumulative_pnl", 0.0) \
                  if data.get("monthly", {}).get("month_key") == mo else 0.0

    weekly_loss  = abs(min(weekly_pnl,  0.0))
    monthly_loss = abs(min(monthly_pnl, 0.0))

    # ── Verificar límites ─────────────────────────────────────────────────────
    if weekly_loss >= WEEKLY_LOSS_LIMIT:
        msg = (
            f"LÍMITE SEMANAL SUPERADO: ${weekly_loss:.2f} >= ${WEEKLY_LOSS_LIMIT:.0f} "
            f"(semana {wk}) — DETENER BOT HASTA PRÓXIMA SEMANA"
        )
        return STATUS_BREACHED, msg

    if monthly_loss >= MONTHLY_LOSS_LIMIT:
        msg = (
            f"LÍMITE MENSUAL SUPERADO: ${monthly_loss:.2f} >= ${MONTHLY_LOSS_LIMIT:.0f} "
            f"(mes {mo}) — DETENER BOT HASTA PRÓXIMO MES"
        )
        return STATUS_BREACHED, msg

    # ── Advertencia al 75% ────────────────────────────────────────────────────
    weekly_pct  = weekly_loss  / WEEKLY_LOSS_LIMIT  * 100
    monthly_pct = monthly_loss / MONTHLY_LOSS_LIMIT * 100

    warnings = []
    if weekly_pct >= 75:
        warnings.append(
            f"Semana: ${weekly_loss:.2f}/${WEEKLY_LOSS_LIMIT:.0f} ({weekly_pct:.0f}%)"
        )
    if monthly_pct >= 75:
        warnings.append(
            f"Mes: ${monthly_loss:.2f}/${MONTHLY_LOSS_LIMIT:.0f} ({monthly_pct:.0f}%)"
        )

    if warnings:
        return STATUS_WARNING, "Límites de pérdida al 75%+ — " + " | ".join(warnings)

    return (
        STATUS_OK,
        f"Límites OK — semana ${weekly_pnl:+.2f} ({weekly_pct:.0f}%) "
        f"| mes ${monthly_pnl:+.2f} ({monthly_pct:.0f}%)"
    )


def get_summary() -> dict:
    """Devuelve resumen legible del estado actual para alertas Telegram."""
    data = _load()
    wk   = _week_key()
    mo   = _month_key()

    weekly_pnl  = data.get("weekly",  {}).get("cumulative_pnl", 0.0) \
                  if data.get("weekly",  {}).get("week_key")  == wk else 0.0
    monthly_pnl = data.get("monthly", {}).get("cumulative_pnl", 0.0) \
                  if data.get("monthly", {}).get("month_key") == mo else 0.0

    return {
        "week_key":          wk,
        "month_key":         mo,
        "weekly_pnl":        weekly_pnl,
        "monthly_pnl":       monthly_pnl,
        "weekly_limit":      WEEKLY_LOSS_LIMIT,
        "monthly_limit":     MONTHLY_LOSS_LIMIT,
        "weekly_pct_used":   abs(min(weekly_pnl, 0.0))  / WEEKLY_LOSS_LIMIT  * 100,
        "monthly_pct_used":  abs(min(monthly_pnl, 0.0)) / MONTHLY_LOSS_LIMIT * 100,
    }


def reset_weekly() -> None:
    """Reset manual del contador semanal (para uso desde CLI o dashboard)."""
    data = _load()
    data["weekly"] = {"week_key": _week_key(), "cumulative_pnl": 0.0, "updated_at": time.time()}
    _save(data)
    print(f"[LOSS_TRACKER] Contador semanal reseteado ({_week_key()})")


def reset_monthly() -> None:
    """Reset manual del contador mensual (para uso desde CLI o dashboard)."""
    data = _load()
    data["monthly"] = {"month_key": _month_key(), "cumulative_pnl": 0.0, "updated_at": time.time()}
    _save(data)
    print(f"[LOSS_TRACKER] Contador mensual reseteado ({_month_key()})")


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "reset-weekly":
            reset_weekly()
        elif cmd == "reset-monthly":
            reset_monthly()
        elif cmd == "status":
            status, msg = check()
            summary = get_summary()
            print(f"Estado: {status.upper()}")
            print(f"Mensaje: {msg}")
            print(f"\nSemana {summary['week_key']}:  ${summary['weekly_pnl']:+.2f} "
                  f"({summary['weekly_pct_used']:.0f}% de ${summary['weekly_limit']:.0f})")
            print(f"Mes {summary['month_key']}:     ${summary['monthly_pnl']:+.2f} "
                  f"({summary['monthly_pct_used']:.0f}% de ${summary['monthly_limit']:.0f})")
        else:
            print(f"Comandos: status | reset-weekly | reset-monthly")
    else:
        status, msg = check()
        print(f"{status.upper()}: {msg}")
