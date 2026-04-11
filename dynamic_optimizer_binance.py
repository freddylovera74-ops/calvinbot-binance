"""
dynamic_optimizer_binance.py — Auto-regulador de parámetros para CalvinBot Binance

Lee binance_trades.csv cada OPTIMIZE_INTERVAL_MIN minutos y ajusta los parámetros
de strategy_binance.py de forma adaptativa según el rendimiento reciente.

Lógica de auto-regulación:
  - Si va bien (win_rate > 60%, PF > 1.3): aumenta agresividad 5%
  - Si va mal  (win_rate < 45%, PF < 0.8): reduce agresividad 5%
  - Dentro de bounds de seguridad siempre

Los parámetros ajustados se publican en Redis key 'config:strategy_binance:current'
y la strategy los lee en cada ciclo.
"""

import asyncio
import csv
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

from utils import send_telegram_async

# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
REDIS_URL  = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_KEY  = "config:strategy_binance:current"

OPTIMIZE_INTERVAL_MIN = int(os.getenv("OPTIMIZE_INTERVAL_MIN", "30"))
MIN_TRADES_REQUIRED   = int(os.getenv("MIN_TRADES_REQUIRED",   "15"))
SMOOTH_FACTOR         = float(os.getenv("SMOOTH_FACTOR",       "0.30"))

TRADES_CSV = BASE_DIR / "binance_trades.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [OPT] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(BASE_DIR / "optimizer_binance.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("optimizer")

# ─────────────────────────────────────────────────────────────────────────────
#  BOUNDS DE SEGURIDAD (mínimo y máximo permitido por parámetro)
# ─────────────────────────────────────────────────────────────────────────────

BOUNDS = {
    "STAKE_USD":     (20.0,  200.0),
    "BTC_MIN_PCT":   (0.05,  0.30),   # % mínimo momentum
    "BTC_WINDOW_S":  (15,    60),      # ventana en segundos
    "TP_PCT":        (0.010, 0.040),   # % take profit
    "SL_DROP_PCT":   (0.005, 0.020),   # % stop loss
    "MAX_OPEN_POS":  (1,     3),       # posiciones simultáneas
    "THROTTLE_S":    (10,    60),      # segundos entre entradas
    "MAX_HOLD_S":    (120,   600),     # segundos máximo en posición
    "TP_PARTIAL_PCT":(0.005, 0.020),   # % TP parcial
    "DAILY_LOSS_LIMIT": (30.0, 300.0),
}

# Parámetros iniciales 7.5/10 de riesgo (se pisan con lo que hay en Redis si ya existe)
INITIAL_PARAMS = {
    "STAKE_USD":       75.0,
    "BTC_MIN_PCT":     0.08,
    "BTC_WINDOW_S":    20,
    "TP_PCT":          0.020,
    "TP_PARTIAL_PCT":  0.010,
    "SL_DROP_PCT":     0.008,
    "MAX_OPEN_POS":    2,
    "THROTTLE_S":      15,
    "MAX_HOLD_S":      240,
    "DAILY_LOSS_LIMIT": 100.0,
}

# ─────────────────────────────────────────────────────────────────────────────
#  LECTURA DE TRADES
# ─────────────────────────────────────────────────────────────────────────────

def read_trades(last_n: int = 0) -> List[dict]:
    """Lee binance_trades.csv. Si last_n > 0, solo devuelve los últimos N."""
    if not TRADES_CSV.exists():
        return []
    try:
        trades = []
        with open(TRADES_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("event", "CLOSE") != "CLOSE":
                    continue
                try:
                    pnl = float(row.get("pnl_usd", 0) or 0)
                    ts_raw = row.get("timestamp", "") or ""
                    try:
                        ts = datetime.fromisoformat(ts_raw).timestamp()
                    except Exception:
                        ts = 0.0
                    trades.append({
                        "pnl":    pnl,
                        "ts":     ts,
                        "reason": row.get("reason", ""),
                    })
                except (ValueError, KeyError):
                    pass
        trades.sort(key=lambda t: t["ts"])
        if last_n > 0:
            return trades[-last_n:]
        return trades
    except Exception as ex:
        log.error(f"Error leyendo CSV: {ex}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  MÉTRICAS
# ─────────────────────────────────────────────────────────────────────────────

def compute_metrics(trades: List[dict]) -> dict:
    if not trades:
        return {}

    pnls     = [t["pnl"] for t in trades]
    wins     = [p for p in pnls if p > 0]
    losses   = [p for p in pnls if p <= 0]
    total    = len(pnls)
    win_rate = len(wins) / total * 100 if total else 0

    gross_profit = sum(wins)   if wins   else 0
    gross_loss   = abs(sum(losses)) if losses else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (9.9 if gross_profit > 0 else 1.0)

    # Max drawdown desde pico acumulado
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnls:
        cum += p
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    total_pnl = sum(pnls)
    avg_win   = sum(wins) / len(wins)     if wins   else 0
    avg_loss  = sum(losses) / len(losses) if losses else 0

    return {
        "total":         total,
        "win_rate":      round(win_rate, 1),
        "profit_factor": round(profit_factor, 3),
        "total_pnl":     round(total_pnl, 2),
        "max_drawdown":  round(max_dd, 2),
        "avg_win":       round(avg_win, 2),
        "avg_loss":      round(avg_loss, 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  MOTOR DE AJUSTE
# ─────────────────────────────────────────────────────────────────────────────

def compute_score(metrics: dict) -> float:
    """
    Score compuesto: 0.0 (muy mal) → 2.0 (excelente). Neutro = 1.0.
    - win_rate 50% → 1.0 | 70% → 1.4 | 30% → 0.6
    - profit_factor 1.0 → 1.0 | 2.0 → 1.3 | 0.5 → 0.75
    """
    wr = metrics.get("win_rate", 50) / 50.0
    pf = min(metrics.get("profit_factor", 1.0), 3.0) / 1.0
    pf_norm = 0.75 + (pf - 0.5) * 0.3  # normalizado
    return round((wr * 0.6 + pf_norm * 0.4), 3)


def _clip(val, lo, hi):
    return max(lo, min(hi, val))


def _smooth(new_val, old_val, factor=SMOOTH_FACTOR):
    """Suavizado: 30% nuevo + 70% viejo para evitar cambios bruscos."""
    return old_val + factor * (new_val - old_val)


def adjust_params(current: dict, score: float) -> Tuple[dict, str]:
    """
    Ajusta parámetros según el score de rendimiento.
    score > 1.15 → +5% agresividad
    score < 0.85 → -5% agresividad
    """
    params = dict(current)
    reason = "hold"

    if score > 1.15:
        # Ir más agresivo: bajar umbral entrada, subir stake
        factor = 1.05
        reason = f"subiendo agresividad (score={score})"
        params["STAKE_USD"]    = _smooth(params["STAKE_USD"] * factor, params["STAKE_USD"])
        params["BTC_MIN_PCT"]  = _smooth(params["BTC_MIN_PCT"] / factor, params["BTC_MIN_PCT"])
        params["BTC_WINDOW_S"] = _smooth(params["BTC_WINDOW_S"] / factor, params["BTC_WINDOW_S"])
        params["THROTTLE_S"]   = _smooth(params["THROTTLE_S"] / factor, params["THROTTLE_S"])
        params["TP_PCT"]       = _smooth(params["TP_PCT"] * factor, params["TP_PCT"])

    elif score < 0.85:
        # Ir más conservador: subir umbral entrada, bajar stake
        factor = 1.05
        reason = f"bajando agresividad (score={score})"
        params["STAKE_USD"]    = _smooth(params["STAKE_USD"] / factor, params["STAKE_USD"])
        params["BTC_MIN_PCT"]  = _smooth(params["BTC_MIN_PCT"] * factor, params["BTC_MIN_PCT"])
        params["BTC_WINDOW_S"] = _smooth(params["BTC_WINDOW_S"] * factor, params["BTC_WINDOW_S"])
        params["THROTTLE_S"]   = _smooth(params["THROTTLE_S"] * factor, params["THROTTLE_S"])
        params["SL_DROP_PCT"]  = _smooth(params["SL_DROP_PCT"] * factor, params["SL_DROP_PCT"])

    # Aplicar bounds
    for key, (lo, hi) in BOUNDS.items():
        if key in params:
            params[key] = _clip(params[key], lo, hi)

    # Redondear enteros
    for key in ("BTC_WINDOW_S", "MAX_OPEN_POS", "THROTTLE_S", "MAX_HOLD_S"):
        if key in params:
            params[key] = int(round(params[key]))

    # Redondear floats a 4 decimales
    for key in params:
        if isinstance(params[key], float):
            params[key] = round(params[key], 4)

    return params, reason


# ─────────────────────────────────────────────────────────────────────────────
#  REDIS
# ─────────────────────────────────────────────────────────────────────────────

async def load_current_params(r: aioredis.Redis) -> dict:
    """Carga parámetros actuales desde Redis. Si no existen, usa INITIAL_PARAMS."""
    try:
        raw = await r.get(REDIS_KEY)
        if raw:
            data = json.loads(raw)
            return data.get("params", INITIAL_PARAMS)
    except Exception as ex:
        log.warning(f"No se pudo leer params de Redis: {ex}")
    return dict(INITIAL_PARAMS)


async def save_params(r: aioredis.Redis, params: dict, metrics: dict, reason: str):
    payload = {
        "params":    params,
        "metrics":   metrics,
        "reason":    reason,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    "dynamic_optimizer_binance",
    }
    await r.set(REDIS_KEY, json.dumps(payload))
    log.info(f"Params publicados → {reason}")
    log.info(f"  STAKE={params['STAKE_USD']} BTC_MIN={params['BTC_MIN_PCT']}% "
             f"TP={params['TP_PCT']*100:.2f}% SL={params['SL_DROP_PCT']*100:.2f}%")


# ─────────────────────────────────────────────────────────────────────────────
#  LOOP PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

async def optimize_cycle(r: aioredis.Redis, cycle: int):
    """Ejecuta un ciclo de optimización."""
    trades = read_trades()

    if len(trades) < MIN_TRADES_REQUIRED:
        log.info(f"Solo {len(trades)} trades — mínimo {MIN_TRADES_REQUIRED}. Manteniendo params.")

        # Primera vez: publicar params iniciales si no hay nada en Redis
        existing = await r.get(REDIS_KEY)
        if not existing:
            await save_params(r, dict(INITIAL_PARAMS), {}, "params_iniciales_7.5")
            await send_telegram_async(
                f"Optimizer Binance iniciado\n"
                f"Params iniciales riesgo 7.5/10 publicados\n"
                f"Esperando {MIN_TRADES_REQUIRED} trades para auto-regular.",
                level="INFO", prefix_label="Optimizer"
            )
        return

    # Métricas sobre últimos 50 trades (recientes)
    recent   = trades[-50:]
    metrics  = compute_metrics(recent)
    score    = compute_score(metrics)

    current  = await load_current_params(r)
    new_params, reason = adjust_params(current, score)

    changed = any(
        abs(new_params.get(k, 0) - current.get(k, 0)) > 0.0001
        for k in new_params
    )

    await save_params(r, new_params, metrics, reason)

    log.info(
        f"Ciclo {cycle} | trades={len(recent)} win_rate={metrics['win_rate']}% "
        f"PF={metrics['profit_factor']} score={score} | {reason}"
    )

    # Telegram si hay cambio significativo
    if changed and cycle > 1:
        await send_telegram_async(
            f"Optimizer Binance ajustó parámetros\n"
            f"Win rate: {metrics['win_rate']}% | PF: {metrics['profit_factor']}\n"
            f"Score: {score} → {reason}\n"
            f"STAKE: ${new_params['STAKE_USD']} | BTC_MIN: {new_params['BTC_MIN_PCT']}%\n"
            f"TP: {new_params['TP_PCT']*100:.2f}% | SL: {new_params['SL_DROP_PCT']*100:.2f}%",
            level="INFO", prefix_label="Optimizer"
        )


async def main():
    log.info("=" * 55)
    log.info("  Dynamic Optimizer Binance — iniciando")
    log.info(f"  Intervalo: {OPTIMIZE_INTERVAL_MIN} min | Min trades: {MIN_TRADES_REQUIRED}")
    log.info("=" * 55)

    r = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await r.ping()
        log.info(f"Redis conectado: {REDIS_URL}")
    except Exception as ex:
        log.error(f"Redis no disponible: {ex}")
        sys.exit(1)

    cycle = 0
    while True:
        cycle += 1
        try:
            await optimize_cycle(r, cycle)
        except Exception as ex:
            log.error(f"Error en ciclo {cycle}: {ex}")
        await asyncio.sleep(OPTIMIZE_INTERVAL_MIN * 60)


if __name__ == "__main__":
    asyncio.run(main())
