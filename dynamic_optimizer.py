"""
dynamic_optimizer.py — Robot de Ajuste Dinámico para calvin5.py

Fase 1 — Variables optimizables detectadas en calvin5.py:
┌─────────────────────┬──────────┬────────┬────────┬──────────────────────────────────────────────┐
│ Variable            │ Actual   │ Min    │ Max    │ Descripción                                  │
├─────────────────────┼──────────┼────────┼────────┼──────────────────────────────────────────────┤
│ ENTRY_MIN           │ 0.62     │ 0.50   │ 0.75   │ Precio mínimo de entrada al token            │
│ ENTRY_MAX           │ 0.90     │ 0.78   │ 0.95   │ Precio máximo de entrada al token            │
│ ENTRY_CUTOFF_S      │ 40       │ 15     │ 90     │ Segundos finales de ronda sin entradas        │
│ TP_FULL_PCT         │ 0.25     │ 0.12   │ 0.40   │ Take Profit completo (% desde entrada)       │
│ TP_MID_PCT          │ 0.09     │ 0.04   │ 0.18   │ Take Profit parcial (% desde entrada)        │
│ TP_LAST_15S_PCT     │ 0.04     │ 0.02   │ 0.08   │ TP rápido en últimos 15s                     │
│ SL_DROP             │ 0.07     │ 0.03   │ 0.15   │ Stop Loss en caída absoluta (cents)          │
│ SL_FLOOR            │ 0.45     │ 0.30   │ 0.60   │ Floor absoluto de precio — BTC revirtió      │
│ SL_TIME_S           │ 230      │ 120    │ 270    │ SL temporal si holding >= N segundos         │
│ SL_LAST_30S_PCT     │ 0.05     │ 0.02   │ 0.10   │ SL ajustado en últimos 30s (% relativo)      │
│ BTC_WINDOW_S        │ 10       │ 5      │ 30     │ Ventana de momentum BTC en segundos          │
│ BTC_MIN_PCT         │ 0.10     │ 0.05   │ 0.35   │ % mínimo de movimiento BTC para entrar       │
│ HOLD_BTC_PCT        │ 0.15     │ 0.08   │ 0.30   │ % BTC para hold a resolución                 │
│ SLIP_MAX_PCT        │ 0.04     │ 0.01   │ 0.07   │ Slippage máximo tolerado entre señal y fill  │
│ FOK_PRICE_BUF       │ 0.03     │ 0.01   │ 0.06   │ Buffer de precio FOK sobre señal             │
│ PRICE_STABILITY_S   │ 2        │ 1      │ 5      │ Scans de estabilidad de precio antes entrar  │
│ STAKE_USD           │ 5.0      │ 3.0    │ 20.0   │ Dólares por operación                        │
│ THROTTLE_S          │ 2        │ 1      │ 10     │ Segundos mínimos entre entradas              │
└─────────────────────┴──────────┴────────┴────────┴──────────────────────────────────────────────┘

Fase 2 — Robot de Ajuste Dinámico:
  - Lee trades de calvin5_trades.csv como fuente de rendimiento reciente
  - Usa Optuna (optimización Bayesiana) para recalcular parámetros ideales
  - Aplica suavizado del 30% para evitar cambios bruscos
  - Respeta límites duros (hard bounds) en todos los parámetros
  - Publica configuración optimizada en Redis → canal config:update:calculation_bot
  - Trigger: cada OPTIMIZE_INTERVAL_H horas O si drawdown supera DRAWDOWN_TRIGGER_PCT

Dependencias extra:
  pip install optuna redis

Variables de entorno nuevas:
  REDIS_URL             — URL de Redis (default: redis://localhost:6379)
  OPTIMIZE_INTERVAL_H   — Horas entre optimizaciones (default: 1 h = optimizado para 5min)
  DRAWDOWN_TRIGGER_PCT  — % de drawdown para trigger urgente (default: 0.10 = 10% = más agresivo)
  OPTUNA_N_TRIALS       — Número de trials de Optuna (default: 100 = optimizado para velocidad)
  SMOOTH_FACTOR         — Factor de suavizado 0-1 (default: 0.25 = 25% = evita whipsaws)

Uso:
  python dynamic_optimizer.py
  python dynamic_optimizer.py --optimize-now   # fuerza optimización inmediata
"""

import csv
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import optuna
import redis
from dotenv import load_dotenv

load_dotenv()

# Silenciar los logs verbosos de Optuna en consola (el optimizer tiene su propio log)
optuna.logging.set_verbosity(optuna.logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────

from utils import madrid_now as _madrid_now, in_trading_hours as _in_trading_hours, madrid_today_str as _madrid_today_str, configure_structlog  # noqa: E402
configure_structlog("optimizer", log_file="dynamic_optimizer.log")


def _setup_logger(name: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.stream.reconfigure(encoding="utf-8", errors="replace") if hasattr(sh.stream, "reconfigure") else None
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


log = _setup_logger("optimizer", "dynamic_optimizer.log")

# Comisión real de Polymarket (igual que en calvin5.py)
POLYMARKET_FEE_PCT = 0.03  # 3% del USDC recibido en SELL anticipado


def _log_unified(redis_client: Optional[redis.Redis], msg: str, level: str = "INFO") -> None:
    """Publica el mensaje al canal logs:unified para el Dashboard (sync)."""
    if redis_client is None:
        return
    try:
        redis_client.publish("logs:unified", json.dumps({
            "bot":       "optimizer",
            "level":     level,
            "msg":       msg[:300],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

TRADES_CSV            = "calvin5_trades.csv"
WINDOW_PNL_FILE       = "calvin5_window.json"
REDIS_URL             = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL         = "config:update:calculation_bot"
OPTIMIZE_INTERVAL_H   = float(os.getenv("OPTIMIZE_INTERVAL_H", "1"))      # optimizado para 5min: cada 1 hora
DRAWDOWN_TRIGGER_PCT  = float(os.getenv("DRAWDOWN_TRIGGER_PCT", "0.10"))  # trigger urgente más agresivo (10%)
OPTUNA_N_TRIALS       = int(os.getenv("OPTUNA_N_TRIALS", "100"))          # menos trials para reactividad en 5min
OPTUNA_DB             = os.getenv("OPTUNA_DB", "sqlite:///optuna_calvin.db")  # persistencia entre reinicios
OPTUNA_STUDY_NAME     = os.getenv("OPTUNA_STUDY_NAME", "calvin5_v1")
SMOOTH_FACTOR         = float(os.getenv("SMOOTH_FACTOR", "0.25"))         # suavizado mayor (25%) para evitar whipsaws
MIN_TRADES_REQUIRED   = int(os.getenv("MIN_TRADES_REQUIRED", "150"))      # optimizado para 5min (~13 rondas de 5min)
MIN_TRADES_URGENT     = int(os.getenv("MIN_TRADES_URGENT",   "30"))       # fix 4: mínimo para triggers urgentes (WR bajo)


# ─────────────────────────────────────────────────────────────────────────────
#  LÍMITES DUROS (HARD BOUNDS) — extraídos de calvin5.py
#  Ningún parámetro optimizado puede salir de estos rangos bajo ninguna
#  circunstancia, independientemente de lo que proponga Optuna.
# ─────────────────────────────────────────────────────────────────────────────

HARD_BOUNDS: Dict[str, Tuple] = {
    # (min, max, type)   type: "float" | "int"
    # Límites de exploración para riesgo 8.5/10.
    # Si hay muchas pérdidas el optimizer baja hacia el mínimo;
    # si hay muchas ganancias sube hacia ABSOLUTE_OUTER_BOUNDS.
    "ENTRY_MIN":          (0.52,  0.70,  "float"),  # mínimo duro 0.52 — nunca operar por debajo
    "ENTRY_MAX":          (0.78,  0.90,  "float"),  # techo 0.90 — por encima no hay 6 tokens con stake $5.40
    "ENTRY_CUTOFF_S":     (10,    60,    "int"),     # era (15, 90) — permite cortes más tardíos
    "TP_FULL_PCT":        (0.12,  0.40,  "float"),
    "TP_MID_PCT":         (0.05,  0.18,  "float"),   # mínimo real > 3% (break-even con fee)
    "TP_LAST_15S_PCT":    (0.035, 0.08,  "float"),
    "SL_DROP":            (0.04,  0.20,  "float"),  # ampliado a 0.20 para riesgo 8/10
    "SL_FLOOR":           (0.25,  0.55,  "float"),  # era (0.30, 0.60) — floor más bajo permitido
    "SL_TIME_S":          (120,   270,   "int"),
    "SL_LAST_30S_PCT":    (0.02,  0.10,  "float"),
    "BTC_WINDOW_S":       (5,     30,    "int"),
    "BTC_MIN_PCT":        (0.02,  0.10,  "float"),
    "HOLD_BTC_PCT":       (0.08,  0.30,  "float"),
    "SLIP_MAX_PCT":       (0.01,  0.07,  "float"),
    "FOK_PRICE_BUF":      (0.01,  0.06,  "float"),
    "PRICE_STABILITY_SCANS": (1,   5,    "int"),
    "STAKE_USD":          (5.10,  8.00,  "float"),  # mín $5.10 garantiza ≥5 tokens a ENTRY_MAX=0.85
    "THROTTLE_S":         (1,     10,    "int"),
    "MAX_OPEN_POS":       (1,     3,     "int"),    # era (1, 2) — permite bajar a 1 si pierde o subir a 3
    "MAX_ENTRIES_PER_ROUND": (1,  5,     "int"),   # era (1, 4)
    "ENTRY_MAX_LATE":     (0.80,  0.97,  "float"),
    "HOLD_SECS":          (30,    120,   "int"),
}

# ─────────────────────────────────────────────────────────────────────────────
#  LÍMITES ABSOLUTOS EXTERIORES — techo máximo de exploración
#  Con confianza=1.0 los bounds efectivos alcanzan exactamente estos valores.
#  Son los límites de seguridad física que NUNCA se pueden superar bajo
#  ninguna circunstancia, sin importar cuánta confianza acumule el sistema.
# ─────────────────────────────────────────────────────────────────────────────

ABSOLUTE_OUTER_BOUNDS: Dict[str, Tuple] = {
    # (outer_min, outer_max, type)
    "ENTRY_MIN":          (0.52,  0.75,  "float"),  # mínimo absoluto 0.52 — nunca operar por debajo
    "ENTRY_MAX":          (0.75,  0.90,  "float"),  # techo absoluto 0.90 — límite por stake/tokens
    "ENTRY_CUTOFF_S":     (8,     90,    "int"),    # bajar = permite entradas muy tardías
    "TP_FULL_PCT":        (0.10,  0.55,  "float"),  # subir = esperar más ganancia
    "TP_MID_PCT":         (0.04,  0.25,  "float"),  # mínimo absoluto > 3% fee break-even
    "TP_LAST_15S_PCT":    (0.03,  0.12,  "float"),  # mínimo absoluto > 3% fee break-even
    "SL_DROP":            (0.02,  0.20,  "float"),  # bajar = stop loss más ajustado
    "SL_FLOOR":           (0.25,  0.70,  "float"),  # bajar = aguanta bajadas más profundas
    "SL_TIME_S":          (60,    300,   "int"),    # subir = aguanta más tiempo en posición
    "SL_LAST_30S_PCT":    (0.01,  0.15,  "float"),
    "BTC_WINDOW_S":       (3,     60,    "int"),    # subir = usa más contexto de BTC
    "BTC_MIN_PCT":        (0.02,  0.12,  "float"),  # techo absoluto 12%
    "HOLD_BTC_PCT":       (0.05,  0.40,  "float"),
    "SLIP_MAX_PCT":       (0.005, 0.10,  "float"),
    "FOK_PRICE_BUF":      (0.005, 0.08,  "float"),
    "PRICE_STABILITY_SCANS": (1,  8,     "int"),
    "STAKE_USD":          (5.10,  8.00,  "float"),  # mín $5.10 garantiza ≥5 tokens
    "THROTTLE_S":         (1,     20,    "int"),
    "MAX_OPEN_POS":       (1,     4,     "int"),    # subir = más posiciones simultáneas
    "MAX_ENTRIES_PER_ROUND": (1,  8,     "int"),
    "ENTRY_MAX_LATE":     (0.75,  0.99,  "float"),
    "HOLD_SECS":          (15,    180,   "int"),
}

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN DEL SISTEMA DE CONFIANZA DINÁMICA
# ─────────────────────────────────────────────────────────────────────────────

DYNAMIC_BOUNDS_FILE    = "dynamic_bounds.json"
BOUNDS_SMOOTH_FACTOR   = 0.15   # 15% de cambio por ciclo en los propios bounds
BOUNDS_MIN_TRADES      = 50     # mínimo de trades para empezar a expandir
BOUNDS_FULL_CONF_TRADES = 300   # trades para alcanzar confianza máxima (sin bonuses)

# Valores por defecto de calvin5.py (baseline cuando no hay CSV histórico)
# IMPORTANTE: Estos valores deben coincidir exactamente con las constantes en calvin5.py.
# Si el optimizer se resetea (SQLite borrado), enviará estos valores como primera actualización.
DEFAULTS: Dict[str, float] = {
    "ENTRY_MIN":          0.52,   # calvin5.py: ENTRY_MIN = 0.52
    "ENTRY_MAX":          0.90,   # calvin5.py: ENTRY_MAX = 0.90
    "ENTRY_CUTOFF_S":     12,
    "TP_FULL_PCT":        0.35,
    "TP_MID_PCT":         0.18,
    "TP_LAST_15S_PCT":    0.05,
    "SL_DROP":            0.18,   # calvin5.py: SL_DROP = 0.18
    "SL_FLOOR":           0.28,   # calvin5.py: SL_FLOOR = 0.28
    "SL_TIME_S":          230,
    "SL_LAST_30S_PCT":    0.05,
    "BTC_WINDOW_S":       10,
    "BTC_MIN_PCT":        0.05,   # calvin5.py: BTC_MIN_PCT = 0.05
    "HOLD_BTC_PCT":       0.15,
    "SLIP_MAX_PCT":       0.04,
    "FOK_PRICE_BUF":      0.01,
    "PRICE_STABILITY_SCANS": 1,
    "STAKE_USD":          5.40,   # $5.40 garantiza ≥6 tokens a ENTRY_MAX=0.90
    "THROTTLE_S":         1,
    "MAX_OPEN_POS":       1,      # alineado con calvin5.py
    "MAX_ENTRIES_PER_ROUND": 3,   # alineado con calvin5.py
    "ENTRY_MAX_LATE":     0.90,   # alineado con ENTRY_MAX
    "HOLD_SECS":          60,
}


# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS DE DATOS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """Representa un trade cerrado leído del CSV de calvin5."""
    id:           str
    side:         str
    market_slug:  str
    entry_price:  float
    exit_price:   float
    size_usd:     float
    pnl:          float
    exit_reason:  str
    entry_time:   float
    exit_time:    float

    @property
    def hold_seconds(self) -> float:
        return self.exit_time - self.entry_time

    @property
    def is_win(self) -> bool:
        return self.pnl > 0

    @property
    def pnl_pct(self) -> float:
        """PnL como % del size_usd."""
        return self.pnl / self.size_usd if self.size_usd > 0 else 0.0

    @property
    def exit_category(self) -> str:
        """Categoriza la salida: tp_full, tp_mid, sl, time."""
        r = self.exit_reason.lower()
        if "tp_full" in r:  return "tp_full"
        if "tp_mid"  in r:  return "tp_mid"
        if "sl"      in r:  return "sl"
        if "time"    in r:  return "time"
        return "other"


@dataclass
class PerformanceMetrics:
    """Métricas calculadas sobre un conjunto de trades."""
    n_trades:      int   = 0
    win_rate:      float = 0.0   # 0-1
    avg_pnl:       float = 0.0   # promedio de PnL por trade en $
    total_pnl:     float = 0.0
    max_drawdown:  float = 0.0   # drawdown máximo de la serie (positivo = pérdida)
    avg_hold_s:    float = 0.0
    profit_factor: float = 0.0   # suma ganancias / suma pérdidas
    avg_entry:     float = 0.0   # precio de entrada promedio
    sharpe_proxy:  float = 0.0   # avg_pnl / std_pnl (proxy sin risk-free)


# ─────────────────────────────────────────────────────────────────────────────
#  LECTOR DE DATOS DE RENDIMIENTO
# ─────────────────────────────────────────────────────────────────────────────

class PerformanceReader:
    """Lee y analiza los trades recientes de calvin5_trades.csv."""

    def __init__(self, csv_path: str = TRADES_CSV):
        self.csv_path = Path(csv_path)

    def load_recent_trades(self, last_n: int = 100) -> List[TradeRecord]:
        """Carga los últimos N trades del CSV. Devuelve lista vacía si no existe."""
        if not self.csv_path.exists():
            log.warning(f"CSV de trades no encontrado: {self.csv_path}")
            return []

        trades = []
        try:
            with open(self.csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        trades.append(TradeRecord(
                            id          = row["id"],
                            side        = row["side"],
                            market_slug = row["market_slug"],
                            entry_price = float(row["entry_price"]),
                            exit_price  = float(row["exit_price"]),
                            size_usd    = float(row["size_usd"]),
                            pnl         = float(row["pnl"]),
                            exit_reason = row["exit_reason"],
                            entry_time  = float(row["entry_time"]),
                            exit_time   = float(row["exit_time"]),
                        ))
                    except (KeyError, ValueError) as exc:
                        log.debug(f"Fila CSV ignorada: {exc}")
        except Exception as exc:
            log.error(f"Error leyendo CSV: {exc}")
            return []

        # Ordenar por tiempo de entrada y tomar los últimos N
        trades.sort(key=lambda t: t.entry_time)
        recent = trades[-last_n:]
        log.info(f"Cargados {len(recent)} trades recientes (total en CSV: {len(trades)})")
        return recent

    def compute_metrics(self, trades: List[TradeRecord]) -> PerformanceMetrics:
        """Calcula métricas de rendimiento sobre una lista de trades."""
        if not trades:
            return PerformanceMetrics()

        # Ajustar PnL histórico por fee si no viene ya ajustado en el CSV
        # (trades sin columna fee_usd asumen fee = 0 para no distorsionar historial)
        def _adj_pnl(t: TradeRecord) -> float:
            if t.exit_reason and any(x in t.exit_reason for x in ("tp_", "sl_")):
                tokens    = t.size_usd / t.entry_price if t.entry_price > 0 else 0
                gross     = t.exit_price * tokens
                fee       = gross * POLYMARKET_FEE_PCT
                net       = gross - fee
                return net - (t.entry_price * tokens)
            return t.pnl  # round_end: sin fee, PnL ya correcto

        pnls       = [_adj_pnl(t) for t in trades]
        wins       = [t for t in trades if _adj_pnl(t) > 0]
        losses     = [t for t in trades if _adj_pnl(t) <= 0]
        total_pnl  = sum(pnls)
        avg_pnl    = total_pnl / len(trades)
        win_rate   = len(wins) / len(trades)
        avg_hold   = sum(t.hold_seconds for t in trades) / len(trades)
        avg_entry  = sum(t.entry_price for t in trades) / len(trades)

        # Drawdown máximo
        cumulative = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnls:
            cumulative += p
            if cumulative > peak:
                peak = cumulative
            dd = peak - cumulative
            if dd > max_dd:
                max_dd = dd

        # Profit factor
        gross_profit = sum(_adj_pnl(t) for t in wins) if wins else 0.0
        gross_loss   = abs(sum(_adj_pnl(t) for t in losses)) if losses else 1e-9
        profit_factor = gross_profit / gross_loss

        # Sharpe proxy
        if len(pnls) > 1:
            mean = avg_pnl
            variance = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
            std = math.sqrt(variance) if variance > 0 else 1e-9
            sharpe_proxy = mean / std
        else:
            sharpe_proxy = 0.0

        m = PerformanceMetrics(
            n_trades      = len(trades),
            win_rate      = win_rate,
            avg_pnl       = avg_pnl,
            total_pnl     = total_pnl,
            max_drawdown  = max_dd,
            avg_hold_s    = avg_hold,
            profit_factor = profit_factor,
            avg_entry     = avg_entry,
            sharpe_proxy  = sharpe_proxy,
        )

        log.info(
            f"Métricas calculadas | trades={m.n_trades} WR={m.win_rate:.1%} "
            f"PnL={m.total_pnl:+.2f}$ avg={m.avg_pnl:+.3f}$ DD={m.max_drawdown:.2f}$ "
            f"PF={m.profit_factor:.2f} Sharpe={m.sharpe_proxy:.3f}"
        )
        return m

    def get_session_drawdown(self) -> float:
        """Lee el drawdown de la sesión actual desde calvin5_window.json."""
        path = Path(WINDOW_PNL_FILE)
        if not path.exists():
            return 0.0
        try:
            data  = json.loads(path.read_text(encoding="utf-8"))
            today = _madrid_today_str()
            if data.get("date") == today:
                pnl = float(data.get("pnl", 0.0))
                return abs(pnl) if pnl < 0 else 0.0
        except Exception as exc:
            log.warning(f"Error leyendo window PnL: {exc}")
        return 0.0

    def get_calvin5_live_state(self, redis_client) -> Optional[dict]:
        """Lee el estado en tiempo real de Calvin5 desde Redis (key state:calculator:latest)."""
        try:
            raw = redis_client.get("state:calculator:latest")
            if raw:
                return json.loads(raw)
        except Exception as exc:
            log.warning(f"No se pudo leer estado de Calvin5 desde Redis: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  GESTOR DE BOUNDS DINÁMICOS — expansión gradual basada en confianza
# ─────────────────────────────────────────────────────────────────────────────

class BoundsManager:
    """
    Gestiona la expansión gradual de los límites de búsqueda de Optuna
    conforme el sistema acumula confianza estadística.

    Confianza (0.0 – 1.0):
      - 0.0 → bounds = HARD_BOUNDS originales (conservadores)
      - 1.0 → bounds = ABSOLUTE_OUTER_BOUNDS (máxima exploración permitida)

    Componentes de la confianza:
      - Base:       crece con n_trades (0 en 50 trades, máx en 300)
      - Sharpe:     sharpe > 0.5 añade hasta +0.30
      - Win Rate:   WR > 55% añade hasta +0.20
      - Penaliza:   Sharpe < 0 o WR < 40% reduce la confianza fuertemente

    Suavizado doble:
      - La confianza misma se suaviza con BOUNDS_SMOOTH_FACTOR por ciclo
      - Cada bound se suaviza individualmente también
    """

    def __init__(self):
        self._effective: Dict[str, Tuple] = {}
        self._confidence: float = 0.0
        self.load()

    def load(self) -> None:
        """Carga bounds efectivos desde disco. Si no existe, usa HARD_BOUNDS."""
        path = Path(DYNAMIC_BOUNDS_FILE)
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                bounds = data.get("effective_bounds", {})
                confidence = float(data.get("confidence", 0.0))
                if all(k in bounds for k in HARD_BOUNDS):
                    self._effective = {k: tuple(v) for k, v in bounds.items()}
                    self._confidence = confidence
                    log.info(
                        f"Bounds dinámicos cargados desde {DYNAMIC_BOUNDS_FILE} "
                        f"(confianza={confidence:.1%})"
                    )
                    return
            except Exception as exc:
                log.warning(f"Error cargando {DYNAMIC_BOUNDS_FILE}: {exc} — usando HARD_BOUNDS")
        self._effective = dict(HARD_BOUNDS)
        self._confidence = 0.0
        log.info("Bounds inicializados con HARD_BOUNDS conservadores (sin historial previo)")

    def save(self) -> None:
        """Persiste bounds efectivos actuales en disco."""
        try:
            path = Path(DYNAMIC_BOUNDS_FILE)
            data = {
                "timestamp":  datetime.now(timezone.utc).isoformat(),
                "confidence": round(self._confidence, 4),
                "effective_bounds": {k: list(v) for k, v in self._effective.items()},
            }
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            log.warning(f"Error guardando {DYNAMIC_BOUNDS_FILE}: {exc}")

    def compute_confidence(self, metrics: PerformanceMetrics) -> float:
        """
        Calcula la confianza estadística 0.0–1.0 basada en métricas de rendimiento.
        Devuelve el valor TARGET (no suavizado) para el ciclo actual.
        """
        if metrics.n_trades < BOUNDS_MIN_TRADES:
            return 0.0

        # Componente base: crece linealmente con cantidad de datos
        base = min(
            (metrics.n_trades - BOUNDS_MIN_TRADES) / (BOUNDS_FULL_CONF_TRADES - BOUNDS_MIN_TRADES),
            1.0
        )

        # Bonus por Sharpe robusto (> 0.5 añade hasta +0.30)
        sharpe_bonus = min(max(metrics.sharpe_proxy - 0.5, 0.0), 1.0) * 0.30

        # Bonus por Win Rate saludable (> 55% añade hasta +0.20)
        wr_bonus = min(max(metrics.win_rate - 0.55, 0.0) * 4.0, 1.0) * 0.20

        raw = base + sharpe_bonus + wr_bonus

        # Penalización si el sistema va mal
        if metrics.sharpe_proxy < 0:
            raw *= 0.3
        elif metrics.win_rate < 0.40:
            raw *= 0.5

        return round(min(raw, 1.0), 4)

    def expand(self, metrics: PerformanceMetrics) -> float:
        """
        Ajusta los bounds efectivos según la confianza actual.
        Aplica suavizado a la confianza misma y a cada bound individualmente.
        Devuelve el nivel de confianza suavizado resultante.
        """
        target_confidence = self.compute_confidence(metrics)
        prev_confidence   = self._confidence

        # Suavizar la confianza (no saltar bruscamente)
        self._confidence = prev_confidence + (target_confidence - prev_confidence) * BOUNDS_SMOOTH_FACTOR

        new_effective: Dict[str, Tuple] = {}
        expanded: list = []

        for key in HARD_BOUNDS:
            init_low,  init_high,  ptype = HARD_BOUNDS[key]
            outer_low, outer_high, _     = ABSOLUTE_OUTER_BOUNDS[key]
            curr_low,  curr_high,  _     = self._effective.get(key, HARD_BOUNDS[key])

            # Target bounds interpolados por confianza suavizada
            target_low  = init_low  - (init_low  - outer_low)  * self._confidence
            target_high = init_high + (outer_high - init_high) * self._confidence

            # Suavizar el movimiento del bound mismo
            new_low  = curr_low  + (target_low  - curr_low)  * BOUNDS_SMOOTH_FACTOR
            new_high = curr_high + (target_high - curr_high) * BOUNDS_SMOOTH_FACTOR

            # Redondear por tipo
            if ptype == "int":
                new_low  = int(round(new_low))
                new_high = int(round(new_high))
            else:
                new_low  = round(new_low,  4)
                new_high = round(new_high, 4)

            new_effective[key] = (new_low, new_high, ptype)

            if abs(new_low - curr_low) > 1e-4 or abs(new_high - curr_high) > 1e-4:
                expanded.append(
                    f"  {key}: [{curr_low}, {curr_high}] → [{new_low}, {new_high}]"
                )

        self._effective = new_effective

        log.info(
            f"Confianza: {prev_confidence:.1%} → {self._confidence:.1%} "
            f"(target={target_confidence:.1%} | trades={metrics.n_trades} | "
            f"WR={metrics.win_rate:.1%} | Sharpe={metrics.sharpe_proxy:.3f})"
        )
        if expanded:
            log.info(f"Bounds expandidos en {len(expanded)} parámetros:")
            for line in expanded:
                log.info(line)
        else:
            log.info("Bounds sin cambios significativos este ciclo")

        self.save()
        return self._confidence

    def get_bounds(self) -> Dict[str, Tuple]:
        """Devuelve los bounds efectivos actuales (copia)."""
        return dict(self._effective)

    @property
    def confidence(self) -> float:
        return self._confidence


# ─────────────────────────────────────────────────────────────────────────────
#  MOTOR DE OPTIMIZACIÓN (OPTUNA — Bayesian Optimization)
# ─────────────────────────────────────────────────────────────────────────────

class BayesianOptimizer:
    """
    Usa Optuna para encontrar los parámetros óptimos de calvin5.py
    basándose en los trades recientes.

    Función objetivo:
      Simula qué trades habrían ocurrido bajo los parámetros del trial
      (filtrando por ENTRY_MIN/MAX) y calcula un score combinado de:
        - Win Rate
        - Sharpe proxy (avg_pnl / std_pnl)
        - Profit Factor
        - Penalización por drawdown excesivo
        - Penalización por muy pocos trades (sobre-filtrado)
    """

    def __init__(
        self,
        trades: List[TradeRecord],
        current_params: Dict[str, float],
        bounds: Optional[Dict[str, Tuple]] = None,
    ):
        self.trades         = trades
        self.current_params = current_params
        self.bounds         = bounds if bounds is not None else dict(HARD_BOUNDS)
        self.best_params:   Optional[Dict[str, float]] = None
        self.best_score:    float = float("-inf")

    def _simulate_filtered_trades(
        self,
        entry_min: float,
        entry_max: float,
        sl_drop: float,
        sl_floor: float,
        tp_mid_pct: float,
        tp_full_pct: float,
        max_entries_per_round: int = 4,
        throttle_s: float = 1,
    ) -> List[TradeRecord]:
        """
        Filtra los trades reales que habrían pasado los filtros de entrada
        del trial. Para TP/SL usamos los resultados reales (no los simulamos
        porque no tenemos el tick-by-tick — asumimos que la dirección del
        resultado es correcta pero la magnitud depende del TP/SL).

        max_entries_per_round y throttle_s se back-simulan agrupando por
        market_slug y aplicando restricciones temporales sobre entry_time.
        """
        # Primera pasada: filtros de precio
        candidates = []
        for t in self.trades:
            if not (entry_min <= t.entry_price <= entry_max):
                continue
            if t.exit_price < sl_floor and t.pnl < 0:
                simulated_loss = -(sl_floor - t.entry_price) * (t.size_usd / t.entry_price)
                sim = TradeRecord(
                    id=t.id, side=t.side, market_slug=t.market_slug,
                    entry_price=t.entry_price, exit_price=sl_floor,
                    size_usd=t.size_usd, pnl=simulated_loss,
                    exit_reason="sl_floor_sim",
                    entry_time=t.entry_time, exit_time=t.exit_time
                )
                candidates.append(sim)
            else:
                candidates.append(t)

        # Segunda pasada: back-simular MAX_ENTRIES_PER_ROUND y THROTTLE_S
        # Agrupar por market_slug y aplicar restricciones temporales
        from collections import defaultdict
        slug_entries: Dict[str, List[float]] = defaultdict(list)  # slug → [entry_time_ts]
        filtered = []
        for t in sorted(candidates, key=lambda x: x.entry_time):
            slug = t.market_slug or ""
            prev_times = slug_entries[slug]
            # Límite de entradas por ronda
            if len(prev_times) >= max_entries_per_round:
                continue
            # Throttle: mínimo throttle_s entre entradas del mismo mercado
            if prev_times and (t.entry_time - prev_times[-1]) < throttle_s:
                continue
            slug_entries[slug].append(t.entry_time)
            filtered.append(t)

        return filtered

    def objective(self, trial: optuna.Trial) -> float:
        """Función objetivo para Optuna. Devuelve el score a maximizar."""
        # ── Sugerir parámetros dentro de los bounds efectivos (dinámicos) ───
        b = self.bounds   # alias corto
        entry_min      = trial.suggest_float("ENTRY_MIN",      *b["ENTRY_MIN"][:2])
        entry_max      = trial.suggest_float("ENTRY_MAX",      *b["ENTRY_MAX"][:2])
        entry_max_late = trial.suggest_float("ENTRY_MAX_LATE", *b["ENTRY_MAX_LATE"][:2])
        entry_cutoff_s = trial.suggest_int  ("ENTRY_CUTOFF_S", *b["ENTRY_CUTOFF_S"][:2])
        tp_mid_pct     = trial.suggest_float("TP_MID_PCT",     *b["TP_MID_PCT"][:2])
        tp_full_pct    = trial.suggest_float("TP_FULL_PCT",    *b["TP_FULL_PCT"][:2])
        tp_last_15s    = trial.suggest_float("TP_LAST_15S_PCT",*b["TP_LAST_15S_PCT"][:2])
        sl_drop        = trial.suggest_float("SL_DROP",        *b["SL_DROP"][:2])
        sl_floor       = trial.suggest_float("SL_FLOOR",       *b["SL_FLOOR"][:2])
        sl_time_s      = trial.suggest_int  ("SL_TIME_S",      *b["SL_TIME_S"][:2])
        sl_last_30s    = trial.suggest_float("SL_LAST_30S_PCT",*b["SL_LAST_30S_PCT"][:2])
        btc_window_s   = trial.suggest_int  ("BTC_WINDOW_S",   *b["BTC_WINDOW_S"][:2])
        btc_min_pct    = trial.suggest_float("BTC_MIN_PCT",    *b["BTC_MIN_PCT"][:2])
        hold_btc_pct   = trial.suggest_float("HOLD_BTC_PCT",   *b["HOLD_BTC_PCT"][:2])
        hold_secs      = trial.suggest_int  ("HOLD_SECS",      *b["HOLD_SECS"][:2])
        slip_max_pct   = trial.suggest_float("SLIP_MAX_PCT",   *b["SLIP_MAX_PCT"][:2])
        fok_price_buf  = trial.suggest_float("FOK_PRICE_BUF",  *b["FOK_PRICE_BUF"][:2])
        price_stab     = trial.suggest_int  ("PRICE_STABILITY_SCANS", *b["PRICE_STABILITY_SCANS"][:2])
        stake_usd      = trial.suggest_float("STAKE_USD",      *b["STAKE_USD"][:2])
        throttle_s     = trial.suggest_int  ("THROTTLE_S",     *b["THROTTLE_S"][:2])
        max_open_pos   = trial.suggest_int  ("MAX_OPEN_POS",   *b["MAX_OPEN_POS"][:2])
        max_entries    = trial.suggest_int  ("MAX_ENTRIES_PER_ROUND", *b["MAX_ENTRIES_PER_ROUND"][:2])

        # ── Restricciones lógicas (pruning temprano) ────────────────────────
        if entry_min >= entry_max:
            raise optuna.exceptions.TrialPruned()
        if tp_mid_pct >= tp_full_pct:
            raise optuna.exceptions.TrialPruned()
        if tp_last_15s >= tp_mid_pct:
            raise optuna.exceptions.TrialPruned()

        # ── Simulación de trades bajo estos parámetros ──────────────────────
        sim_trades = self._simulate_filtered_trades(
            entry_min              = entry_min,
            entry_max              = entry_max,
            sl_drop                = sl_drop,
            sl_floor               = sl_floor,
            tp_mid_pct             = tp_mid_pct,
            tp_full_pct            = tp_full_pct,
            max_entries_per_round  = max_entries,
            throttle_s             = throttle_s,
        )

        # Penalización severa por sobre-filtrado (muy pocos trades → no fiable)
        n = len(sim_trades)
        if n < max(3, len(self.trades) * 0.10):
            return -10.0

        # ── Calcular métricas del subconjunto simulado ──────────────────────
        # Ajustar PnL por la fee real de Polymarket
        def _fee_adjusted_pnl(t: TradeRecord) -> float:
            if t.exit_category == "other":  # round_end u otros sin fee
                return t.pnl
            tokens     = t.size_usd / t.entry_price if t.entry_price > 0 else 0
            gross_usdc = t.exit_price * tokens
            fee_usdc   = gross_usdc * POLYMARKET_FEE_PCT
            net_usdc   = gross_usdc - fee_usdc
            return net_usdc - (t.entry_price * tokens)

        pnls       = [_fee_adjusted_pnl(t) for t in sim_trades]
        wins       = [t for t in sim_trades if _fee_adjusted_pnl(t) > 0]
        losses     = [t for t in sim_trades if _fee_adjusted_pnl(t) <= 0]
        win_rate   = len(wins) / n
        total_pnl  = sum(pnls)
        mean_pnl   = total_pnl / n

        # Sharpe proxy
        if n > 1:
            variance = sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)
            std = math.sqrt(variance) if variance > 0 else 1e-9
            sharpe = mean_pnl / std
        else:
            sharpe = 0.0

        # Profit factor
        gross_win  = sum(_fee_adjusted_pnl(t) for t in wins) if wins else 0.0
        gross_loss = abs(sum(_fee_adjusted_pnl(t) for t in losses)) if losses else 1e-9
        pf = gross_win / gross_loss

        # Drawdown máximo de la simulación
        cum = 0.0
        peak_cum = 0.0
        max_dd = 0.0
        for p in pnls:
            cum += p
            if cum > peak_cum:
                peak_cum = cum
            dd = peak_cum - cum
            if dd > max_dd:
                max_dd = dd

        # ── Score combinado ─────────────────────────────────────────────────
        # Ponderaciones: priorizamos Sharpe (consistencia) y WR, luego PF
        score = (
            0.40 * sharpe           # consistencia riesgo/retorno
            + 0.25 * win_rate * 10  # win rate (escalado para misma magnitud)
            + 0.20 * min(pf, 5.0)   # profit factor (capeado en 5 para no explotar)
            + 0.15 * (mean_pnl * 100)  # PnL medio por trade (escalado)
            - 0.10 * max_dd          # penalización drawdown
        )

        # Penalizar filtro BTC demasiado restrictivo cuando hay pocos trades
        # Con <20 trades el optimizer no tiene datos para justificar umbrales altos
        if len(sim_trades) < 20 and btc_min_pct > 0.05:
            score -= (btc_min_pct - 0.05) * 5.0

        # Penalización suave por stake muy alto (riesgo de capital)
        if stake_usd > 10.0:
            score -= (stake_usd - 10.0) * 0.05

        # Bonus si filtramos más que el baseline (ENTRY_MIN más alto = más selectivos)
        current_entry_min = self.current_params.get("ENTRY_MIN", 0.62)
        if entry_min > current_entry_min:
            score += (entry_min - current_entry_min) * 0.5

        # ── Ajustes secundarios con parámetros no directamente simulables ───
        # Estos parámetros son explorados por Optuna para que Guardrails.apply()
        # los publique a calvin5. Se usan en ajustes menores donde es posible.

        # entry_max_late debe ser lógicamente <= entry_max (restrict)
        if entry_max_late > entry_max:
            raise optuna.exceptions.TrialPruned()

        # entry_cutoff_s: más alto → más disciplinado (evitar late-squeeze)
        score += (entry_cutoff_s - 40) * 0.001

        # Penalizar trades donde hold_seconds excedió sl_time_s con pérdida
        held_overtime_losses = sum(
            1 for t in sim_trades if t.hold_seconds > sl_time_s and t.pnl < 0
        )
        if sim_trades:
            score -= (held_overtime_losses / len(sim_trades)) * 0.5

        # max_open_pos > 1: penalizar concentración de riesgo
        if max_open_pos > 1:
            score -= (max_open_pos - 1) * 0.2

        # Los siguientes no son simulables con los datos disponibles pero
        # se registran en Optuna para que Guardrails.apply() los publique.
        # Usamos una expresión sin efecto para suprimir el warning del linter.
        _ = (sl_last_30s, btc_window_s, hold_btc_pct, fok_price_buf,
             price_stab, hold_secs)

        return score

    def run(self, n_trials: int = OPTUNA_N_TRIALS) -> Optional[Dict[str, float]]:
        """
        Ejecuta la optimización Bayesiana.
        Devuelve el diccionario de mejores parámetros o None si falla.
        """
        log.info(f"Iniciando optimización Bayesiana con {n_trials} trials...")

        try:
            study = optuna.create_study(
                direction   = "maximize",
                sampler     = optuna.samplers.TPESampler(seed=42),
                pruner      = optuna.pruners.MedianPruner(n_startup_trials=10),
                storage     = OPTUNA_DB,          # persiste trials entre reinicios
                study_name  = OPTUNA_STUDY_NAME,
                load_if_exists = True,            # reanuda estudio existente
            )
            study.optimize(
                self.objective,
                n_trials    = n_trials,
                show_progress_bar = False,
                n_jobs      = 1,   # 1 para reproducibilidad
            )

            best = study.best_trial
            log.info(
                f"Optimización completada — mejor score: {best.value:.4f} "
                f"(trial #{best.number})"
            )

            # Registrar los mejores parámetros encontrados
            result = dict(best.params)

            # Completar con los parámetros no optimizados directamente
            # (se mantienen los valores actuales para los que no se incluyeron en Optuna)
            for k, v in self.current_params.items():
                if k not in result:
                    result[k] = v

            self.best_params = result
            self.best_score  = best.value
            return result

        except Exception as exc:
            log.error(f"Error en optimización Bayesiana: {exc}", exc_info=True)
            return None


# ─────────────────────────────────────────────────────────────────────────────
#  GUARDRAILS — Suavizado y validación de parámetros
# ─────────────────────────────────────────────────────────────────────────────

class Guardrails:
    """
    Aplica límites duros y suavizado a los parámetros propuestos por Optuna.
    Ningún parámetro puede salir de HARD_BOUNDS y los cambios se suavizan
    aplicando solo SMOOTH_FACTOR del delta propuesto.
    """

    def __init__(self, smooth_factor: float = SMOOTH_FACTOR):
        self.smooth_factor = smooth_factor

    def apply(
        self,
        current: Dict[str, float],
        proposed: Dict[str, float],
        bounds: Optional[Dict[str, Tuple]] = None,
    ) -> Dict[str, float]:
        """
        Aplica suavizado y bounds efectivos (dinámicos si se pasan, HARD_BOUNDS si no).
        Devuelve los parámetros finales seguros para publicar.
        """
        effective_bounds = bounds if bounds is not None else HARD_BOUNDS
        result = {}

        for key, bounds_tuple in effective_bounds.items():
            low, high, ptype = bounds_tuple
            curr_val = current.get(key, DEFAULTS.get(key, (low + high) / 2))
            prop_val = proposed.get(key, curr_val)

            # ── Suavizado: aplicar solo smooth_factor del cambio ──────────
            delta     = prop_val - curr_val
            smoothed  = curr_val + delta * self.smooth_factor

            # ── Hard bounds: clamp al rango seguro ────────────────────────
            clamped   = max(low, min(high, smoothed))

            # ── Casting al tipo correcto ───────────────────────────────────
            if ptype == "int":
                clamped = int(round(clamped))
            else:
                clamped = round(clamped, 4)

            result[key] = clamped

            if abs(clamped - curr_val) > 1e-6:
                log.debug(
                    f"  {key}: {curr_val} -> prop={prop_val:.4f} "
                    f"-> smoothed={smoothed:.4f} -> final={clamped}"
                )

        # ── Validaciones de consistencia lógica ───────────────────────────
        if result.get("ENTRY_MIN", 0) >= result.get("ENTRY_MAX", 1):
            log.warning("ENTRY_MIN >= ENTRY_MAX tras suavizado — restaurando ENTRY_MAX")
            result["ENTRY_MAX"] = min(
                result["ENTRY_MIN"] + 0.10,
                effective_bounds["ENTRY_MAX"][1]
            )

        if result.get("TP_MID_PCT", 0) >= result.get("TP_FULL_PCT", 1):
            log.warning("TP_MID_PCT >= TP_FULL_PCT tras suavizado — ajustando")
            result["TP_FULL_PCT"] = min(
                result["TP_MID_PCT"] + 0.05,
                effective_bounds["TP_FULL_PCT"][1]
            )

        return result


# ─────────────────────────────────────────────────────────────────────────────
#  PUBLICADOR REDIS
# ─────────────────────────────────────────────────────────────────────────────

class RedisPublisher:
    """Publica la nueva configuración en Redis para que calvin5.py la consuma."""

    def __init__(self, url: str = REDIS_URL, channel: str = REDIS_CHANNEL):
        self.url     = url
        self.channel = channel
        self._client: Optional[redis.Redis] = None

    def connect(self) -> bool:
        """Intenta conectar a Redis. Devuelve True si tiene éxito."""
        try:
            self._client = redis.Redis.from_url(self.url, decode_responses=True)
            self._client.ping()
            log.info(f"Redis conectado: {self.url}")
            return True
        except Exception as exc:
            log.error(f"No se pudo conectar a Redis ({self.url}): {exc}")
            self._client = None
            return False

    def publish(self, params: Dict[str, float], metrics: PerformanceMetrics) -> bool:
        """
        Publica los parámetros optimizados en el canal de Redis.
        El payload incluye los parámetros + metadatos de la optimización.
        """
        payload = {
            "timestamp":    datetime.utcnow().isoformat(),
            "source":       "dynamic_optimizer",
            "bot":          "calvin5",
            "params":       params,
            "metadata": {
                "n_trades_analyzed": metrics.n_trades,
                "win_rate":          round(metrics.win_rate, 4),
                "total_pnl":         round(metrics.total_pnl, 4),
                "max_drawdown":      round(metrics.max_drawdown, 4),
                "profit_factor":     round(metrics.profit_factor, 4),
                "sharpe_proxy":      round(metrics.sharpe_proxy, 4),
            }
        }

        message = json.dumps(payload, ensure_ascii=False)

        if self._client is None:
            log.warning("Redis no conectado — guardando config en archivo local como fallback")
            self._save_local_fallback(payload)
            return False

        try:
            subscribers = self._client.publish(self.channel, message)
            log.info(
                f"Config publicada en Redis '{self.channel}' "
                f"({subscribers} subscriber(s) notificados)"
            )
            # También guardar en una key para que el bot pueda leerla al arrancar
            self._client.set(
                f"{self.channel}:latest",
                message,
                ex=86400  # expira en 24h
            )
            return True
        except Exception as exc:
            log.error(f"Error publicando en Redis: {exc}")
            self._save_local_fallback(payload)
            return False

    def _save_local_fallback(self, payload: dict) -> None:
        """Si Redis no está disponible, guarda la config en un archivo JSON local."""
        path = Path("dynamic_optimizer_output.json")
        try:
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            log.info(f"Config guardada en fallback local: {path}")
        except Exception as exc:
            log.error(f"Error guardando fallback local: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  ROBOT DE AJUSTE DINÁMICO (orquestador principal)
# ─────────────────────────────────────────────────────────────────────────────

class DynamicOptimizerBot:
    """
    Orquestador principal del Robot de Ajuste Dinámico.

    Ciclo de vida:
      1. Cargar trades recientes y calcular métricas
      2. Verificar si se cumple el trigger (tiempo o drawdown)
      3. Ejecutar optimización Bayesiana con Optuna
      4. Aplicar guardrails (suavizado + hard bounds)
      5. Publicar nueva config en Redis
      6. Esperar hasta el próximo trigger
    """

    def __init__(self):
        self.reader        = PerformanceReader()
        self.guardrails    = Guardrails()
        self.publisher     = RedisPublisher()
        self.bounds_manager = BoundsManager()
        self.current_params: Dict[str, float] = dict(DEFAULTS)
        self.last_optimize_ts: float = 0.0
        self._optimize_interval_s = OPTIMIZE_INTERVAL_H * 3600

        log.info("=" * 65)
        log.info("DynamicOptimizerBot iniciado")
        log.info(f"  Intervalo: cada {OPTIMIZE_INTERVAL_H}h | "
                 f"Drawdown trigger: {DRAWDOWN_TRIGGER_PCT:.0%} | "
                 f"Optuna trials: {OPTUNA_N_TRIALS} | "
                 f"Suavizado: {SMOOTH_FACTOR:.0%} | "
                 f"[OPTIMIZADO PARA MERCADOS 5MIN]")
        log.info("=" * 65)

    def _load_current_params_from_redis(self) -> None:
        """
        Al arrancar, intenta leer la última config publicada desde Redis
        para usar como baseline en lugar de los defaults hardcodeados.
        """
        if self.publisher._client is None:
            return
        try:
            raw = self.publisher._client.get(f"{REDIS_CHANNEL}:latest")
            if raw:
                data = json.loads(raw)
                params = data.get("params", {})
                if params:
                    self.current_params.update(params)
                    log.info("Parámetros actuales restaurados desde Redis")
        except Exception as exc:
            log.warning(f"No se pudieron cargar parámetros desde Redis: {exc}")

    def _should_optimize(self, session_drawdown: float) -> Tuple[bool, str, bool]:
        """
        Evalúa si se debe lanzar una optimización.
        Devuelve (should_run, reason, urgent).
        urgent=True significa usar MIN_TRADES_URGENT en lugar de MIN_TRADES_REQUIRED (fix 4).
        """
        now = time.time()

        # Trigger por tiempo
        elapsed_h = (now - self.last_optimize_ts) / 3600
        if elapsed_h >= OPTIMIZE_INTERVAL_H:
            return True, f"Intervalo de {OPTIMIZE_INTERVAL_H}h alcanzado ({elapsed_h:.1f}h desde última optimización)", False

        # Trigger por drawdown urgente
        if session_drawdown > 0:
            capital_ref = max(self.current_params.get("STAKE_USD", 5.0) * 20, 50.0)
            dd_pct = session_drawdown / capital_ref
            if dd_pct >= DRAWDOWN_TRIGGER_PCT:
                return True, (
                    f"Drawdown urgente: ${session_drawdown:.2f} "
                    f"({dd_pct:.1%} >= {DRAWDOWN_TRIGGER_PCT:.0%} umbral)"
                ), False

        # Trigger por WR bajo en las últimas 20 operaciones (estrategia deteriorada)
        trades = self.reader.load_recent_trades(last_n=100)
        if len(trades) >= 20:
            recent_20 = trades[-20:]
            recent_wr = sum(1 for t in recent_20 if t.pnl > 0) / 20
            if recent_wr < 0.45:
                # fix 4: WR trigger es urgente — usa MIN_TRADES_URGENT (100) en lugar de 300
                return True, f"WR bajo en últimas 20 operaciones: {recent_wr:.0%} < 45%", True

            # Trigger si Calvin5 activo pero sin trades en las últimas 2h en horario operativo
            last_trade_age_h = (time.time() - trades[-1].exit_time) / 3600
            if last_trade_age_h > 2.0 and _in_trading_hours():
                return True, f"Sin trades desde hace {last_trade_age_h:.1f}h en horario activo", False

        return False, "", False

    def run_optimization_cycle(self, urgent: bool = False) -> bool:
        """
        Ejecuta un ciclo completo de optimización.
        urgent=True usa MIN_TRADES_URGENT (100) en lugar de MIN_TRADES_REQUIRED (300) (fix 4).
        Devuelve True si se publicó una nueva configuración.
        """
        log.info("── Iniciando ciclo de optimización ──────────────────────────")

        # 0. Leer parámetros vivos reales de Calvin5 desde Redis y actualizar baseline
        if self.publisher._client is not None:
            calvin_state = self.reader.get_calvin5_live_state(self.publisher._client)
            if calvin_state and "params" in calvin_state:
                live_params = calvin_state["params"]
                updated_keys = []
                for k, v in live_params.items():
                    if k in self.current_params:
                        self.current_params[k] = float(v)
                        updated_keys.append(k)
                if updated_keys:
                    log.info(f"Baseline actualizado con parámetros vivos de Calvin5: {updated_keys}")

        # 1. Cargar datos de rendimiento
        min_trades = MIN_TRADES_URGENT if urgent else MIN_TRADES_REQUIRED
        trades = self.reader.load_recent_trades(last_n=max(min_trades, 100))
        metrics = self.reader.compute_metrics(trades)

        if metrics.n_trades < min_trades:
            log.warning(
                f"Solo {metrics.n_trades} trades disponibles "
                f"(mínimo {min_trades}{'  [urgent]' if urgent else ''}). Optimización pospuesta."
            )
            self.last_optimize_ts = time.time()
            return False

        log.info(
            f"Datos cargados: {metrics.n_trades} trades | "
            f"WR={metrics.win_rate:.1%} | PnL={metrics.total_pnl:+.2f}$ | "
            f"DD_max={metrics.max_drawdown:.2f}$ | Sharpe={metrics.sharpe_proxy:.3f}"
        )

        # 2. Actualizar bounds dinámicos según confianza estadística actual
        confidence = self.bounds_manager.expand(metrics)
        effective_bounds = self.bounds_manager.get_bounds()
        log.info(f"Confianza estadística actual: {confidence:.1%} | Bounds activos para Optuna")

        # 3. Ejecutar Optuna con los bounds efectivos actuales
        optimizer = BayesianOptimizer(
            trades         = trades,
            current_params = self.current_params,
            bounds         = effective_bounds,
        )
        proposed_params = optimizer.run(n_trials=OPTUNA_N_TRIALS)

        if proposed_params is None:
            log.error("Optimización fallida — manteniendo parámetros actuales")
            self.last_optimize_ts = time.time()
            return False

        log.info(f"Optuna score: {optimizer.best_score:.4f}")

        # 4. Aplicar guardrails con los bounds efectivos del ciclo actual
        log.info(f"Aplicando guardrails (suavizado {SMOOTH_FACTOR:.0%}, confianza {confidence:.1%})...")
        safe_params = self.guardrails.apply(
            current  = self.current_params,
            proposed = proposed_params,
            bounds   = effective_bounds,
        )

        # 5. Log de cambios significativos
        changes = []
        for k, new_v in safe_params.items():
            old_v = self.current_params.get(k, DEFAULTS.get(k))
            if old_v is not None and abs(new_v - old_v) > 1e-6:
                pct_change = (new_v - old_v) / abs(old_v) * 100 if old_v != 0 else 0
                changes.append(f"  {k}: {old_v} → {new_v} ({pct_change:+.1f}%)")

        if changes:
            log.info(f"Cambios aplicados ({len(changes)}):")
            for c in changes:
                log.info(c)
        else:
            log.info("No hay cambios significativos respecto a los parámetros actuales")

        # 5. Publicar en Redis
        published = self.publisher.publish(safe_params, metrics)

        # 6. Actualizar estado interno
        self.current_params = safe_params
        self.last_optimize_ts = time.time()

        log.info(
            f"Ciclo completado — publicado={'sí' if published else 'no (fallback local)'} | "
            f"próxima optimización en {OPTIMIZE_INTERVAL_H}h"
        )
        _log_unified(
            self.publisher._client,
            f"OPTIMIZER ciclo completado: {metrics.n_trades} trades analizados, "
            f"score={optimizer.best_score:.4f}, params aplicados: {len(changes)}",
            "INFO",
        )
        log.info("─" * 65)
        return True

    def run(self) -> None:
        """Bucle principal del robot. Corre indefinidamente."""
        # Intentar conectar a Redis al arrancar
        self.publisher.connect()
        self._load_current_params_from_redis()

        # Si se ejecuta con --optimize-now, forzar ciclo inmediato
        force_now = "--optimize-now" in sys.argv
        if force_now:
            log.info("--optimize-now detectado: ejecutando optimización inmediata")
            self.run_optimization_cycle()

        CHECK_SLEEP_S = 60  # verificar triggers cada minuto

        while True:
            try:
                # Solo optimizar en horario Madrid o justo antes/después (±1h)
                now_madrid = _madrid_now()
                in_or_near_trading = 9 <= now_madrid.hour <= 23
                if not in_or_near_trading and not force_now:
                    log.debug(
                        f"Fuera de horario ({now_madrid.hour}h Madrid) — optimización diferida"
                    )
                    time.sleep(CHECK_SLEEP_S)
                    continue
                force_now = False  # solo aplica una vez

                session_drawdown = self.reader.get_session_drawdown()
                should_run, reason, urgent = self._should_optimize(session_drawdown)

                if should_run:
                    log.info(f"Trigger activado: {reason}")
                    _log_unified(self.publisher._client, f"Trigger activado: {reason}", "INFO")
                    self.run_optimization_cycle(urgent=urgent)
                else:
                    # Log periódico de estado (cada 15 min aproximadamente)
                    elapsed_min = (time.time() - self.last_optimize_ts) / 60
                    next_in_min = max(0, OPTIMIZE_INTERVAL_H * 60 - elapsed_min)
                    log.debug(
                        f"En espera — próxima optimización en {next_in_min:.0f}min | "
                        f"DD sesión: ${session_drawdown:.2f}"
                    )

                # ── Chequear comando de optimización forzada desde Dashboard ──
                rc = self.publisher._client
                if rc is not None:
                    try:
                        force_cmd = rc.get("optimizer:force_now")
                        if force_cmd:
                            rc.delete("optimizer:force_now")
                            log.info("Optimización forzada desde Dashboard")
                            _log_unified(rc, "Optimización forzada desde Dashboard", "INFO")
                            self.run_optimization_cycle()
                    except Exception:
                        pass

            except KeyboardInterrupt:
                log.info("DynamicOptimizerBot detenido por el usuario (Ctrl+C)")
                break
            except Exception as exc:
                log.error(
                    f"Error inesperado en bucle principal: {exc} — "
                    f"continuando en {CHECK_SLEEP_S}s",
                    exc_info=True
                )

            time.sleep(CHECK_SLEEP_S)


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from utils import configure_structlog
    configure_structlog("dynamic_optimizer", log_file="dynamic_optimizer.log")
    bot = DynamicOptimizerBot()
    bot.run()
