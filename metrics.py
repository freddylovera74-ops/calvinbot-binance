"""
metrics.py — Métricas Prometheus para CalvinBot.

Expone un endpoint HTTP /metrics en el puerto METRICS_PORT (default: 9090).
Si prometheus_client no está instalado, es un no-op completo (el resto del
sistema sigue funcionando sin ningún cambio).

Métricas disponibles:
  calvin_trades_total{mode, side, result}  — trades cerrados (counter)
  calvin_pnl_usd{mode}                    — PnL acumulado de sesión (gauge)
  calvin_open_positions                    — posiciones abiertas ahora (gauge)
  calvin_btc_momentum_pct                 — último valor de momentum BTC (gauge)
  calvin_signal_eval_total{outcome}       — evaluaciones de señal (counter)
  calvin_ws_reconnects_total              — reconexiones WebSocket (counter)
  executor_orders_total{side, result}     — órdenes ejecutadas (counter)
  executor_order_latency_seconds          — latencia de orden (histogram)
  optimizer_trials_total                  — trials Optuna completados (counter)
  optimizer_best_score                    — mejor score del estudio (gauge)

Uso:
    from metrics import metrics
    metrics.inc_trades(mode="REAL", side="UP", result="win")
    metrics.set_pnl(mode="REAL", value=12.5)

    # En main(), antes de asyncio.gather():
    await metrics.start_server()
"""

import os
from typing import Optional

METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "1") == "1"

# ── Intentar importar prometheus_client ───────────────────────────────────────

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, start_http_server, CollectorRegistry, REGISTRY
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False


class _NullMetric:
    """Stub que acepta cualquier llamada sin hacer nada."""
    def labels(self, **_):      return self
    def inc(self, *_, **__):    pass
    def set(self, *_, **__):    pass
    def observe(self, *_, **__): pass
    def __call__(self, *_, **__): return self


class CalvinMetrics:
    """
    Fachada única de métricas para todos los bots.
    Si prometheus_client no está disponible, todas las operaciones son no-op.
    """

    def __init__(self) -> None:
        self._ready = False
        if not _PROM_AVAILABLE or not METRICS_ENABLED:
            self._null = _NullMetric()
            return

        self.trades = Counter(
            "calvin_trades_total",
            "Trades cerrados",
            ["mode", "side", "result"],
        )
        self.pnl = Gauge(
            "calvin_pnl_usd",
            "PnL acumulado de sesión en USD",
            ["mode"],
        )
        self.open_positions = Gauge(
            "calvin_open_positions",
            "Posiciones abiertas actualmente",
        )
        self.btc_momentum = Gauge(
            "calvin_btc_momentum_pct",
            "Último valor de momentum BTC (%)",
        )
        self.signal_evals = Counter(
            "calvin_signal_eval_total",
            "Evaluaciones de señal por resultado",
            ["outcome"],   # "entry", "no_market", "no_price", "momentum_fail", "price_range_fail", ...
        )
        self.ws_reconnects = Counter(
            "calvin_ws_reconnects_total",
            "Reconexiones del WebSocket Polymarket",
        )
        self.executor_orders = Counter(
            "executor_orders_total",
            "Órdenes enviadas al exchange",
            ["side", "result"],   # result: "filled", "timeout", "error"
        )
        self.executor_latency = Histogram(
            "executor_order_latency_seconds",
            "Latencia de orden en el exchange (segundos)",
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
        )
        self.optimizer_trials = Counter(
            "optimizer_trials_total",
            "Trials Optuna completados",
        )
        self.optimizer_best = Gauge(
            "optimizer_best_score",
            "Mejor score del estudio Optuna actual",
        )
        self._ready = True

    # ── Helpers de acceso seguro ───────────────────────────────────────────────

    def _get(self, name: str) -> _NullMetric:
        """Devuelve la métrica o un NullMetric si no está disponible."""
        if not self._ready:
            return _NullMetric()
        return getattr(self, name, _NullMetric())

    # ── API pública ────────────────────────────────────────────────────────────

    def inc_trade(self, mode: str, side: str, result: str) -> None:
        self._get("trades").labels(mode=mode, side=side, result=result).inc()

    def set_pnl(self, mode: str, value: float) -> None:
        self._get("pnl").labels(mode=mode).set(value)

    def set_open_positions(self, n: int) -> None:
        self._get("open_positions").set(n)

    def set_btc_momentum(self, pct: float) -> None:
        self._get("btc_momentum").set(pct)

    def inc_signal_eval(self, outcome: str) -> None:
        self._get("signal_evals").labels(outcome=outcome).inc()

    def inc_ws_reconnect(self) -> None:
        self._get("ws_reconnects").inc()

    def inc_executor_order(self, side: str, result: str) -> None:
        self._get("executor_orders").labels(side=side, result=result).inc()

    def observe_executor_latency(self, seconds: float) -> None:
        self._get("executor_latency").observe(seconds)

    def inc_optimizer_trial(self) -> None:
        self._get("optimizer_trials").inc()

    def set_optimizer_best(self, score: float) -> None:
        self._get("optimizer_best").set(score)

    async def start_server(self) -> None:
        """Arranca el servidor HTTP de métricas en METRICS_PORT."""
        if not self._ready:
            return
        try:
            start_http_server(METRICS_PORT)
            import logging
            logging.getLogger(__name__).info(
                f"[METRICS] Prometheus endpoint en http://localhost:{METRICS_PORT}/metrics"
            )
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(f"[METRICS] No se pudo iniciar servidor: {exc}")


# Singleton global — importar y usar directamente
metrics = CalvinMetrics()
