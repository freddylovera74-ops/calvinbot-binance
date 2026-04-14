"""
risk_engine.py — Motor de riesgo para CalvinBot Binance.

Implementa las reglas de riesgo que el sistema carecía:
  1. Tamaño de posición basado en % del wallet (no stake fijo)
  2. Filtro de comisión pre-trade (bloquea si ganancia esperada < comisión × 2)
  3. Validación de R:R mínimo antes de abrir
  4. Límite de trades por hora (anti-overtrading)
  5. Cooldown tras pérdida (tiempo de espera obligatorio)
  6. Contador de pérdidas consecutivas (pausa automática)

Uso:
    from risk_engine import RiskEngine, TradeSafetyManager

    engine = RiskEngine(leverage=20, taker_rate=0.0004)
    stake, qty = engine.compute_position_size(wallet=500, risk_pct=0.0025, sl_pct=0.008, price=84000)
    ok, reason = engine.check_commission(qty, price, tp_pct=0.012)
    ok, rr     = engine.check_rr(tp_pct=0.012, sl_pct=0.008, min_rr=1.5)
"""

import logging
import time
from typing import List, Optional, Tuple

log = logging.getLogger("risk_engine")


# ─────────────────────────────────────────────────────────────────────────────
#  MOTOR DE RIESGO
# ─────────────────────────────────────────────────────────────────────────────

class RiskEngine:
    """
    Calcula parámetros de riesgo para cada trade.

    Todos los métodos son puros (sin estado interno) — el estado de sesión
    se gestiona en TradeSafetyManager.
    """

    def __init__(self, leverage: int = 20, taker_rate: float = 0.0004) -> None:
        self.leverage    = leverage
        self.taker_rate  = taker_rate  # 0.04% por defecto en Binance Futures

    def compute_position_size(
        self,
        wallet:   float,
        risk_pct: float,
        sl_pct:   float,
        price:    float,
    ) -> Tuple[float, float]:
        """
        Calcula el tamaño de posición basado en riesgo % del wallet.

        Fórmula:
          risk_usd     = wallet × risk_pct
          position_val = risk_usd / sl_pct          (notional en USDT)
          stake_usd    = position_val / leverage     (margen requerido)
          qty_btc      = position_val / price        (BTC a comprar/vender)

        Args:
          wallet:   balance total del wallet en USDT
          risk_pct: % del wallet a arriesgar (ej. 0.0025 = 0.25%)
          sl_pct:   distancia del SL en precio (ej. 0.008 = 0.8%)
          price:    precio actual de BTC

        Returns:
          (stake_usd, qty_btc) — margen a usar y BTC a operar
        """
        if wallet <= 0 or risk_pct <= 0 or sl_pct <= 0 or price <= 0:
            return 0.0, 0.0

        risk_usd     = wallet * risk_pct
        position_val = risk_usd / sl_pct          # notional en USDT
        stake_usd    = position_val / self.leverage
        qty_btc      = position_val / price

        log.debug(
            f"[RISK] wallet={wallet:.2f} risk={risk_pct:.3%} sl={sl_pct:.3%} → "
            f"risk_usd={risk_usd:.2f} position={position_val:.2f} "
            f"margin={stake_usd:.2f} qty={qty_btc:.5f}BTC"
        )
        return round(stake_usd, 4), round(qty_btc, 6)

    def estimate_commission(self, qty_btc: float, price: float) -> float:
        """
        Estima la comisión total (apertura + cierre) en USDT.

        commission_per_side = notional × taker_rate
        total = × 2 (entrada + salida)
        """
        notional = qty_btc * price
        commission_one_side = notional * self.taker_rate
        return round(commission_one_side * 2, 4)

    def expected_tp_gain(self, qty_btc: float, price: float, tp_pct: float) -> float:
        """Ganancia bruta esperada si se alcanza el TP (antes de comisiones)."""
        notional = qty_btc * price
        return round(notional * tp_pct, 4)

    def check_commission(
        self,
        qty_btc: float,
        price:   float,
        tp_pct:  float,
        min_ratio: float = 2.0,
    ) -> Tuple[bool, str]:
        """
        Bloquea el trade si la ganancia esperada en TP es < comisión × min_ratio.

        Un min_ratio de 2.0 significa que la ganancia debe cubrir 2× las comisiones
        para que el trade tenga expectativa positiva mínima.

        Returns (ok, reason)
        """
        commission = self.estimate_commission(qty_btc, price)
        tp_gain    = self.expected_tp_gain(qty_btc, price, tp_pct)

        if tp_gain < commission * min_ratio:
            return False, (
                f"COMMISSION_FILTER: TP esperado ${tp_gain:.4f} < "
                f"comisión×{min_ratio:.0f} ${commission*min_ratio:.4f} "
                f"(comisión total=${commission:.4f})"
            )
        return True, ""

    def check_rr(
        self,
        tp_pct: float,
        sl_pct: float,
        min_rr: float = 1.5,
    ) -> Tuple[bool, float]:
        """
        Valida que el ratio R:R sea >= min_rr.

        rr = tp_pct / sl_pct (en términos de precio, sin apalancamiento)
        El apalancamiento cancela en la división, así que rr es simétrico.

        Returns (ok, rr_ratio)
        """
        if sl_pct <= 0:
            return False, 0.0
        rr = tp_pct / sl_pct
        return rr >= min_rr, round(rr, 3)

    def compute_sl_price(self, entry: float, sl_pct: float, side: str) -> float:
        """Calcula el precio de stop-loss."""
        if side == "SHORT":
            return round(entry * (1.0 + sl_pct), 2)
        return round(entry * (1.0 - sl_pct), 2)

    def compute_tp_price(self, entry: float, tp_pct: float, side: str) -> float:
        """Calcula el precio de take-profit."""
        if side == "SHORT":
            return round(entry * (1.0 - tp_pct), 2)
        return round(entry * (1.0 + tp_pct), 2)

    def compute_tp_partial_price(self, entry: float, tp_partial_pct: float, side: str) -> float:
        """Calcula el precio del take-profit parcial."""
        if side == "SHORT":
            return round(entry * (1.0 - tp_partial_pct), 2)
        return round(entry * (1.0 + tp_partial_pct), 2)


# ─────────────────────────────────────────────────────────────────────────────
#  GESTOR DE SEGURIDAD DE SESIÓN
# ─────────────────────────────────────────────────────────────────────────────

class TradeSafetyManager:
    """
    Gestiona el estado de seguridad de la sesión de trading:
      - Límite de trades por hora (anti-overtrading)
      - Cooldown obligatorio tras pérdida
      - Pausa automática tras N pérdidas consecutivas

    Uso:
        safety = TradeSafetyManager(max_per_hour=3, cooldown_loss_s=300, max_consecutive=3)

        ok, reason = safety.check_safe_to_trade()
        if ok:
            # ejecutar trade
            if win:
                safety.record_win()
            else:
                safety.record_loss()
    """

    def __init__(
        self,
        max_per_hour:      int   = 3,
        cooldown_loss_s:   float = 300.0,
        max_consecutive:   int   = 3,
    ) -> None:
        self.max_per_hour    = max_per_hour
        self.cooldown_loss_s = cooldown_loss_s
        self.max_consecutive = max_consecutive

        self._trade_timestamps: List[float] = []  # ts de cada trade cerrado (últimas 1h)
        self._consecutive_losses: int        = 0
        self._last_loss_ts:       float      = 0.0
        self._safety_paused:      bool       = False

    @property
    def consecutive_losses(self) -> int:
        return self._consecutive_losses

    @property
    def is_safety_paused(self) -> bool:
        return self._safety_paused

    def resume(self) -> None:
        """Reanuda operaciones (Telegram /resume o manual)."""
        self._safety_paused    = False
        self._consecutive_losses = 0

    def check_safe_to_trade(self) -> Tuple[bool, str]:
        """
        Verifica si es seguro abrir un trade en este momento.

        Returns (ok, reason_if_not_ok)
        """
        now = time.time()

        # 1. Pausa por pérdidas consecutivas
        if self._safety_paused:
            return False, (
                f"SAFETY_PAUSE: {self._consecutive_losses} pérdidas consecutivas — "
                f"envía /resume para reanudar"
            )

        # 2. Cooldown tras última pérdida
        if self._last_loss_ts > 0:
            elapsed = now - self._last_loss_ts
            remaining = self.cooldown_loss_s - elapsed
            if remaining > 0:
                return False, (
                    f"LOSS_COOLDOWN: {remaining:.0f}s restantes "
                    f"(cooldown de {self.cooldown_loss_s:.0f}s tras pérdida)"
                )

        # 3. Límite de trades por hora
        cutoff = now - 3600
        self._trade_timestamps = [ts for ts in self._trade_timestamps if ts >= cutoff]
        if len(self._trade_timestamps) >= self.max_per_hour:
            oldest_in_window = self._trade_timestamps[0]
            recover_in = oldest_in_window + 3600 - now
            return False, (
                f"HOURLY_LIMIT: {len(self._trade_timestamps)}/{self.max_per_hour} "
                f"trades en la última hora — recupera en {recover_in:.0f}s"
            )

        return True, ""

    def record_win(self) -> None:
        """Registra un trade ganador. Resetea racha de pérdidas."""
        self._trade_timestamps.append(time.time())
        self._consecutive_losses = 0
        log.debug(f"[SAFETY] WIN — consecutivas pérdidas reseteadas")

    def record_loss(self) -> None:
        """Registra un trade perdedor. Activa cooldown y pausa si se supera el límite."""
        now = time.time()
        self._trade_timestamps.append(now)
        self._last_loss_ts = now
        self._consecutive_losses += 1

        log.warning(
            f"[SAFETY] LOSS #{self._consecutive_losses}/{self.max_consecutive} consecutivas"
        )

        if self._consecutive_losses >= self.max_consecutive:
            self._safety_paused = True
            log.warning(
                f"[SAFETY] PAUSA AUTOMÁTICA: {self._consecutive_losses} pérdidas consecutivas"
            )

    def record_trade_opened(self) -> None:
        """Registra apertura de trade para el contador horario."""
        self._trade_timestamps.append(time.time())

    def stats(self) -> dict:
        """Retorna estado resumido para heartbeat/dashboard."""
        now = time.time()
        cutoff = now - 3600
        trades_hour = len([ts for ts in self._trade_timestamps if ts >= cutoff])
        cooldown_remaining = max(0.0, self.cooldown_loss_s - (now - self._last_loss_ts)) if self._last_loss_ts > 0 else 0.0
        return {
            "consecutive_losses":  self._consecutive_losses,
            "trades_last_hour":    trades_hour,
            "max_per_hour":        self.max_per_hour,
            "cooldown_remaining_s": round(cooldown_remaining),
            "safety_paused":       self._safety_paused,
        }
