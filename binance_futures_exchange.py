"""
binance_futures_exchange.py — Adaptador Binance USDT-M Futures con apalancamiento para CalvinBot.

Opera en BTCUSDT Perpetual (mercado de futuros de Binance):
  - Apalancamiento configurable (default x20)
  - Margen aislado por posición
  - Órdenes STOP_MARKET nativas (sin precio límite, cierre garantizado)
  - Compatible con la misma interfaz que BinanceSpotExchange

Testnet:  https://testnet.binancefuture.com  (necesita API keys específicas de futuros)
Mainnet:  https://fapi.binance.com

Variables de entorno (.env):
  BINANCE_API_KEY       — API key de Binance Futures (Testnet o Real)
  BINANCE_API_SECRET    — API secret de Binance Futures
  BINANCE_TESTNET       — "true" para testnet (default: true)
  BINANCE_SYMBOL        — símbolo perpetuo a operar (default: BTCUSDT)
  LEVERAGE              — apalancamiento (default: 20)
  MARGIN_TYPE           — "isolated" o "cross" (default: isolated)
  SL_DROP_PCT           — % de caída desde entrada para stop-loss (default: 0.008 = 0.8%)

Nota sobre sizing con apalancamiento:
  Si STAKE_USD=$75 y LEVERAGE=20:
    Margen usado:       $75
    Posición real:      $1500 de BTC
    BTC comprado:       $1500 / precio_BTC  (ej. 0.01578 BTC a $95,000)
"""

import asyncio
import logging
import math
import os
import time
from typing import Optional, Tuple

log = logging.getLogger("execution_bot")

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_TESTNET    = os.getenv("BINANCE_TESTNET", "true").lower() in ("1", "true")
BINANCE_SYMBOL     = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
LEVERAGE           = int(os.getenv("LEVERAGE", "20"))
MARGIN_TYPE        = os.getenv("MARGIN_TYPE", "isolated")   # "isolated" | "cross"
SL_DROP_PCT        = float(os.getenv("SL_DROP_PCT", "0.008"))
DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"

# Cantidad mínima de BTC a operar (filtro LOT_SIZE de Binance Futures)
MIN_QTY = 0.001  # 0.001 BTC mínimo en BTCUSDT perp


# ─────────────────────────────────────────────────────────────────────────────
#  ADAPTADOR FUTURES
# ─────────────────────────────────────────────────────────────────────────────

class BinanceFuturesExchange:
    """
    Adaptador para Binance USDT-M Futures Perpetual (BTCUSDT).

    Interfaz pública compatible con BinanceSpotExchange:
      initialize()            → configura ccxt + leverage + margin type
      fetch_balance()         → saldo USDT disponible como margen
      create_buy_order()      → abre LONG por mercado
      create_sell_order()     → cierra posición por mercado (reduceOnly)
      place_stop_loss()       → coloca STOP_MARKET protector
      cancel_order()          → cancela una orden por ID
      cancel_all_orders()     → cancela todas las órdenes abiertas del símbolo
      compute_stop_prices()   → calcula precio stop desde fill_price
    """

    def __init__(self) -> None:
        self._client = None
        self._initialized = False
        self._symbol = BINANCE_SYMBOL
        self._leverage = LEVERAGE

    # ── Inicialización ─────────────────────────────────────────────────────────

    def initialize(self) -> None:
        """Configura ccxt, establece apalancamiento y tipo de margen."""
        if DRY_RUN:
            log.info(f"[FUTURES][DRY] DRY RUN — sin conexión real a Binance Futures")
            self._initialized = True
            return

        try:
            import ccxt

            client = ccxt.binanceusdm({
                "apiKey":  BINANCE_API_KEY,
                "secret":  BINANCE_API_SECRET,
                "options": {
                    "adjustForTimeDifference": True,
                    "recvWindow": 10000,
                },
            })

            if BINANCE_TESTNET:
                client.set_sandbox_mode(True)
                log.info("[FUTURES] Modo TESTNET activo — testnet.binancefuture.com")
            else:
                log.warning("[FUTURES] Modo REAL — operando en Binance Futures MAINNET")

            # Cargar mercados
            client.load_markets()

            # Configurar apalancamiento
            try:
                client.fapiPrivate_post_leverage({
                    "symbol":   self._symbol,
                    "leverage": self._leverage,
                })
                log.info(f"[FUTURES] Leverage configurado: x{self._leverage} en {self._symbol}")
            except Exception as ex:
                log.warning(f"[FUTURES] No se pudo configurar leverage ({ex}) — puede que ya esté configurado")

            # Configurar tipo de margen
            try:
                margin_upper = MARGIN_TYPE.upper()
                client.fapiPrivate_post_margintype({
                    "symbol":     self._symbol,
                    "marginType": margin_upper,
                })
                log.info(f"[FUTURES] Margin type: {margin_upper}")
            except Exception as ex:
                # Si ya está en ese modo, Binance devuelve error "No need to change"
                log.debug(f"[FUTURES] margin type: {ex}")

            self._client = client
            self._initialized = True
            log.info(
                f"[FUTURES] Inicializado OK | symbol={self._symbol} "
                f"leverage=x{self._leverage} margin={MARGIN_TYPE} "
                f"testnet={BINANCE_TESTNET}"
            )

        except ImportError:
            raise RuntimeError("ccxt no instalado — ejecutar: pip install ccxt")
        except Exception as exc:
            log.error(f"[FUTURES] Error inicializando: {exc}")
            raise

    async def _async_init(self) -> None:
        """No-op: los filtros de futuros ya se cargan en load_markets()."""
        pass

    # ── Balance ────────────────────────────────────────────────────────────────

    async def fetch_balance(self) -> float:
        """Retorna el saldo USDT disponible como margen."""
        if DRY_RUN or self._client is None:
            return 9999.0

        loop = asyncio.get_running_loop()
        try:
            balance = await loop.run_in_executor(
                None, lambda: self._client.fetch_balance()
            )
            usdt = balance.get("USDT", {})
            free = float(usdt.get("free", 0) or 0)
            log.debug(f"[FUTURES] Balance USDT: ${free:.2f}")
            return free
        except Exception as exc:
            log.warning(f"[FUTURES] Error consultando balance: {exc}")
            return 0.0

    # ── Compra (abrir LONG) ────────────────────────────────────────────────────

    async def create_buy_order(
        self,
        token_id: str,
        price: float,
        size_usd: float,
    ) -> Tuple[float, float]:
        """
        Abre posición LONG en futuros por mercado.

        Con apalancamiento: position_value = size_usd * leverage
        El qty en BTC = position_value / precio_actual.

        Returns (qty_btc, fill_price). (0.0, price) si falla.
        """
        if DRY_RUN or self._client is None:
            position_value = size_usd * self._leverage
            qty = round(position_value / price, 3)
            qty = max(qty, MIN_QTY)
            log.info(
                f"[FUTURES][DRY] BUY simulado: {qty:.4f} BTC "
                f"margin=${size_usd:.2f} posición=${position_value:.2f} "
                f"leverage=x{self._leverage}"
            )
            return qty, price

        position_value = size_usd * self._leverage
        qty = self._round_qty(position_value / price)

        if qty < MIN_QTY:
            log.error(
                f"[FUTURES] BUY rechazado: qty={qty:.6f} < mínimo {MIN_QTY} "
                f"(margin=${size_usd:.2f} leverage=x{self._leverage} price={price:.2f})"
            )
            return 0.0, price

        loop = asyncio.get_running_loop()
        t_start = time.time()

        try:
            order = await loop.run_in_executor(
                None,
                lambda: self._client.create_market_buy_order(
                    symbol=token_id,
                    amount=qty,
                    params={"positionSide": "BOTH"},
                )
            )
            latency_ms = (time.time() - t_start) * 1000
            fill_price = float(order.get("average") or order.get("price") or price)
            filled_qty = float(order.get("filled") or qty)

            log.info(
                f"[FUTURES] BUY FILLED: {filled_qty:.4f} BTC "
                f"@ ${fill_price:.2f} (margin=${size_usd:.2f} "
                f"posición=${filled_qty * fill_price:.2f}) "
                f"lat={latency_ms:.0f}ms"
            )
            return filled_qty, fill_price

        except Exception as exc:
            log.error(f"[FUTURES] BUY error: {exc}")
            return 0.0, price

    # ── Venta (cerrar LONG) ────────────────────────────────────────────────────

    async def create_sell_order(
        self,
        token_id: str,
        price: float,
        size_tokens: float,
        **kwargs,
    ) -> bool:
        """Cierra posición LONG por mercado (reduceOnly)."""
        if DRY_RUN or self._client is None:
            log.info(f"[FUTURES][DRY] SELL simulado: {size_tokens:.4f} BTC @ ~${price:.2f}")
            return True

        qty = self._round_qty(size_tokens)
        if qty <= 0:
            log.error(f"[FUTURES] SELL rechazado: qty={qty}")
            return False

        loop = asyncio.get_running_loop()
        try:
            order = await loop.run_in_executor(
                None,
                lambda: self._client.create_market_sell_order(
                    symbol=token_id,
                    amount=qty,
                    params={"reduceOnly": True, "positionSide": "BOTH"},
                )
            )
            fill_price = float(order.get("average") or order.get("price") or price)
            log.info(f"[FUTURES] SELL FILLED: {qty:.4f} BTC @ ${fill_price:.2f}")
            return True

        except Exception as exc:
            log.error(f"[FUTURES] SELL error: {exc}")
            return False

    # ── Stop-Loss nativo ───────────────────────────────────────────────────────

    def compute_stop_prices(self, fill_price: float) -> Tuple[float, float]:
        """
        Calcula el precio stop desde el precio de entrada.
        Retorna (stop_price, stop_price) — en futuros STOP_MARKET no necesita precio límite.
        """
        stop_price = round(fill_price * (1.0 - SL_DROP_PCT), 2)
        return stop_price, stop_price

    async def place_stop_loss(
        self,
        symbol:      str,
        qty:         float,
        stop_price:  float,
        limit_price: float,   # ignorado en STOP_MARKET de futuros
    ) -> str:
        """
        Coloca una orden STOP_MARKET (proteción de posición LONG).
        Se activa cuando el precio cae a stop_price y cierra la posición.
        Retorna el order ID como string, o "" si falla.
        """
        if DRY_RUN or self._client is None:
            log.info(f"[FUTURES][DRY] Stop-loss simulado @ ${stop_price:.2f}")
            return "DRY_STOP_ORDER"

        loop = asyncio.get_running_loop()
        try:
            order = await loop.run_in_executor(
                None,
                lambda: self._client.create_order(
                    symbol=symbol,
                    type="STOP_MARKET",
                    side="sell",
                    amount=self._round_qty(qty),
                    params={
                        "stopPrice":    stop_price,
                        "reduceOnly":   True,
                        "positionSide": "BOTH",
                        "workingType":  "CONTRACT_PRICE",
                    },
                )
            )
            order_id = str(order["id"])
            log.info(f"[FUTURES] Stop-loss STOP_MARKET colocado: id={order_id} stop=${stop_price:.2f}")
            return order_id

        except Exception as exc:
            log.error(f"[FUTURES] Error colocando stop-loss: {exc}")
            return ""

    # ── Cancelación de órdenes ─────────────────────────────────────────────────

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancela una orden por ID. Retorna True si canceló o ya no existía."""
        if DRY_RUN or self._client is None:
            return True

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._client.cancel_order(order_id, symbol)
            )
            log.info(f"[FUTURES] Orden {order_id} cancelada")
            return True
        except Exception as exc:
            exc_str = str(exc).lower()
            if "unknown order" in exc_str or "-2011" in exc_str:
                log.debug(f"[FUTURES] Orden {order_id} ya no existe (posiblemente ejecutada)")
                return True
            log.warning(f"[FUTURES] Error cancelando {order_id}: {exc}")
            return False

    async def cancel_all_orders(self) -> None:
        """Cancela todas las órdenes abiertas del símbolo."""
        if DRY_RUN or self._client is None:
            return

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                lambda: self._client.cancel_all_orders(self._symbol)
            )
            log.info(f"[FUTURES] Todas las órdenes de {self._symbol} canceladas")
        except Exception as exc:
            log.warning(f"[FUTURES] Error cancelando órdenes: {exc}")

    # ── Reconciliación ─────────────────────────────────────────────────────────

    async def reconcile_open_positions(self, tracked: dict) -> dict:
        """
        Verifica qué posiciones del tracker siguen activas en Binance Futures.
        Devuelve el subconjunto de `tracked` que sigue abierto.
        """
        if DRY_RUN or self._client is None:
            return tracked

        loop = asyncio.get_running_loop()
        try:
            positions = await loop.run_in_executor(
                None,
                lambda: self._client.fetch_positions([self._symbol])
            )
            # Hay posición activa si positionAmt != 0
            has_real_position = any(
                float(p.get("positionAmt", 0) or 0) != 0
                for p in positions
                if p.get("symbol") == self._symbol
            )

            if not has_real_position:
                log.warning("[FUTURES] Reconcile: sin posición abierta en Binance — limpiando tracker")
                return {}

            return tracked

        except Exception as exc:
            log.warning(f"[FUTURES] Error en reconcile: {exc} — manteniendo tracker")
            return tracked

    async def fetch_open_orders(self, symbol: str) -> list:
        """Retorna lista de órdenes abiertas para el símbolo."""
        if DRY_RUN or self._client is None:
            return []
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                None, lambda: self._client.fetch_open_orders(symbol)
            )
        except Exception:
            return []

    async def close(self) -> None:
        pass

    # ── Helpers internos ───────────────────────────────────────────────────────

    @staticmethod
    def _round_qty(qty: float, step: float = 0.001) -> float:
        """Redondea qty al step size de BTCUSDT Futures (0.001 BTC)."""
        if step <= 0:
            return qty
        rounded = math.floor(qty / step) * step
        return round(rounded, 3)
