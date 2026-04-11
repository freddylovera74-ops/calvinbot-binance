"""
binance_exchange.py — Adaptador Binance Spot Testnet para CalvinBot.

Implementa la misma interfaz conceptual que PolymarketCLOBExchange para que
execution_bot.py pueda cambiar de exchange configurando EXCHANGE_ADAPTER=binance.

Diferencias clave vs Polymarket CLOB:
  - Binance usa precios reales (ej. 95000.00), no tokens 0-1
  - STOP_LOSS_LIMIT nativa — no necesita monitoring manual de SL en la estrategia
  - LOT_SIZE, PRICE_FILTER, NOTIONAL: filtros consultados vía exchangeInfo
  - Market BUY con quoteOrderQty (USDT) → recibe qty base (BTC)
  - Market SELL con quantity (BTC qty) → recibe USDT

Instalación extra:
  pip install ccxt

Variables de entorno (.env):
  BINANCE_API_KEY     — API key de Binance Testnet
  BINANCE_API_SECRET  — API secret de Binance Testnet
  BINANCE_TESTNET     — "true" para testnet (default: true)
  BINANCE_SYMBOL      — símbolo a operar (default: BTCUSDT)
  SL_DROP_PCT         — % de caída desde entrada para stop-loss (default: 0.01 = 1%)
  SL_LIMIT_BUFFER     — % por debajo del stop para precio límite (default: 0.003 = 0.3%)

Uso:
  from binance_exchange import BinanceSpotExchange
  exchange = BinanceSpotExchange()
  exchange.initialize()            # síncrono — configura ccxt
  await exchange._async_init()     # async — carga exchangeInfo y filtros
"""

import asyncio
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("execution_bot")

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_TESTNET    = os.getenv("BINANCE_TESTNET", "true").lower() in ("1", "true")
BINANCE_SYMBOL     = os.getenv("BINANCE_SYMBOL", "BTCUSDT")

# Stop-loss nativo
SL_DROP_PCT      = float(os.getenv("SL_DROP_PCT",      "0.01"))   # 1% bajo entrada
SL_LIMIT_BUFFER  = float(os.getenv("SL_LIMIT_BUFFER",  "0.003"))  # precio límite = stop * (1-0.3%)

# Slippage
SLIP_MAX_PCT     = float(os.getenv("SLIP_MAX_PCT",     "0.004"))  # 0.4% máximo

# DRY RUN
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() == "true"
DRY_SLIPPAGE_MAX = float(os.getenv("DRY_SLIPPAGE_MAX", "0.002"))
DRY_LATENCY_MIN  = int(os.getenv("DRY_LATENCY_MIN_MS", "150"))
DRY_LATENCY_MAX  = int(os.getenv("DRY_LATENCY_MAX_MS", "600"))
DRY_REJECT_RATE  = float(os.getenv("DRY_REJECT_RATE",  "0.05"))  # 5% rechazos simulados


# ─────────────────────────────────────────────────────────────────────────────
#  ESTRUCTURAS
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SymbolInfo:
    """
    Filtros y precisión de un símbolo de Binance, extraídos de exchangeInfo.
    Necesario para redondear correctamente cantidad y precio antes de enviar órdenes.
    """
    symbol:              str
    qty_precision:       int        # decimales para quantity (base asset)
    price_precision:     int        # decimales para price (quote asset)
    step_size:           float      # mínimo incremento de qty (LOT_SIZE)
    tick_size:           float      # mínimo incremento de precio (PRICE_FILTER)
    min_qty:             float      # qty mínima (LOT_SIZE.minQty)
    max_qty:             float      # qty máxima (LOT_SIZE.maxQty)
    min_notional:        float      # valor mínimo en USDT (NOTIONAL o MIN_NOTIONAL)
    allowed_order_types: List[str]  # tipos de orden disponibles para este símbolo


# ─────────────────────────────────────────────────────────────────────────────
#  ADAPTADOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

class BinanceSpotExchange:
    """
    Adaptador Binance Spot Testnet.

    Implementa la misma interfaz que PolymarketCLOBExchange para que
    execution_bot.py pueda intercambiarlos sin modificar la lógica de negocio.

    Ventajas sobre Polymarket:
      - STOP_LOSS_LIMIT nativo: Binance gestiona el SL automáticamente
      - MARKET BUY con quoteOrderQty: se especifica USDT, Binance devuelve BTC exacto
      - Rate limits conocidos: 1200 req/min, 50 órdenes/10s (gestionados por ccxt)
      - exchangeInfo: validación de filtros antes de enviar la orden

    Flujo típico:
      1. BUY → create_buy_order() → recibe (qty_btc, fill_price)
      2. SL  → place_stop_loss()  → recibe stop_order_id
      3. TP  → cancel_order(stop_order_id) + create_sell_order()
         (o el SL se dispara solo si el precio cae)
    """

    def __init__(self):
        self._exchange   = None          # instancia ccxt.async_support.binance
        self._initialized = False
        self._symbol_info: Optional[SymbolInfo] = None
        self._symbol     = BINANCE_SYMBOL

    # ── Inicialización ────────────────────────────────────────────────────────

    def initialize(self) -> None:
        """
        Configura el cliente ccxt (síncrono).
        No carga mercados todavía — eso requiere await _async_init().
        """
        if DRY_RUN:
            log.info("[BINANCE] DRY RUN — sin conexión real al exchange")
            self._symbol_info = SymbolInfo(
                symbol=self._symbol,
                qty_precision=5, price_precision=2,
                step_size=0.00001, tick_size=0.01,
                min_qty=0.00001, max_qty=9000.0,
                min_notional=10.0,
                allowed_order_types=["MARKET", "LIMIT", "STOP_LOSS_LIMIT"],
            )
            self._initialized = True
            return

        if not BINANCE_API_KEY or not BINANCE_API_SECRET:
            raise ValueError(
                "BINANCE_API_KEY y BINANCE_API_SECRET deben estar configuradas en .env"
            )

        try:
            import ccxt.async_support as ccxt

            self._exchange = ccxt.binance({
                "apiKey":          BINANCE_API_KEY,
                "secret":          BINANCE_API_SECRET,
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            })

            if BINANCE_TESTNET:
                self._exchange.set_sandbox_mode(True)
                log.info("[BINANCE] Modo TESTNET activado → testnet.binance.vision")
            else:
                log.warning("[BINANCE] MAINNET activado — cuidado con dinero real")

            self._initialized = True
            log.info(f"[BINANCE] Exchange configurado — símbolo: {self._symbol}")

        except ImportError:
            raise RuntimeError(
                "ccxt no instalado. Ejecutar: pip install ccxt"
            )
        except Exception as exc:
            log.error(f"[BINANCE] Error inicializando: {exc}")
            raise

    async def _async_init(self) -> None:
        """
        Carga exchangeInfo del símbolo y extrae filtros de precisión.
        Llamar desde el event loop después de initialize().
        """
        if DRY_RUN or self._exchange is None:
            return

        try:
            log.info(f"[BINANCE] Cargando exchangeInfo para {self._symbol}...")
            await self._exchange.load_markets()
            market = self._exchange.market(self._symbol)
            info   = market.get("info", {})

            # Parsear filtros de Binance
            filters = {f["filterType"]: f for f in info.get("filters", [])}

            lot_filter  = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})
            # Binance usa NOTIONAL en nuevos símbolos, MIN_NOTIONAL en legacy
            notional_filter = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))

            step_size = float(lot_filter.get("stepSize", "0.00001"))
            tick_size = float(price_filter.get("tickSize", "0.01"))
            min_notional = float(
                notional_filter.get("minNotional") or
                notional_filter.get("notional") or
                "10.0"
            )

            allowed_types = info.get("orderTypes", [])

            self._symbol_info = SymbolInfo(
                symbol=self._symbol,
                qty_precision=self._count_decimals(step_size),
                price_precision=self._count_decimals(tick_size),
                step_size=step_size,
                tick_size=tick_size,
                min_qty=float(lot_filter.get("minQty", "0.00001")),
                max_qty=float(lot_filter.get("maxQty", "9000")),
                min_notional=min_notional,
                allowed_order_types=allowed_types,
            )

            log.info(
                f"[BINANCE] {self._symbol} cargado: "
                f"stepSize={step_size} tickSize={tick_size} "
                f"minNotional=${min_notional:.2f} "
                f"orderTypes={allowed_types}"
            )

            if "STOP_LOSS_LIMIT" not in allowed_types:
                log.warning(
                    f"[BINANCE] STOP_LOSS_LIMIT NO disponible para {self._symbol}. "
                    f"La estrategia gestionará el SL manualmente."
                )

        except Exception as exc:
            log.error(f"[BINANCE] Error cargando exchangeInfo: {exc}")
            raise

    # ── Helpers de precisión ──────────────────────────────────────────────────

    @staticmethod
    def _count_decimals(value: float) -> int:
        """Cuenta decimales significativos. Ej: 0.00001 → 5, 0.10 → 1."""
        s = f"{value:.10f}".rstrip("0")
        return len(s.split(".")[-1]) if "." in s else 0

    def _round_qty(self, qty: float) -> float:
        """
        Redondea qty al stepSize con math.floor (no round).
        Floor garantiza que nunca enviamos más de lo que tenemos.
        """
        if self._symbol_info is None:
            return round(qty, 5)
        step = self._symbol_info.step_size
        if step <= 0:
            return qty
        floored = math.floor(qty / step) * step
        return round(floored, self._symbol_info.qty_precision)

    def _round_price(self, price: float) -> float:
        """Redondea precio al tickSize del símbolo."""
        if self._symbol_info is None:
            return round(price, 2)
        tick = self._symbol_info.tick_size
        if tick <= 0:
            return price
        rounded = round(round(price / tick) * tick, self._symbol_info.price_precision)
        return rounded

    # ── Balance ───────────────────────────────────────────────────────────────

    async def fetch_balance(self) -> float:
        """Retorna el balance USDT libre (disponible para órdenes)."""
        if DRY_RUN or self._exchange is None:
            return 9999.0

        try:
            balance = await self._exchange.fetch_balance()
            usdt    = float(balance.get("USDT", {}).get("free", 0.0) or 0.0)
            log.debug(f"[BINANCE] Balance USDT libre: ${usdt:.2f}")
            return usdt
        except Exception as exc:
            log.warning(f"[BINANCE] Error consultando balance USDT: {exc}")
            return 0.0

    # ── Órdenes BUY ──────────────────────────────────────────────────────────

    async def create_buy_order(
        self,
        token_id: str,    # reutilizado como símbolo (ej. "BTCUSDT")
        price: float,     # precio de señal — solo para cálculo de slippage
        size_usd: float,  # USDT a gastar
    ) -> Tuple[float, float]:
        """
        MARKET BUY con quoteOrderQty: especifica USDT, Binance devuelve qty exacta de BTC.

        Returns:
            (qty_btc, fill_price_avg) — qty en BTC recibida y precio promedio de fill.
            (0.0, price) si falla o hay slippage inaceptable.

        Nota: En Binance Spot, un MARKET BUY con quoteOrderQty garantiza que
        se gasta exactamente size_usd USDT (o menos si el libro está vacío).
        El fill puede ocurrir a múltiples precios — fill_price es el promedio ponderado.
        """
        symbol = token_id

        # ── DRY RUN simulado ────────────────────────────────────────────────
        if DRY_RUN:
            latency_ms = random.randint(DRY_LATENCY_MIN, DRY_LATENCY_MAX)
            await asyncio.sleep(latency_ms / 1000)
            if random.random() < DRY_REJECT_RATE:
                log.info(f"[BINANCE][DRY] BUY rechazado (simulado): {symbol} ${size_usd:.2f}")
                return 0.0, price
            slip       = random.uniform(0, DRY_SLIPPAGE_MAX)
            fill_price = price * (1 + slip)
            qty_btc    = self._round_qty(size_usd / fill_price)
            log.info(
                f"[BINANCE][DRY] BUY: {symbol} señal={price:.2f} fill={fill_price:.2f} "
                f"slip={slip:.3%} qty={qty_btc:.6f}BTC lat={latency_ms}ms"
            )
            return qty_btc, fill_price

        if self._exchange is None:
            log.error("[BINANCE] create_buy_order llamado sin exchange inicializado")
            return 0.0, price

        # Validar notional mínimo
        si = self._symbol_info
        if si and size_usd < si.min_notional:
            log.error(
                f"[BINANCE] BUY rechazado: ${size_usd:.2f} < "
                f"minNotional ${si.min_notional:.2f} para {symbol}"
            )
            return 0.0, price

        t_start = time.time()
        try:
            # quoteOrderQty: Binance entiende "gasta X USDT"
            order = await self._exchange.create_order(
                symbol=symbol,
                type="MARKET",
                side="buy",
                amount=None,
                price=None,
                params={"quoteOrderQty": size_usd},
            )
            latency_ms = (time.time() - t_start) * 1000

            # Extraer fill real (ccxt normaliza en 'average' y 'filled')
            fill_price = float(order.get("average") or order.get("price") or price)
            qty_btc    = float(order.get("filled") or 0.0)

            if qty_btc <= 0:
                log.warning(f"[BINANCE] BUY filled=0: {order.get('status')} id={order.get('id')}")
                return 0.0, price

            slip_pct = abs(fill_price - price) / price if price > 0 else 0.0
            if slip_pct > SLIP_MAX_PCT:
                log.warning(
                    f"[BINANCE] BUY slippage alto: {slip_pct:.3%} > max {SLIP_MAX_PCT:.3%} "
                    f"(señal={price:.2f} fill={fill_price:.2f}) — "
                    f"fill aceptado (orden ya ejecutada en exchange)"
                )

            log.info(
                f"[BINANCE] BUY FILLED: {symbol} señal={price:.2f} fill={fill_price:.2f} "
                f"slip={slip_pct:.3%} qty={qty_btc:.6f}BTC usdt={size_usd:.2f} "
                f"lat={latency_ms:.0f}ms orderId={order.get('id')}"
            )
            return qty_btc, fill_price

        except Exception as exc:
            log.error(f"[BINANCE] BUY error {symbol}: {exc}")
            return 0.0, price

    # ── Órdenes SELL ─────────────────────────────────────────────────────────

    async def create_sell_order(
        self,
        token_id: str,         # símbolo (ej. "BTCUSDT")
        price: float,          # precio referencia para slippage
        size_tokens: float,    # qty BTC a vender
        current_prices: Optional[Dict[str, float]] = None,
        side_key: str = "",
        max_retries: int = 3,
        entry_price: float = 0.0,
    ) -> bool:
        """
        MARKET SELL de qty BTC.

        En Binance Spot, un MARKET SELL es inmediato y garantiza ejecución
        (siempre que haya liquidez, que en BTC/USDT es prácticamente siempre).

        Returns:
            True si la orden fue ejecutada. False si falla definitivamente.
        """
        symbol  = token_id
        qty_btc = self._round_qty(size_tokens)

        # ── DRY RUN simulado ────────────────────────────────────────────────
        if DRY_RUN:
            latency_ms = random.randint(DRY_LATENCY_MIN, DRY_LATENCY_MAX)
            await asyncio.sleep(latency_ms / 1000)
            slip       = random.uniform(-DRY_SLIPPAGE_MAX, 0)
            fill_price = price * (1 + slip)
            pnl_pct    = (fill_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
            log.info(
                f"[BINANCE][DRY] SELL: {symbol} qty={qty_btc:.6f}BTC "
                f"fill={fill_price:.2f} PnL={pnl_pct:+.2f}% lat={latency_ms}ms"
            )
            return True

        if self._exchange is None:
            log.error("[BINANCE] create_sell_order llamado sin exchange inicializado")
            return False

        # Validar qty mínima
        si = self._symbol_info
        if si and qty_btc < si.min_qty:
            log.error(
                f"[BINANCE] SELL rechazado: qty {qty_btc:.8f} < "
                f"minQty {si.min_qty} para {symbol}"
            )
            return False

        for attempt in range(1, max_retries + 1):
            t_start = time.time()
            try:
                order = await self._exchange.create_order(
                    symbol=symbol,
                    type="MARKET",
                    side="sell",
                    amount=qty_btc,
                )
                latency_ms = (time.time() - t_start) * 1000
                fill_price = float(order.get("average") or order.get("price") or price)
                filled_qty = float(order.get("filled") or 0.0)

                if filled_qty > 0:
                    slip_pct = abs(fill_price - price) / price if price > 0 else 0.0
                    pnl_pct  = (fill_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                    log.info(
                        f"[BINANCE] SELL FILLED (intento {attempt}): {symbol} "
                        f"qty={filled_qty:.6f}BTC fill={fill_price:.2f} "
                        f"slip={slip_pct:.3%} PnL={pnl_pct:+.2f}% "
                        f"lat={latency_ms:.0f}ms id={order.get('id')}"
                    )
                    return True

                log.warning(
                    f"[BINANCE] SELL intento {attempt}/{max_retries}: "
                    f"filled=0 status={order.get('status')}"
                )

            except Exception as exc:
                exc_str = str(exc).lower()
                log.warning(f"[BINANCE] SELL intento {attempt}/{max_retries} error: {exc}")

                if any(s in exc_str for s in ("insufficient", "not enough", "account has insufficient")):
                    log.error("[BINANCE] Balance insuficiente en SELL — abortar reintentos")
                    return False

                if attempt < max_retries:
                    await asyncio.sleep(attempt * 0.5)

        log.error(
            f"[BINANCE] SELL DEFINITIVAMENTE FALLIDO tras {max_retries} intentos — "
            f"{symbol} qty={qty_btc:.6f}BTC — POSICIÓN ABIERTA EN EXCHANGE"
        )
        return False

    # ── Stop-Loss nativo ──────────────────────────────────────────────────────

    async def place_stop_loss(
        self,
        symbol: str,
        qty: float,
        stop_price: float,
        limit_price: Optional[float] = None,
    ) -> str:
        """
        Coloca una orden STOP_LOSS_LIMIT en Binance.

        Binance dispara la orden cuando el precio toca stop_price.
        Entonces envía una orden LIMIT al precio limit_price.
        Si limit_price es None, se calcula como stop_price * (1 - SL_LIMIT_BUFFER).

        Args:
            symbol:      Ej. "BTCUSDT"
            qty:         Cantidad BTC a vender en el stop
            stop_price:  Precio que activa la orden (trigger price)
            limit_price: Precio límite de ejecución (debe ser <= stop_price para SELL)

        Returns:
            order_id como str si éxito. "" si falla o no disponible.

        Nota sobre slippage en SL:
            Un STOP_LOSS_LIMIT puede no ejecutarse si el precio cae VERY rápidamente
            (gap). En ese caso Binance cancela la orden y la posición queda abierta.
            Para evitar esto, limit_price debe ser suficientemente bajo respecto al
            stop_price. SL_LIMIT_BUFFER=0.3% es un buen balance para BTC.
        """
        qty_r   = self._round_qty(qty)
        stop_r  = self._round_price(stop_price)
        limit_r = self._round_price(
            limit_price if limit_price is not None
            else stop_price * (1 - SL_LIMIT_BUFFER)
        )

        if DRY_RUN:
            fake_id = f"dry-sl-{int(time.time())}"
            log.info(
                f"[BINANCE][DRY] STOP_LOSS_LIMIT: {symbol} qty={qty_r:.6f}BTC "
                f"stop={stop_r:.2f} limit={limit_r:.2f} id={fake_id}"
            )
            return fake_id

        if self._exchange is None:
            return ""

        si = self._symbol_info
        if si and "STOP_LOSS_LIMIT" not in si.allowed_order_types:
            log.warning(
                f"[BINANCE] STOP_LOSS_LIMIT no disponible para {symbol} — "
                f"la estrategia debe monitorear el SL manualmente"
            )
            return ""

        try:
            order = await self._exchange.create_order(
                symbol=symbol,
                type="STOP_LOSS_LIMIT",
                side="sell",
                amount=qty_r,
                price=limit_r,
                params={"stopPrice": stop_r},
            )
            order_id = str(order.get("id", ""))
            log.info(
                f"[BINANCE] STOP_LOSS_LIMIT colocado: {symbol} qty={qty_r:.6f}BTC "
                f"triggerStop={stop_r:.2f} execLimit={limit_r:.2f} "
                f"(buffer={SL_LIMIT_BUFFER:.1%}) orderId={order_id}"
            )
            return order_id

        except Exception as exc:
            log.error(f"[BINANCE] Error colocando STOP_LOSS_LIMIT {symbol}: {exc}")
            return ""

    def compute_stop_prices(
        self,
        fill_price: float,
        sl_drop_pct: Optional[float] = None,
        buffer_pct: Optional[float] = None,
    ) -> Tuple[float, float]:
        """
        Calcula stop_price y limit_price para un STOP_LOSS_LIMIT.

        Args:
            fill_price:  Precio de entrada (fill del BUY)
            sl_drop_pct: % de caída desde entrada (default: SL_DROP_PCT del .env)
            buffer_pct:  % por debajo del stop para el limit (default: SL_LIMIT_BUFFER)

        Returns:
            (stop_price, limit_price) redondeados al tickSize del símbolo.

        Ejemplo con BTC fill=95000, sl_drop=1%, buffer=0.3%:
            stop_price  = 95000 * (1 - 0.01) = 94050
            limit_price = 94050 * (1 - 0.003) = 93768
        """
        drop   = sl_drop_pct if sl_drop_pct is not None else SL_DROP_PCT
        buf    = buffer_pct  if buffer_pct  is not None else SL_LIMIT_BUFFER
        stop   = fill_price * (1 - drop)
        limit  = stop * (1 - buf)
        return self._round_price(stop), self._round_price(limit)

    # ── Cancelación y consulta de órdenes ────────────────────────────────────

    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """
        Cancela una orden abierta (ej. el stop-loss cuando se dispara el TP).

        Si la orden ya fue ejecutada o no existe, Binance devuelve error
        'Unknown order' — este caso se trata como éxito (la orden ya no está abierta).

        Returns:
            True si la orden fue cancelada o ya no existía.
            False si hubo un error real al cancelar.
        """
        if DRY_RUN:
            log.info(f"[BINANCE][DRY] cancel_order: {order_id} ({symbol})")
            return True

        if not order_id or self._exchange is None:
            return False

        try:
            await self._exchange.cancel_order(order_id, symbol)
            log.info(f"[BINANCE] Orden cancelada: {order_id} ({symbol})")
            return True
        except Exception as exc:
            exc_str = str(exc).lower()
            if any(s in exc_str for s in ("unknown order", "order does not exist", "-2011")):
                # La orden ya fue ejecutada (SL disparado) o cancelada — no es error
                log.info(
                    f"[BINANCE] Orden {order_id} ya no existe "
                    f"(ejecutada o cancelada previamente) — OK"
                )
                return True
            log.warning(f"[BINANCE] Error cancelando orden {order_id} ({symbol}): {exc}")
            return False

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> bool:
        """
        Cancela todas las órdenes abiertas para el símbolo.
        Usado por el Circuit Breaker en emergencias.
        """
        if DRY_RUN:
            log.info(f"[BINANCE][DRY] cancel_all_orders simulado para {symbol or self._symbol}")
            return True

        if self._exchange is None:
            return False

        sym = symbol or self._symbol
        try:
            await self._exchange.cancel_all_orders(sym)
            log.info(f"[BINANCE] cancel_all_orders ejecutado para {sym}")
            return True
        except Exception as exc:
            log.error(f"[BINANCE] Error en cancel_all_orders ({sym}): {exc}")
            return False

    async def get_order_status(self, order_id: str, symbol: str) -> dict:
        """
        Consulta el estado actual de una orden.

        Returns:
            dict ccxt normalizado con 'status': 'open' | 'closed' | 'canceled' | 'unknown'

        Usos típicos:
            - Verificar si el stop-loss fue disparado por Binance
            - Reconciliación de posiciones al reiniciar
        """
        if DRY_RUN:
            return {"id": order_id, "status": "open", "symbol": symbol}

        if not order_id or self._exchange is None:
            return {"id": order_id, "status": "unknown"}

        try:
            order = await self._exchange.fetch_order(order_id, symbol)
            log.debug(
                f"[BINANCE] Orden {order_id}: status={order.get('status')} "
                f"filled={order.get('filled')}"
            )
            return order
        except Exception as exc:
            exc_str = str(exc).lower()
            if any(s in exc_str for s in ("unknown order", "-2011")):
                return {"id": order_id, "status": "canceled", "symbol": symbol}
            log.debug(f"[BINANCE] Error consultando orden {order_id}: {exc}")
            return {"id": order_id, "status": "unknown", "error": str(exc)}

    async def fetch_open_orders(self, symbol: Optional[str] = None) -> list:
        """
        Retorna todas las órdenes abiertas para el símbolo.
        Útil para reconciliación al reiniciar el bot.
        """
        if DRY_RUN or self._exchange is None:
            return []

        sym = symbol or self._symbol
        try:
            orders = await self._exchange.fetch_open_orders(sym)
            log.info(f"[BINANCE] Órdenes abiertas en {sym}: {len(orders)}")
            return orders
        except Exception as exc:
            log.warning(f"[BINANCE] Error consultando órdenes abiertas ({sym}): {exc}")
            return []

    async def fetch_ticker(self, symbol: Optional[str] = None) -> Optional[float]:
        """
        Retorna el precio mid actual del símbolo (último precio).
        Útil para verificar precio antes de cerrar posición.
        """
        if DRY_RUN or self._exchange is None:
            return None

        sym = symbol or self._symbol
        try:
            ticker = await self._exchange.fetch_ticker(sym)
            return float(ticker.get("last") or ticker.get("close") or 0)
        except Exception as exc:
            log.warning(f"[BINANCE] Error consultando ticker ({sym}): {exc}")
            return None

    # ── Reconciliación ────────────────────────────────────────────────────────

    async def reconcile_open_positions(
        self,
        tracked_positions: Dict[str, dict],
    ) -> Dict[str, dict]:
        """
        Verifica el estado real de posiciones trackeadas contra Binance.

        Detecta:
          - Posiciones fantasma: en tracker pero SL ya ejecutado en Binance
          - Posiciones sin stop: en tracker sin stop_order_id válido

        Args:
            tracked_positions: {position_id: {symbol, size_tokens, stop_order_id, ...}}

        Returns:
            Dict con las posiciones que siguen activas (sin las fantasma).
        """
        if DRY_RUN or not tracked_positions:
            return tracked_positions

        active = {}
        for pos_id, pos in tracked_positions.items():
            stop_id = pos.get("stop_order_id", "")
            symbol  = pos.get("token_id", self._symbol)

            if not stop_id:
                # Sin stop_order_id — asumir activa y registrar advertencia
                log.warning(
                    f"[BINANCE][RECONCILE] Posición {pos_id} sin stop_order_id — "
                    f"verificar manualmente"
                )
                active[pos_id] = pos
                continue

            status_data = await self.get_order_status(stop_id, symbol)
            status      = status_data.get("status", "unknown")

            if status in ("closed", "filled"):
                log.warning(
                    f"[BINANCE][RECONCILE] Posición {pos_id}: stop {stop_id} EJECUTADO "
                    f"— posición cerrada por Binance. Eliminando del tracker."
                )
                # No incluir en active — ya cerrada por el SL
            elif status == "canceled":
                log.warning(
                    f"[BINANCE][RECONCILE] Posición {pos_id}: stop {stop_id} CANCELADO "
                    f"— sin SL activo. Recolocar o cerrar manualmente."
                )
                active[pos_id] = {**pos, "stop_order_id": "", "sl_missing": True}
            else:
                # "open" o "unknown" — considerar activa
                active[pos_id] = pos

        removed = len(tracked_positions) - len(active)
        if removed > 0:
            log.info(f"[BINANCE][RECONCILE] {removed} posiciones cerradas por SL eliminadas del tracker")

        return active

    # ── Cierre de sesión ──────────────────────────────────────────────────────

    async def close(self) -> None:
        """Cierra la sesión ccxt y libera recursos de red."""
        if self._exchange is not None:
            try:
                await self._exchange.close()
                log.info("[BINANCE] Sesión ccxt cerrada correctamente")
            except Exception:
                pass
