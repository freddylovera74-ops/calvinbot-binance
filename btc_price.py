"""
btc_price.py — Feed de precio BTC/USDT desde Binance API pública.

Polling REST cada 2 segundos. No requiere autenticación.
Calcula momentum: variación % del precio en ventana deslizante.

El momentum es la ventaja real sobre el 50% WR del sniper anterior:
  BTC sube +0.2% en 60s → el token UP de Polymarket debería seguir
  → entramos antes de que el crowd actualice sus órdenes en el CLOB
"""

import asyncio
import time
from collections import deque
from typing import Optional, Tuple

import aiohttp

def info(msg: str) -> None:  print(msg)
def warn(msg: str) -> None:  print(f"[WARN] {msg}")
def debug(msg: str) -> None: pass  # silenciado en producción

BINANCE_PRICE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
POLL_INTERVAL_S   = 2.0    # segundos entre consultas REST
HISTORY_WINDOW_S  = 180    # segundos de historia a mantener

# ── Estado compartido ──────────────────────────────────────────────────────────
_price_history: deque        = deque()  # [(timestamp, btc_price), ...]
_last_price:    Optional[float] = None
_connected:     bool            = False


async def run_btc_poller() -> None:
    """Task principal: polling del precio BTC/USDT en Binance cada POLL_INTERVAL_S.

    Mantiene _price_history con los últimos HISTORY_WINDOW_S segundos de datos.
    Se ejecuta en paralelo con las demás tasks de CalvinBot.
    """
    global _last_price, _connected

    info("[BTC] Iniciando feed de precio BTC/USDT (Binance REST)...")

    connector = aiohttp.TCPConnector(ssl=True)
    timeout   = aiohttp.ClientTimeout(total=4)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        while True:
            try:
                async with session.get(BINANCE_PRICE_URL) as resp:
                    data  = await resp.json(content_type=None)
                    price = float(data["price"])
                    ts    = time.time()

                    _price_history.append((ts, price))
                    _last_price = price
                    _connected  = True

                    # Elimina datos más antiguos de la ventana + 10s de margen
                    cutoff = ts - HISTORY_WINDOW_S - 10
                    while _price_history and _price_history[0][0] < cutoff:
                        _price_history.popleft()

                    debug(f"[BTC] ${price:,.2f}")

            except Exception as exc:
                _connected = False
                warn(f"[BTC] Error Binance: {exc}")

            await asyncio.sleep(POLL_INTERVAL_S)


def get_momentum(window_s: float = 60.0) -> Tuple[float, Optional[str]]:
    """Calcula el momentum de precio BTC en los últimos window_s segundos.

    Retorna (pct_change, direction):
      pct_change: % de cambio (positivo=sube, negativo=baja)
      direction:  'UP', 'DOWN', o None si no hay suficiente historia o es flat

    Ejemplo:
      BTC: $80,000 hace 60s → $80,240 ahora → (+0.30%, 'UP')
    """
    if len(_price_history) < 3 or _last_price is None:
        return 0.0, None

    now    = time.time()
    cutoff = now - window_s

    # Precio más antiguo dentro de la ventana
    old_price: Optional[float] = None
    for ts, price in _price_history:
        if ts >= cutoff:
            old_price = price
            break

    if old_price is None:
        return 0.0, None

    pct_change = (_last_price - old_price) / old_price * 100.0

    if abs(pct_change) < 0.001:
        return pct_change, None  # completamente flat

    direction = "UP" if pct_change > 0 else "DOWN"
    return pct_change, direction


def get_btc_price() -> Optional[float]:
    """Retorna el último precio conocido de BTC/USDT."""
    return _last_price


def is_connected() -> bool:
    """True si la última consulta a Binance tuvo éxito."""
    return _connected


def has_enough_history(min_window_s: float = 30.0) -> bool:
    """True si hay suficiente historia acumulada para calcular momentum."""
    if len(_price_history) < 5:
        return False
    oldest = _price_history[0][0]
    return (time.time() - oldest) >= min_window_s
