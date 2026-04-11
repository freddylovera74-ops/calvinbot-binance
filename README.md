# CalvinBTC — Sistema de Trading Automático BTC Spot

> **Exchange:** Binance Spot (Testnet / Real)
> **Par:** BTCUSDT
> **Estrategia:** BTC Momentum Sniper
> **Modo:** Paper trading por defecto (`DRY_RUN=true`). Real con `DRY_RUN=false`.

---

## ¿Qué hace este sistema?

CalvinBot Binance es un sistema de trading algorítmico que opera **BTCUSDT en Binance Spot** usando momentum de precio. Detecta movimientos fuertes de BTC en ventanas cortas y abre posiciones largas con Take Profit y Stop Loss gestionados mediante órdenes nativas de Binance.

El sistema está compuesto por **4 bots independientes** que se supervisan entre sí y se auto-regulan con el tiempo.

---

## Arquitectura — Los 4 componentes

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        CALVINBOT BINANCE SYSTEM                          │
│                                                                          │
│  ┌──────────────────┐   Redis Stream    ┌──────────────────┐             │
│  │ strategy_binance │ ──── signals ───► │  execution_bot   │ ──► Binance │
│  │  🧠 Cerebro       │ ◄─── fills ────── │  💪 Músculo       │  Spot API  │
│  └────────┬─────────┘                   └──────────────────┘             │
│           │                                                              │
│  ┌────────▼──────────────────┐    ┌──────────────────────┐               │
│  │ dynamic_optimizer_binance │    │  watchdog_binance    │               │
│  │  🔬 Científico (30 min)    │    │  🐕 Vigilante (20s)  │               │
│  └───────────────────────────┘    └──────────────────────┘               │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────┐            │
│  │  dashboard_server.py  →  http://servidor:8084            │            │
│  │  📊 Dashboard dark TradingView-style en tiempo real       │            │
│  └──────────────────────────────────────────────────────────┘            │
└──────────────────────────────────────────────────────────────────────────┘
```

### 🧠 strategy_binance.py — El Cerebro

- Lee el precio de BTC en tiempo real desde Binance WebSocket
- Evalúa momentum cada segundo: si BTC sube ≥ `BTC_MIN_PCT`% en `BTC_WINDOW` velas → señal BUY
- Publica señales via **Redis Stream** (`signals:execution`) al Músculo
- Gestiona posiciones abiertas: monitorea TP y SL en tiempo real
- En arranque, reconcilia posiciones abiertas contra la API de Binance (elimina fantasmas)
- Cada 60s recarga parámetros dinámicos desde Redis (`config:strategy_binance:current`)
- Publica heartbeat en `state:strategy:latest` y guarda historial en `binance_trades.csv`

### 💪 execution_bot.py — El Músculo

- Escucha la Redis Stream en busca de señales de compra/venta
- En modo REAL: llama a la API de Binance para ejecutar órdenes de mercado
- En modo DRY: simula fills con precio actual sin tocar dinero real
- Gestiona stop-loss via órdenes nativas STOP_LOSS_LIMIT en Binance
- Publica heartbeat en `state:executor:latest` con saldo USDT actualizado (caché 30s)

### 🔬 dynamic_optimizer_binance.py — El Científico

- Corre cada 30 minutos analizando `binance_trades.csv`
- Requiere mínimo **15 trades** para empezar a ajustar
- Calcula un score: `win_rate/50 × 0.6 + profit_factor_norm × 0.4`
- Si score > 1.15 → aumenta agresividad +5%; si score < 0.85 → la reduce -5%
- Aplica suavizado 30%: los cambios son graduales, nunca bruscos
- No modifica TP/SL si hay posiciones abiertas (evita inconsistencias con órdenes en Binance)
- Publica parámetros en Redis (`config:strategy_binance:current`) → el Cerebro los aplica sin reiniciar

### 🐕 watchdog_binance.py — El Vigilante

- Comprueba heartbeats cada 20 segundos
- Si un bot lleva >60s offline → alerta Telegram WARNING
- Si un bot lleva >120s offline → reinicio automático vía `systemctl` + alerta CRITICAL
- Máximo 3 reinicios por hora (protección contra crash loop)
- Si ambos bots caen simultáneamente → reinicio de emergencia inmediato
- Monitorea límites de pérdida via `loss_tracker` y drawdown de sesión
- Si drawdown ≥ $80 → publica comando PAUSE al executor vía Redis

---

## Cómo toma las decisiones

### Checklist de entrada (todos deben pasar)

```
¿Es horario de trading (en_trading_hours)?              ✓ / ✗
¿El executor no está en PAUSE?                          ✓ / ✗
¿No se superó el límite de pérdida diaria?              ✓ / ✗
¿Hay menos de MAX_POS posiciones abiertas?              ✓ / ✗
¿Han pasado ≥ THROTTLE segundos desde la última entrada? ✓ / ✗
¿BTC tiene momentum ≥ BTC_MIN_PCT% en BTC_WINDOW velas? ✓ / ✗
→ Si todo OK: señal BUY → Redis Stream → execution_bot
```

### Gestión de la posición

Una vez abierta, el Cerebro monitorea cada segundo:

| Condición | Acción |
|-----------|--------|
| Precio subió ≥ TP% desde entrada | Cierre por Take Profit |
| Precio cayó ≥ SL% desde entrada | Cierre por Stop Loss |
| Posición abierta ≥ MAX_HOLD segundos | Cierre por tiempo máximo |
| Señal de PAUSE del watchdog | Cierre inmediato de todo |

---

## Parámetros de riesgo (nivel inicial 7.5/10)

| Parámetro | Valor | Qué controla |
|-----------|-------|--------------|
| `STAKE_USD` | $75 | USDT por operación |
| `BTC_MIN_PCT` | 0.08% | Momentum mínimo de BTC requerido |
| `BTC_WINDOW` | 20 velas | Ventana para medir momentum |
| `TP` | 2.0% | Take Profit |
| `SL` | 0.8% | Stop Loss |
| `MAX_POS` | 2 | Posiciones simultáneas máximas |
| `THROTTLE_S` | 15s | Segundos mínimos entre entradas |
| `MAX_HOLD_S` | 240s | Tiempo máximo de una posición abierta |
| `DAILY_LOSS` | $100 | Pérdida diaria máxima antes de parar |

El optimizador puede ajustar todos estos parámetros dentro de los siguientes límites:

| Parámetro | Mínimo | Máximo |
|-----------|--------|--------|
| `STAKE_USD` | $20 | $200 |
| `BTC_MIN_PCT` | 0.05% | 0.30% |
| `BTC_WINDOW` | 15 velas | 60 velas |
| `TP` | 1.0% | 4.0% |
| `SL` | 0.5% | 2.0% |

---

## Circuit Breakers (protecciones de riesgo)

```
Nivel 1: Throttle entre trades (THROTTLE_S = 15s)
  → Mínimo 15s entre entradas consecutivas

Nivel 2: Posiciones simultáneas (MAX_POS = 2)
  → Máximo 2 posiciones abiertas al mismo tiempo

Nivel 3: Drawdown de sesión (DRAWDOWN_KILL = $80)
  → Watchdog pausa el executor si la sesión pierde ≥ $80

Nivel 4: Pérdida diaria (DAILY_LOSS = $100)
  → La estrategia se detiene si la pérdida diaria supera $100

Nivel 5: Crash loop protection (MAX_RESTARTS_PER_H = 3)
  → El watchdog no reinicia más de 3 veces por hora
```

---

## Cómo arrancar

### Requisitos previos

```bash
# Redis corriendo
redis-server

# Dependencias
pip install -r requirements.txt

# Variables de entorno (.env)
BINANCE_API_KEY=...         # API key de Binance (Testnet o Real)
BINANCE_API_SECRET=...      # Secret de Binance
BINANCE_TESTNET=true        # false para real
DRY_RUN=true                # false para operar con dinero real
REDIS_URL=redis://localhost:6379
TELEGRAM_BOT_TOKEN=...      # Para alertas
TELEGRAM_CHAT_ID=...
```

### Arranque en producción (servidor Linux con systemd)

```bash
# Copiar servicios
cp systemd/*.service /etc/systemd/system/
systemctl daemon-reload

# Activar y arrancar todos
systemctl enable calvinbot-strategy calvinbot-executor calvinbot-optimizer calvinbot-watchdog calvinbot-dashboard
systemctl start  calvinbot-strategy calvinbot-executor calvinbot-optimizer calvinbot-watchdog calvinbot-dashboard

# Ver estado
systemctl is-active calvinbot-strategy calvinbot-executor calvinbot-optimizer calvinbot-watchdog calvinbot-dashboard

# Ver logs en tiempo real
journalctl -fu calvinbot-strategy
tail -f /opt/calvinbot-binance/strategy_binance.log
```

### Deploy de actualizaciones

```bash
# En el servidor — pull y reinicio automático
cd /opt/calvinbot-binance && bash deploy.sh

# O manualmente
git pull && systemctl restart calvinbot-strategy calvinbot-executor calvinbot-watchdog calvinbot-optimizer calvinbot-dashboard
```

### Dashboard

```
http://TU_IP_SERVIDOR:8084
```

---

## Archivos del sistema

| Archivo | Qué es |
|---------|--------|
| `strategy_binance.py` | Bot principal — señales y gestión de posiciones |
| `execution_bot.py` | Ejecutor de órdenes en Binance |
| `binance_exchange.py` | Adaptador para la API de Binance Spot |
| `dynamic_optimizer_binance.py` | Optimizador rule-based de parámetros |
| `watchdog_binance.py` | Supervisor con reinicio automático |
| `dashboard_server.py` | Servidor web del dashboard (puerto 8084) |
| `loss_tracker.py` | Módulo de control de pérdida diaria |
| `utils.py` | Utilidades compartidas (Telegram, horarios) |
| `deploy.sh` | Script de despliegue automático |
| `systemd/` | Archivos de servicio para systemd |
| `binance_trades.csv` | Historial de todos los trades |
| `binance_open.json` | Posiciones abiertas actuales |
| `.env` | Credenciales y config (no compartir) |

---

## Redis — Claves importantes

| Clave | Tipo | Descripción |
|-------|------|-------------|
| `state:strategy:latest` | String (JSON) | Heartbeat del Cerebro (ISO timestamp) |
| `state:executor:latest` | String (JSON) | Heartbeat del Músculo (float timestamp) |
| `state:watchdog:latest` | String (JSON) | Heartbeat del Vigilante |
| `signals:execution` | Stream | Canal de señales BUY/SELL Cerebro→Músculo |
| `fills:strategy` | Stream | Canal de fills Músculo→Cerebro |
| `config:strategy_binance:current` | String (JSON) | Parámetros activos (escrito por el optimizador) |
| `emergency:commands` | PubSub | Canal para comandos PAUSE/RESUME del watchdog |

---

## Glosario

| Término | Significado |
|---------|-------------|
| **Momentum** | Velocidad y dirección del movimiento de BTC en una ventana de tiempo |
| **TP (Take Profit)** | % de ganancia al que se cierra la posición automáticamente |
| **SL (Stop Loss)** | % de pérdida al que se cierra la posición para limitar el daño |
| **DRY RUN** | Modo simulado, sin dinero real, fills a precio de mercado |
| **PnL** | Profit and Loss — ganancias y pérdidas acumuladas |
| **Drawdown** | Caída desde el máximo de PnL de la sesión |
| **Win Rate** | % de trades que terminaron en ganancia |
| **Profit Factor** | Ganancias totales ÷ Pérdidas totales (>1 = rentable) |
| **Heartbeat** | Estado que cada bot publica en Redis cada pocos segundos para confirmar que vive |
| **Redis Stream** | Cola de mensajes persistente usada para comunicar señales entre bots |
| **STOP_LOSS_LIMIT** | Orden nativa de Binance que activa un límite cuando el precio cae al nivel de stop |
| **Testnet** | Entorno de pruebas de Binance con dinero ficticio (misma API, sin riesgo real) |
