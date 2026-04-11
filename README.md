# CalvinBot — Sistema de Trading Automático para Polymarket

> **Mercados:** BTC Up/Down 5 minutos en Polymarket
> **Estrategia:** BTC Momentum Sniper
> **Modo:** Paper trading por defecto (DRY RUN). Real money con `--real`.

---

## ¿Qué hace este sistema?

CalvinBot es un sistema de trading automatizado que apuesta en mercados de predicción de **Polymarket**. Concretamente opera en los mercados del tipo:

> *"¿Estará Bitcoin por encima de X precio dentro de 5 minutos?"*

Cada ronda dura exactamente 5 minutos. Al final:
- El token **UP** vale 1 USDC si BTC subió → resuelve en 1.00
- El token **DOWN** vale 1 USDC si BTC bajó → resuelve en 1.00
- El perdedor vale 0 USDC

El bot **no predice el futuro**. Lo que hace es aprovechar el **momentum de BTC**: si BTC está subiendo con fuerza en los últimos segundos de la ronda, compra tokens UP baratos (ej. 0.70¢) esperando que sigan subiendo hacia 1.00¢ o que el mercado resuelva a su favor.

---

## Arquitectura — Los 5 componentes

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           CALVINBOT SYSTEM                              │
│                                                                         │
│  ┌─────────────┐    Redis     ┌──────────────┐    Polymarket CLOB       │
│  │  calvin5.py │ ──signals──► │execution_bot │ ──────────────────────►  │
│  │  🧠 Cerebro │ ◄──fills──── │ 💪 Músculo   │                          │
│  └──────┬──────┘              └──────────────┘                          │
│         │                                                               │
│  ┌──────▼──────┐              ┌──────────────┐                          │
│  │  btc_price  │              │  watchdog.py │                          │
│  │  📡 BTC feed│              │  🐕 Vigilante│                          │
│  └─────────────┘              └──────────────┘                          │
│                                                                         │
│  ┌─────────────────────┐      ┌──────────────────────┐                  │
│  │  dynamic_optimizer  │      │   dashboard_server   │                  │
│  │  🔬 Científico       │      │   📊 Vista en web    │                  │
│  └─────────────────────┘      └──────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### 🧠 calvin5.py — El Cerebro (Calculador de señales)
- Monitorea los mercados BTC 5min en Polymarket via WebSocket
- Lee el precio de BTC en tiempo real
- Evalúa cada segundo si las condiciones son buenas para entrar
- Publica señales de BUY/SELL a Redis para que el Músculo las ejecute
- Gestiona las posiciones abiertas (TP, SL, hold a resolución)

### 💪 execution_bot.py — El Músculo (Ejecutor de órdenes)
- Escucha señales de Redis
- Llama a la API real de Polymarket (CLOB) para comprar/vender
- En modo DRY RUN → simula el fill sin tocar dinero real
- Devuelve el recibo del fill al Cerebro

### 🔬 dynamic_optimizer.py — El Científico (Optimizador)
- Analiza el historial de trades en `calvin5_trades.csv`
- Usa **Optimización Bayesiana** (Optuna) para encontrar los mejores parámetros
- Publica los parámetros mejorados via Redis → el Cerebro los aplica sin reiniciar

### 🐕 watchdog.py — El Vigilante (Supervisor)
- Comprueba que todos los bots siguen vivos (heartbeats)
- Reinicia automáticamente los que hayan caído
- Alerta via Redis si algo va mal

### 📊 dashboard_server.py — La Vista (Web dashboard)
- Servidor web en `http://localhost:8081`
- Muestra el estado en tiempo real de todos los bots
- Panel de control con kill switches por bot
- Consola de logs unificada

---

## Cómo toma las decisiones

### La ventana sniper (¿cuándo mira?)

El bot **no opera durante toda la ronda**. Solo activa la búsqueda en los **últimos 60 segundos** de cada ronda de 5 minutos.

```
Ronda de 5 min (300s):
│←─── NO ACTIVO (primeros 240s) ───────────►│←── SNIPER (60s) ──►│
0s                                          240s               300s
```

Esto es intencional: en los primeros minutos hay mucha incertidumbre. En los últimos 60 segundos, el mercado ya tiene más información y el precio de los tokens refleja mejor la probabilidad real.

### La checklist de entrada (todas deben pasar)

Cada segundo dentro de la ventana sniper, el bot evalúa esta lista de filtros:

```
¿Estamos en la ventana sniper (últimos 60s)?          ✓ / ✗
¿Es horario de trading (14:00-22:00 hora España)?     ✓ / ✗
¿El Dashboard no ha pausado las entradas?             ✓ / ✗
¿No se superó el límite de pérdida diaria?            ✓ / ✗
¿No se activó el circuit breaker de sesión?           ✓ / ✗
¿Hay menos de MAX_OPEN_POS posiciones abiertas?       ✓ / ✗
¿No se superó MAX_ENTRIES_PER_ROUND esta ronda?       ✓ / ✗
¿Han pasado ≥ THROTTLE_S segundos desde la última entrada? ✓ / ✗
¿Quedan más de ENTRY_CUTOFF_S segundos (no demasiado tarde)? ✓ / ✗

Para CADA LADO (UP y DOWN):
  ¿El precio está en ENTRY_MIN–ENTRY_MAX?             ✓ / ✗
  ¿El precio ha sido estable N scans?                 ✓ / ✗
  ¿BTC tiene momentum ≥ BTC_MIN_PCT% en BTC_WINDOW_S? ✓ / ✗
  ¿La dirección de BTC coincide con UP o DOWN?        ✓ / ✗
  → Si todo OK: ¡ENTRADA!
```

### Los filtros de precio — ¿Por qué 0.62–0.90?

```
0.00  0.50  0.62        0.90  1.00
  │     │    │            │    │
  │  Too │  ZONA DE    │ Too │
  │  cheap│  ENTRADA   │ exp │
  │  risky│            │ ensive│
```

- **< 0.62 (muy barato):** El mercado no cree que vaya a ganar. Demasiado riesgo.
- **0.62–0.90 (zona óptima):** Precio razonable con buen potencial de ganancia.
- **> 0.90 (muy caro):** Poca ganancia potencial si gana, pérdida grande si pierde.

### El momentum de BTC — La señal clave

```
BTC: ─────────────────/────────────────────
                     ↗
                  momentum +0.15% en 10s
                  ↓
                  Condición: ≥ 0.10% en 10s
                  → SEÑAL UP VÁLIDA
```

Si BTC sube ≥ 0.10% en los últimos 10 segundos → señal **UP**
Si BTC baja ≥ 0.10% en los últimos 10 segundos → señal **DOWN**

### Cuándo cerrar la posición (salidas)

Una vez dentro, el bot monitorea el precio cada segundo y sale en el primero que se cumpla:

| Condición | Nombre | Qué significa |
|-----------|--------|---------------|
| Precio subió ≥ 25% desde entrada | **TP Full** | Ganancia grande, salida total |
| Precio subió ≥ 9% y queda tiempo | **TP Mid** | Ganancia parcial, asegurar |
| En últimos 15s, precio subió ≥ 4% | **TP Last** | Antes de resolución incierta |
| Quedan ≤ 15s y precio ≥ 0.80 | **Hold** | Dejar resolver en 1.00 |
| Precio cayó 7 cents desde entrada | **SL Drop** | Stop loss por caída |
| Precio < 0.45 | **SL Floor** | BTC revirtió fuerte, salir ya |
| Holding ≥ 230s sin recuperarse | **SL Time** | Llevas mucho tiempo perdiendo |
| La ronda termina | **Round End** | Resolución automática (0% fee) |

> **Nota importante sobre fees:** Polymarket cobra **3% del USDC recibido** en ventas anticipadas (TP/SL). Si el mercado resuelve solo (round_end), la fee es **0%**. Esto significa que el precio de break-even real es más alto que el precio de entrada.

---

## Parámetros configurables

Todos los parámetros que el optimizador puede ajustar:

### Filtros de entrada

| Parámetro | Default | Qué controla |
|-----------|---------|--------------|
| `ENTRY_MIN` | 0.62 | Precio mínimo para entrar |
| `ENTRY_MAX` | 0.90 | Precio máximo para entrar |
| `ENTRY_CUTOFF_S` | 40s | No entrar si quedan menos de N segundos |
| `ENTRY_MAX_LATE` | 0.90 | ENTRY_MAX más restrictivo cuando queda poco tiempo |
| `PRICE_STABILITY_SCANS` | 2 | Precio debe estar en rango N scans consecutivos |

### Momentum de BTC

| Parámetro | Default | Qué controla |
|-----------|---------|--------------|
| `BTC_MIN_PCT` | 0.10% | Mínimo de movimiento de BTC requerido |
| `BTC_WINDOW_S` | 10s | Ventana de tiempo para medir el momentum |

### Take Profit (salidas ganadoras)

| Parámetro | Default | Qué controla |
|-----------|---------|--------------|
| `TP_FULL_PCT` | 25% | Ganancia para salida total |
| `TP_MID_PCT` | 9% | Ganancia para salida parcial |
| `TP_LAST_15S_PCT` | 4% | Ganancia en los últimos 15s para salir |
| `HOLD_SECS` | 15s | Si quedan ≤ N segundos y precio ≥ 0.80, esperar resolución |
| `HOLD_BTC_PCT` | 0.15% | BTC mínimo para mantener hold a resolución |

### Stop Loss (salidas perdedoras)

| Parámetro | Default | Qué controla |
|-----------|---------|--------------|
| `SL_DROP` | 0.07 | Salir si precio cae 7 cents desde entrada |
| `SL_FLOOR` | 0.45 | Floor absoluto de precio (BTC revirtió) |
| `SL_TIME_S` | 230s | Salir si llevas ≥ N segundos sin recuperarte |
| `SL_LAST_30S_PCT` | 5% | En últimos 30s, SL relativo desde peak |

### Gestión de riesgo

| Parámetro | Default | Qué controla |
|-----------|---------|--------------|
| `STAKE_USD` | $5.00 | Dólares por operación |
| `MAX_OPEN_POS` | 1 | Máximo de posiciones abiertas simultáneas |
| `MAX_ENTRIES_PER_ROUND` | 2 | Máximo de entradas por ronda de 5 min |
| `THROTTLE_S` | 2s | Segundos mínimos entre entradas consecutivas |
| `SLIP_MAX_PCT` | 4% | Slippage máximo tolerado |
| `FOK_PRICE_BUF` | 3% | Buffer de precio en la orden límite |

---

## El optimizador — ¿Cómo aprende?

El optimizador funciona como un **científico que hace experimentos**:

1. **Lee el historial** de trades en `calvin5_trades.csv`
2. **Hace pruebas** (50-150 trials por ciclo): "¿Qué habría pasado si ENTRY_MIN fuera 0.65 en lugar de 0.62?"
3. **Simula** qué trades habrían ocurrido con esos parámetros
4. **Puntúa** cada combinación con un score que combina:
   - **Win Rate** (% de trades ganadores) — peso 25%
   - **Sharpe Ratio** (consistencia riesgo/retorno) — peso 40%
   - **Profit Factor** (ganancias totales / pérdidas totales) — peso 20%
   - **PnL medio por trade** — peso 15%
   - **Penalización por drawdown** (pérdida máxima acumulada)
5. **Publica los mejores parámetros** via Redis → Calvin5 los aplica en vivo sin reiniciar

```
Ciclo del optimizador (cada 4 horas):
  Leer CSV → Simular 100 combinaciones → Aplicar suavizado 30% → Publicar
       ↑                                                              ↓
  Nuevos trades                                              calvin5.py en vivo
```

**Suavizado del 30%:** El optimizador no aplica cambios bruscos. Si encuentra que ENTRY_MIN debería ser 0.70 y actualmente es 0.62, aplica: `0.62 + (0.70 - 0.62) × 0.30 = 0.644`. Esto evita sobrerreaccionar a una mala racha temporal.

---

## Circuit Breakers (protecciones de riesgo)

El sistema tiene **4 niveles de protección** automática:

```
Nivel 1: Throttle entre trades (THROTTLE_S)
  → Mínimo 2s entre entradas para evitar trades duplicados

Nivel 2: Límites por ronda (MAX_ENTRIES_PER_ROUND)
  → Máximo 2 entradas por ronda de 5 minutos

Nivel 3: Session Drawdown (SESSION_DRAWDOWN_LIMIT)
  → Pausa automática si la ventana de PnL cae demasiado

Nivel 4: Daily Loss Limit (DAILY_LOSS_LIMIT = $50)
  → Stop total si la sesión pierde más de $50
```

---

## Cómo arrancar

### Requisitos previos
```bash
# Redis corriendo
redis-server

# Dependencias instaladas
pip install -r requirements.txt

# Variables de entorno (.env)
POLY_API_KEY=...       # Solo para modo REAL
POLY_SECRET=...        # Solo para modo REAL
REDIS_URL=redis://localhost:6379
```

### Arranque

```bash
# Paper trading (recomendado para probar)
python start_all.py

# Dinero real
python start_all.py --real

# Sin optimizador (ahorra CPU)
python start_all.py --no-optimizer

# Ver estado
python start_all.py --status

# Parar todo
python start_all.py --stop
```

### Acceder al Dashboard
```
http://localhost:8081
```

---

## Archivos del sistema

| Archivo | Qué es |
|---------|--------|
| `calvin5.py` | Bot principal (cerebro) |
| `execution_bot.py` | Ejecutor de órdenes |
| `dynamic_optimizer.py` | Optimizador bayesiano |
| `watchdog.py` | Supervisor de procesos |
| `dashboard_server.py` | Servidor web del dashboard |
| `btc_price.py` | Feed de precio BTC en tiempo real |
| `polymarket_simulator.py` | Simulador de paper trading |
| `start_all.py` | Lanzador unificado |
| `calvin5_trades.csv` | Historial de todos los trades |
| `calvin5_open.json` | Posiciones abiertas actuales |
| `calvinbot_pids.json` | PIDs de los procesos activos |
| `.env` | Credenciales y config (no compartir) |

---

## Glosario básico

| Término | Significado |
|---------|-------------|
| **Token UP/DOWN** | Lo que compras. Vale 1 USDC si aciertas, 0 si fallas |
| **Precio del token** | Probabilidad implícita. 0.70 = 70% de que ese resultado pase |
| **Momentum** | Velocidad y dirección del movimiento de BTC |
| **TP (Take Profit)** | Precio al que vendemos para asegurar ganancia |
| **SL (Stop Loss)** | Precio al que vendemos para limitar pérdida |
| **Fill** | Confirmación de que la orden fue ejecutada |
| **Slippage** | Diferencia entre el precio que pediste y el que conseguiste |
| **FOK (Fill or Kill)** | Tipo de orden: se ejecuta completa o no se ejecuta |
| **DRY RUN** | Modo simulado, sin dinero real |
| **PnL** | Profit and Loss — ganancias y pérdidas |
| **Drawdown** | Caída desde el máximo histórico de PnL |
| **Win Rate** | % de trades que terminaron en ganancia |
| **Profit Factor** | Ganancias totales ÷ Pérdidas totales (>1 = rentable) |
| **Sharpe Ratio** | Retorno por unidad de riesgo (más alto = más consistente) |
| **CLOB** | Central Limit Order Book — el libro de órdenes de Polymarket |
| **Redis** | Base de datos en memoria usada como canal de comunicación entre bots |
| **Optuna** | Librería de optimización bayesiana usada por el optimizador |
