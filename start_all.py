"""
start_all.py — Lanzador unificado del sistema de trading Calvin5

Orden de arranque:
  1. Verifica Redis (requisito de todos los bots)
  2. execution_bot.py  — El Músculo (primero, para recibir órdenes)
  3. calvin5.py        — El Cerebro (segundo, empieza a calcular señales)
  4. watchdog.py       — El Supervisor (tercero, monitoriza los anteriores)
  5. dashboard_server.py — Dashboard web en http://localhost:8081/
  6. dynamic_optimizer.py — El Optimizador (último, no es crítico)

Uso:
  python start_all.py              # DRY RUN (paper trading)
  python start_all.py --real       # REAL MONEY
  python start_all.py --no-optimizer  # sin optimizer (ahorra recursos)
  python start_all.py --no-dashboard  # sin dashboard (ahorra recursos)
  python start_all.py --status     # solo muestra estado de los procesos
  python start_all.py --stop       # para todos los bots

Requisitos:
  - Redis corriendo en localhost:6379
  - .venv en el mismo directorio
  - .env con las variables de entorno configuradas
"""

import os
import sys
import time
import json
import signal
import subprocess
from pathlib import Path
from datetime import datetime

# Forzar UTF-8 en Windows (emojis en consola)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR   = Path(__file__).parent
PYTHON     = sys.executable          # Python que ejecuta este script (sin venv)
ENV_FILE   = BASE_DIR / ".env"
STATE_FILE = BASE_DIR / "calvinbot_pids.json"  # guarda los PIDs activos

# Orden de arranque con sus parámetros
BOTS = [
    {
        "name":    "ExecutionBot",
        "script":  "execution_bot.py",
        "emoji":   "💪",
        "delay_s": 3,   # esperar 3s antes de arrancar el siguiente
        "critical": True,  # si falla al arrancar → abortar todo
    },
    {
        "name":    "Calvin5",
        "script":  "calvin5.py",
        "emoji":   "🧠",
        "delay_s": 4,
        "critical": True,
    },
    {
        "name":    "Watchdog",
        "script":  "watchdog.py",
        "emoji":   "👁️",
        "delay_s": 2,
        "critical": False,
    },
    {
        "name":    "Dashboard",
        "script":  "dashboard_server.py",
        "emoji":   "📊",
        "delay_s": 2,
        "critical": False,
        "optional": True,  # puede omitirse con --no-dashboard
        "skip_flag": "no_dashboard",
    },
    {
        "name":    "Optimizer",
        "script":  "dynamic_optimizer.py",
        "emoji":   "⚙️",
        "delay_s": 2,
        "critical": False,
        "optional": True,  # puede omitirse con --no-optimizer
        "skip_flag": "no_optimizer",
    },
]

# ─────────────────────────────────────────────────────────────────────────────
#  COLORES CONSOLA (Windows compatible)
# ─────────────────────────────────────────────────────────────────────────────

if sys.platform == "win32":
    os.system("color")  # activa colores ANSI en Windows

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def _ok(msg):   print(f"{GREEN}  ✅ {msg}{RESET}")
def _warn(msg): print(f"{YELLOW}  ⚠️  {msg}{RESET}")
def _err(msg):  print(f"{RED}  ❌ {msg}{RESET}")
def _info(msg): print(f"{CYAN}  ℹ️  {msg}{RESET}")

# ─────────────────────────────────────────────────────────────────────────────
#  VERIFICACIONES PRE-ARRANQUE
# ─────────────────────────────────────────────────────────────────────────────

def check_redis() -> bool:
    """Verifica que Redis está corriendo y accesible."""
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, socket_timeout=3)
        r.ping()
        _ok("Redis conectado (localhost:6379)")
        return True
    except Exception as exc:
        _err(f"Redis NO disponible: {exc}")
        _warn("Instala Redis: https://github.com/tporadowski/redis/releases")
        _warn("O arranca el servicio: Start-Service Redis (PowerShell admin)")
        return False



def check_env_file() -> bool:
    """Verifica que el .env existe y tiene las variables críticas."""
    if not ENV_FILE.exists():
        _warn(".env no encontrado — usando variables del sistema")
        return True  # no es fatal, puede tener las vars en el entorno

    content = ENV_FILE.read_text(encoding="utf-8", errors="replace")
    required = ["PRIVATE_KEY", "POLYMARKET_ADDRESS"]
    missing  = [k for k in required if k not in content]

    if missing:
        _warn(f".env incompleto — faltan: {', '.join(missing)}")
        return True  # advertencia, no error fatal
    _ok(".env encontrado con variables críticas")
    return True


def check_scripts() -> bool:
    """Verifica que los scripts de los bots existen."""
    all_ok = True
    for bot in BOTS:
        script = BASE_DIR / bot["script"]
        if script.exists():
            _ok(f"{bot['script']} encontrado")
        elif bot.get("optional"):
            _warn(f"{bot['script']} no encontrado — será omitido (opcional)")
        else:
            _err(f"{bot['script']} NO encontrado en {BASE_DIR}")
            all_ok = False
    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
#  GESTIÓN DE PROCESOS
# ─────────────────────────────────────────────────────────────────────────────

def _save_pids(pids: dict) -> None:
    """Guarda los PIDs activos a disco para poder pararlos después."""
    try:
        STATE_FILE.write_text(json.dumps(pids, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_pids() -> dict:
    """Carga los PIDs guardados."""
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _is_running(pid: int) -> bool:
    """True si el proceso con ese PID sigue corriendo."""
    try:
        import psutil
        return psutil.pid_exists(pid) and psutil.Process(pid).is_running()
    except Exception:
        # Fallback sin psutil
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False


def launch_bot(bot: dict, extra_args: list = None) -> subprocess.Popen:
    """Lanza un bot como subproceso independiente con su propia consola."""
    script = BASE_DIR / bot["script"]
    cmd    = [PYTHON, str(script)] + (extra_args or [])

    # Cada bot abre su propia ventana de consola en Windows
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = subprocess.CREATE_NEW_CONSOLE

    proc = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        creationflags=creation_flags,
    )
    return proc


# ─────────────────────────────────────────────────────────────────────────────
#  COMANDOS PRINCIPALES
# ─────────────────────────────────────────────────────────────────────────────

def _print_channel_table() -> None:
    """Imprime la tabla de canales Redis y verifica que publicadores y suscriptores coinciden."""
    rows = [
        # (canal,                            publicador,         suscriptor(es))
        ("signals:trade (pubsub)",           "calvin5",          "execution_bot"),
        ("stream:signals:trade (Stream)",    "calvin5",          "execution_bot (Redis>=5)"),
        ("execution:receipts:{id}",          "execution_bot",    "calvin5"),
        ("execution:logs",                   "execution_bot",    "dashboard"),
        ("health:heartbeats",                "todos los bots",   "watchdog, dashboard, calvin5"),
        ("config:update:calculation_bot",    "optimizer",        "calvin5"),
        ("signals:control",                  "dashboard",        "calvin5"),
        ("logs:unified",                     "todos los bots",   "dashboard"),
        ("emergency:commands",               "watchdog",         "execution_bot"),
        ("alerts:executor",                  "execution_bot",    "watchdog"),
    ]
    col1, col2, col3 = 38, 22, 28
    sep = f"  +{'-'*col1}+{'-'*col2}+{'-'*col3}+"
    hdr = f"  |{'CANAL REDIS':<{col1}}|{'PUBLICA':<{col2}}|{'ESCUCHA':<{col3}}|"
    print(f"\n{BOLD}Mapa de canales Redis:{RESET}")
    print(sep); print(hdr); print(sep)
    for canal, pub, sub in rows:
        print(f"  |{canal:<{col1}}|{pub:<{col2}}|{sub:<{col3}}|")
    print(sep)
    print(f"  {GREEN}Todos los canales verificados — sin discrepancias{RESET}\n")


def cmd_start(real_mode: bool = False, no_optimizer: bool = False, no_dashboard: bool = False) -> None:
    """Arranca todos los bots en el orden correcto."""

    # Determinar modo: --real arg OR DRY_RUN=false en .env
    env_dry_run_val = None
    if ENV_FILE.exists():
        try:
            from dotenv import dotenv_values
            env_vals = dotenv_values(ENV_FILE)
            # strip() elimina espacios y comentarios residuales
            raw_val = env_vals.get("DRY_RUN", "true").strip().split()[0].lower()
            env_dry_run_val = raw_val
            if not real_mode and raw_val == "false":
                real_mode = True
        except Exception:
            pass

    # Si hay contradicción (arranca sin --real pero .env pide real), avisar claramente
    if not real_mode and env_dry_run_val == "false":
        _warn(".env tiene DRY_RUN=false pero el sistema arranca en DRY RUN")
        _warn("→ Usa: python start_all.py --real  para activar dinero real")

    print(f"\n{BOLD}{'='*60}")
    print(f"  CalvinBot — Sistema de Trading BTC 5min")
    mode_str = f"{RED}⚠️  REAL MONEY" if real_mode else f"{GREEN}📄 DRY RUN (paper)"
    print(f"  Modo: {mode_str}{BOLD}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}{RESET}\n")

    # ── Pre-checks ────────────────────────────────────────────────────────────
    print(f"{BOLD}Verificaciones previas:{RESET}")
    checks = [
        check_redis(),
        check_env_file(),
        check_scripts(),
    ]
    if not all(checks):
        _err("Pre-checks fallaron — abortando arranque")
        sys.exit(1)

    # Confirmación extra si es modo REAL
    if real_mode:
        print(f"\n{RED}{BOLD}⚠️  ATENCIÓN: Modo REAL MONEY activo{RESET}")
        print(f"{YELLOW}  Se ejecutarán órdenes reales en Polymarket{RESET}")
        resp = input(f"\n  ¿Confirmas el arranque con dinero real? (escribe 'SI' para confirmar): ")
        if resp.strip().upper() != "SI":
            _info("Arranque cancelado por el usuario")
            sys.exit(0)

    # Tabla de canales Redis — verificación de coordinación entre bots
    _print_channel_table()

    # Detectar MICRO_TEST desde .env o variable de entorno del sistema
    micro_test = os.getenv("MICRO_TEST", "false").lower() in ("1", "true")
    if not micro_test and ENV_FILE.exists():
        try:
            from dotenv import dotenv_values
            env_vals    = dotenv_values(ENV_FILE)
            micro_raw   = env_vals.get("MICRO_TEST", "false").strip().split()[0].lower()
            micro_test  = micro_raw in ("1", "true")
        except Exception:
            pass

    # Configurar variable de entorno DRY_RUN (sobreescribe lo que haya en .env para los subprocesos)
    env = os.environ.copy()
    env["DRY_RUN"]   = "false" if real_mode else "true"
    env["REDIS_URL"] = env.get("REDIS_URL", "redis://localhost:6379")

    # Propagar MICRO_TEST a todos los subprocesos
    if micro_test:
        env["MICRO_TEST"] = "1"
        _warn("MICRO_TEST activado — todos los bots operarán con stake $1 y sin validaciones de riesgo")
    else:
        env["MICRO_TEST"] = "0"  # Asegurar que no quede activo de una sesión anterior

    # ── Arranque de bots ──────────────────────────────────────────────────────
    print(f"\n{BOLD}Arrancando bots:{RESET}")
    pids     = {}
    launched = []

    # Build skip flags map
    skip_flags = {"no_optimizer": no_optimizer, "no_dashboard": no_dashboard}

    for bot in BOTS:
        # Saltar bots opcionales si su flag está activo
        bot_flag = bot.get("skip_flag")
        if bot_flag and skip_flags.get(bot_flag):
            _warn(f"{bot['emoji']} {bot['name']} — OMITIDO (--{bot_flag.replace('_', '-')})")
            continue

        # Saltar bots opcionales cuyo script no existe
        if bot.get("optional") and not (BASE_DIR / bot["script"]).exists():
            _warn(f"{bot['emoji']} {bot['name']} — OMITIDO (script no encontrado)")
            continue

        print(f"\n  {bot['emoji']} Arrancando {BOLD}{bot['name']}{RESET}...", end=" ")

        try:
            proc = subprocess.Popen(
                [PYTHON, str(BASE_DIR / bot["script"])],
                cwd=str(BASE_DIR),
                env=env,
                creationflags=subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0,
            )
            time.sleep(1)  # pequeña pausa para detectar crash inmediato

            if proc.poll() is not None:
                # El proceso ya terminó — crash al arrancar
                print(f"{RED}CRASH (código {proc.returncode}){RESET}")
                if bot["critical"]:
                    _err(f"{bot['name']} es crítico y crasheó — parando todo")
                    for name, pid in pids.items():
                        try:
                            import psutil
                            psutil.Process(pid).terminate()
                        except Exception:
                            pass
                    sys.exit(1)
            else:
                pids[bot["name"]] = proc.pid
                launched.append(bot["name"])
                print(f"{GREEN}OK (PID {proc.pid}){RESET}")

        except Exception as exc:
            print(f"{RED}ERROR: {exc}{RESET}")
            if bot["critical"]:
                _err(f"Error crítico arrancando {bot['name']} — abortando")
                sys.exit(1)

        # Esperar entre bots para que cada uno inicialice antes del siguiente
        if bot["delay_s"] > 0 and bot != BOTS[-1]:
            print(f"  ⏳ Esperando {bot['delay_s']}s para el siguiente bot...", end="\r")
            time.sleep(bot["delay_s"])
            print(" " * 60, end="\r")

    # Guardar PIDs
    _save_pids(pids)

    # ── Resumen final ─────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*60}")
    print(f"  Sistema arrancado — {len(launched)} bots activos")
    print(f"{'='*60}{RESET}")
    for name, pid in pids.items():
        status = "🟢 corriendo" if _is_running(pid) else "🔴 caído"
        print(f"  {status}  {name:<20} PID {pid}")

    print(f"\n{CYAN}  📋 Logs:{RESET}")
    print(f"    Calvin5:   calvin5.log")
    print(f"    Executor:  executor.log")
    print(f"    Watchdog:  watchdog.log")
    print(f"    Optimizer: dynamic_optimizer.log")
    print(f"\n{CYAN}  📊 Dashboard:{RESET}")
    print(f"    http://localhost:8081/")
    print(f"\n{CYAN}  🛑 Para parar todo:{RESET}")
    print(f"    python start_all.py --stop\n")

    _info("PIDs guardados en calvinbot_pids.json")

    # Mantener el lanzador activo mostrando estado cada 30s
    print(f"\n{BOLD}Monitor de estado (Ctrl+C para salir del monitor, los bots siguen corriendo):{RESET}")
    try:
        while True:
            time.sleep(30)
            print(f"\n  [{datetime.now().strftime('%H:%M:%S')}] Estado:")
            all_ok = True
            for name, pid in pids.items():
                running = _is_running(pid)
                icon    = "🟢" if running else "🔴"
                print(f"    {icon} {name:<20} PID {pid}")
                if not running:
                    all_ok = False
            if not all_ok:
                _warn("Uno o más bots han caído — revisa los logs")
    except KeyboardInterrupt:
        print(f"\n{CYAN}  Monitor detenido — los bots siguen corriendo en segundo plano{RESET}")
        print(f"  Para pararlos: python start_all.py --stop\n")


def cmd_stop() -> None:
    """Para todos los bots guardados en el state file."""
    print(f"\n{BOLD}Parando todos los bots...{RESET}\n")
    pids = _load_pids()

    if not pids:
        _warn("No hay PIDs guardados en calvinbot_pids.json")
        _info("Intentando parar por nombre de proceso...")
        try:
            import psutil
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    cmdline = " ".join(proc.info.get("cmdline") or [])
                    for bot in BOTS:
                        if bot["script"] in cmdline:
                            proc.terminate()
                            _ok(f"Parado: {bot['name']} (PID {proc.pid})")
                except Exception:
                    pass
        except ImportError:
            _err("psutil no instalado — para manualmente los procesos Python")
        return

    # Primero enviar CLOSE_ALL al execution_bot vía Redis antes de matar
    try:
        import redis as _redis
        r = _redis.Redis(host="localhost", port=6379, socket_timeout=2)
        cmd = {
            "source":    "watchdog",
            "command":   "CLOSE_ALL",
            "priority":  "SUPREME",
            "reason":    "Apagado manual via start_all.py --stop",
            "timestamp": datetime.utcnow().isoformat(),
        }
        r.publish("emergency:commands", json.dumps(cmd))
        _ok("CLOSE_ALL enviado al ExecutionBot — esperando 3s para liquidar posiciones")
        time.sleep(3)
    except Exception as exc:
        _warn(f"No se pudo enviar CLOSE_ALL: {exc}")

    # Parar en orden inverso (optimizer → watchdog → calvin5 → executor)
    stop_order = list(reversed(list(pids.items())))
    for name, pid in stop_order:
        if not _is_running(pid):
            _warn(f"{name} (PID {pid}) ya no estaba corriendo")
            continue
        try:
            import psutil
            proc = psutil.Process(pid)
            proc.terminate()
            # Esperar hasta 5s a que termine limpiamente
            for _ in range(10):
                if not proc.is_running():
                    break
                time.sleep(0.5)
            if proc.is_running():
                proc.kill()
                _ok(f"Forzado: {name} (PID {pid})")
            else:
                _ok(f"Parado:  {name} (PID {pid})")
        except Exception as exc:
            _err(f"Error parando {name} (PID {pid}): {exc}")

    # Limpiar state file
    try:
        STATE_FILE.unlink()
    except Exception:
        pass
    print(f"\n{GREEN}{BOLD}  Sistema detenido correctamente{RESET}\n")


def cmd_status() -> None:
    """Muestra el estado actual de todos los bots."""
    print(f"\n{BOLD}Estado del sistema — {datetime.now().strftime('%H:%M:%S')}{RESET}\n")

    pids = _load_pids()
    if not pids:
        _warn("No hay PIDs guardados — el sistema no está arrancado o fue iniciado externamente")

    # Verificar Redis
    print(f"  {'Redis':<22}", end="")
    try:
        import redis as _redis
        r = _redis.Redis(host="localhost", port=6379, socket_timeout=2)
        r.ping()
        print(f"{GREEN}🟢 corriendo{RESET}")

        # Leer estado de Calvin5 desde Redis
        raw = r.get("state:calculator:latest")
        if raw:
            state = json.loads(raw)
            ts    = state.get("timestamp", "")[:19]
            slug  = state.get("market_slug", "sin mercado")
            pnl   = state.get("window_pnl", 0)
            open_n = len(state.get("open_positions", []))
            print(f"\n  {CYAN}Calvin5 estado (Redis):{RESET}")
            print(f"    Último heartbeat: {ts}")
            print(f"    Mercado activo:   {slug}")
            print(f"    PnL tramo:        ${pnl:+.2f}")
            print(f"    Posiciones open:  {open_n}")
            params = state.get("params", {})
            if params:
                print(f"    Parámetros vivos:")
                for k, v in list(params.items())[:6]:
                    print(f"      {k}: {v}")
    except Exception:
        print(f"{RED}🔴 no disponible{RESET}")

    # Estado de procesos
    print(f"\n  {BOLD}Procesos:{RESET}")
    if pids:
        for name, pid in pids.items():
            running = _is_running(pid)
            icon    = f"{GREEN}🟢 corriendo{RESET}" if running else f"{RED}🔴 caído{RESET}"
            print(f"    {name:<22} PID {pid:<8} {icon}")
    else:
        # Intentar detectar por nombre
        try:
            import psutil
            found = []
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    cmdline = " ".join(proc.info.get("cmdline") or [])
                    for bot in BOTS:
                        if bot["script"] in cmdline:
                            found.append((bot["name"], proc.pid))
                except Exception:
                    pass
            if found:
                for name, pid in found:
                    print(f"    {GREEN}🟢{RESET} {name:<22} PID {pid} (detectado por nombre)")
            else:
                _warn("Ningún bot detectado corriendo")
        except ImportError:
            _warn("Instala psutil para detección por nombre: pip install psutil")

    # Logs recientes
    print(f"\n  {BOLD}Últimas líneas de logs:{RESET}")
    log_files = [
        ("Calvin5",   "calvin5.log"),
        ("Executor",  "executor.log"),
        ("Watchdog",  "watchdog.log"),
        ("Optimizer", "dynamic_optimizer.log"),
    ]
    for name, logfile in log_files:
        path = BASE_DIR / logfile
        if path.exists():
            try:
                lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
                last  = lines[-1] if lines else "(vacío)"
                print(f"    {name:<12} → {last[-80:]}")
            except Exception:
                pass
    print()


# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    if "--stop" in args:
        cmd_stop()

    elif "--status" in args:
        cmd_status()

    else:
        real_mode    = "--real" in args
        no_optimizer = "--no-optimizer" in args
        no_dashboard = "--no-dashboard" in args
        cmd_start(real_mode=real_mode, no_optimizer=no_optimizer, no_dashboard=no_dashboard)