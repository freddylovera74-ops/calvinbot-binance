"""
backtest.py — Simulación histórica de la estrategia CalvinBot.

Lee el CSV de trades y re-simula la estrategia aplicando:
  · Probabilidad de rechazo FOK (30% por defecto)
  · Slippage aleatorio ±1.5% sobre el precio de entrada
  · Latencia de fill simulada (500-2000ms, irrelevante para PnL pero log informativo)

El objetivo es cuantificar el impacto real del sistema de ejecución sobre el
edge bruto medido en validate_edge.py.

Uso:
    python backtest.py                              # usa calvin5_trades.csv
    python backtest.py --csv trades.csv             # archivo alternativo
    python backtest.py --fok-reject-rate 0.3        # 30% de órdenes rechazadas (default)
    python backtest.py --slippage 0.015             # ±1.5% slippage (default)
    python backtest.py --mode REAL                  # solo trades REAL
    python backtest.py --seed 42                    # reproducible
    python backtest.py --runs 1000                  # Monte Carlo (N simulaciones)
"""

import argparse
import csv
import random
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CSV          = "calvin5_trades.csv"
POLYMARKET_FEE_PCT   = 0.03       # fee en SELLs anticipados
DEFAULT_FOK_REJECT   = 0.30       # 30% de rechazos FOK (estimación conservadora)
DEFAULT_SLIPPAGE     = 0.015      # ±1.5% slippage en precio de fill

RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ─────────────────────────────────────────────────────────────────────────────
#  CARGA
# ─────────────────────────────────────────────────────────────────────────────

def load_trades(csv_path: str, mode_filter: Optional[str] = None) -> List[Dict]:
    path = Path(csv_path)
    if not path.exists():
        print(f"{RED}ERROR: No se encontró '{csv_path}'{RESET}")
        sys.exit(1)

    trades = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                mode = row.get("mode", "DRY").strip()
                if mode_filter and mode != mode_filter:
                    continue
                pnl         = float(row.get("pnl", 0) or 0)
                entry_price = float(row.get("entry_price", 0) or 0)
                exit_price  = float(row.get("exit_price", 0) or 0)
                size_usd    = float(row.get("size_usd", 0) or 0)
                exit_reason = row.get("exit_reason", "").strip()
                side        = row.get("side", "").strip()
                trades.append({
                    "pnl": pnl, "entry_price": entry_price, "exit_price": exit_price,
                    "size_usd": size_usd, "exit_reason": exit_reason,
                    "mode": mode, "side": side,
                })
            except (ValueError, KeyError):
                continue
    return trades


# ─────────────────────────────────────────────────────────────────────────────
#  SIMULACIÓN DE UNA PASADA
# ─────────────────────────────────────────────────────────────────────────────

def simulate_once(
    trades: List[Dict],
    fok_reject_rate: float,
    slippage_pct: float,
    rng: random.Random,
) -> Dict:
    """
    Simula el impacto de rechazos FOK y slippage sobre un conjunto de trades.

    Reglas:
      · Si la orden FOK es rechazada → el trade no se ejecuta (PnL = 0, oportunidad perdida)
      · Si se acepta → el precio de entrada se desplaza aleatoriamente en ±slippage_pct
        (peor slippage aumenta el coste; mejor slippage lo reduce)
      · La resolución (exit_reason='resolve') no se ve afectada por slippage en la salida
      · En salidas anticipadas (SELL) el precio de salida también se desplaza
    """
    executed   = 0
    rejected   = 0
    total_pnl  = 0.0
    wins       = 0
    losses     = 0
    gross_win  = 0.0
    gross_loss = 0.0
    equity     = 0.0
    peak       = 0.0
    max_dd     = 0.0

    for t in trades:
        # ── Rechazo FOK ───────────────────────────────────────────────────────
        if rng.random() < fok_reject_rate:
            rejected += 1
            continue
        executed += 1

        entry_price = t["entry_price"]
        exit_price  = t["exit_price"]
        size_usd    = t["size_usd"]
        exit_reason = t["exit_reason"]

        if entry_price <= 0 or size_usd <= 0:
            # Datos insuficientes — usar PnL original
            adj_pnl = t["pnl"]
        else:
            # ── Slippage en entrada (siempre adverso: precio sube → pagamos más) ──
            slip_dir = rng.choice([-1, 1])
            slip_mag = rng.uniform(0, slippage_pct)
            adj_entry = entry_price * (1 + slip_dir * slip_mag)
            adj_entry = max(0.01, min(0.99, adj_entry))

            tokens = size_usd / adj_entry

            if exit_reason == "resolve":
                # Resolución: precio de salida es 1.0 (ganó) ó 0.0 (perdió)
                # Inferir resultado desde exit_price original
                resolved_price = exit_price if exit_price in (0.0, 1.0) else (
                    1.0 if t["pnl"] > 0 else 0.0
                )
                adj_pnl = tokens * resolved_price - size_usd
            else:
                # SELL anticipado — slippage en salida también
                if exit_price > 0:
                    slip_dir2 = -1  # slippage de salida siempre adverso (precio baja al vender)
                    slip_mag2 = rng.uniform(0, slippage_pct)
                    adj_exit = exit_price * (1 + slip_dir2 * slip_mag2)
                    adj_exit = max(0.01, min(0.99, adj_exit))
                    adj_pnl = tokens * adj_exit * (1 - POLYMARKET_FEE_PCT) - size_usd
                else:
                    adj_pnl = t["pnl"]

        total_pnl += adj_pnl
        if adj_pnl > 0:
            wins      += 1
            gross_win += adj_pnl
        elif adj_pnl < 0:
            losses      += 1
            gross_loss  += abs(adj_pnl)

        # Drawdown
        equity += adj_pnl
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

    total_trades = wins + losses
    win_rate     = wins / total_trades if total_trades > 0 else 0.0
    profit_factor = gross_win / gross_loss if gross_loss > 0 else float("inf")

    return {
        "executed":      executed,
        "rejected":      rejected,
        "total_pnl":     total_pnl,
        "win_rate":      win_rate,
        "wins":          wins,
        "losses":        losses,
        "profit_factor": profit_factor,
        "max_drawdown":  max_dd,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  INFORME
# ─────────────────────────────────────────────────────────────────────────────

def _color_val(val: float, good_if_positive: bool = True) -> str:
    if good_if_positive:
        c = GREEN if val > 0 else (RED if val < 0 else RESET)
    else:
        c = RED if val > 0 else (GREEN if val < 0 else RESET)
    return f"{c}{val:+.2f}{RESET}"


def print_single_run(result: Dict, n_original: int, fok_rate: float, slip: float) -> None:
    n_exec   = result["executed"]
    n_rej    = result["rejected"]
    wr       = result["win_rate"]
    pnl      = result["total_pnl"]
    pf       = result["profit_factor"]
    dd       = result["max_drawdown"]
    rej_pct  = n_rej / n_original * 100 if n_original else 0

    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║   BACKTEST CalvinBot — Simulación realista   ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════════╝{RESET}")
    print(f"  Parámetros: FOK rechazo={fok_rate:.0%}  slippage=±{slip:.1%}")
    print(f"  Trades originales:  {n_original}")
    print(f"  Rechazados (FOK):   {n_rej}  ({rej_pct:.1f}%)")
    print(f"  Ejecutados:         {n_exec}")
    print()
    wr_c = GREEN if wr >= 0.45 else RED
    print(f"  Win Rate:       {wr_c}{wr:.1%}{RESET}  ({result['wins']}W / {result['losses']}L)")
    print(f"  PnL neto:       {_color_val(pnl)}")
    pf_c = GREEN if pf >= 1.10 else RED
    pf_str = f"{pf:.3f}" if pf != float('inf') else "∞"
    print(f"  Profit factor:  {pf_c}{pf_str}{RESET}  (mínimo 1.10)")
    dd_c = RED if dd >= 0.40 else (YELLOW if dd >= 0.25 else GREEN)
    print(f"  Max drawdown:   {dd_c}{dd:.1%}{RESET}  (límite 40%)")
    print()
    passes = (
        n_exec >= 10 and wr >= 0.45 and pnl > 0 and pf >= 1.10 and dd < 0.40
    )
    verdict = f"{GREEN}PASA{RESET}" if passes else f"{RED}FALLA{RESET}"
    print(f"  Veredicto: {verdict}")
    print()


def print_monte_carlo(
    results: List[Dict],
    n_original: int,
    fok_rate: float,
    slip: float,
    n_runs: int,
) -> None:
    pnls   = [r["total_pnl"]    for r in results]
    wrs    = [r["win_rate"]      for r in results]
    pfs    = [r["profit_factor"] for r in results if r["profit_factor"] != float("inf")]
    dds    = [r["max_drawdown"]  for r in results]

    def pct(lst: List[float], p: float) -> float:
        s = sorted(lst)
        idx = int(len(s) * p)
        return s[min(idx, len(s) - 1)]

    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║  MONTE CARLO  ({n_runs} runs)  CalvinBot         ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════════╝{RESET}")
    print(f"  Parámetros: FOK rechazo={fok_rate:.0%}  slippage=±{slip:.1%}")
    print(f"  Trades originales: {n_original}")
    print()
    print(f"  {'Métrica':<18} {'P5':>8} {'P50':>8} {'P95':>8} {'Media':>8}")
    print(f"  {'─'*18} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")

    def row(name: str, lst: List[float], fmt: str = ".2f", invert_color: bool = False) -> None:
        p5  = pct(lst, 0.05)
        p50 = pct(lst, 0.50)
        p95 = pct(lst, 0.95)
        mu  = statistics.mean(lst)
        print(f"  {name:<18} {p5:>8{fmt}} {p50:>8{fmt}} {p95:>8{fmt}} {mu:>8{fmt}}")

    row("PnL neto ($)",   pnls,   "+.2f")
    row("Win Rate",       [w*100 for w in wrs], ".1f")
    row("Profit Factor",  pfs if pfs else [0.0], ".3f")
    row("Max Drawdown %", [d*100 for d in dds], ".1f")

    n_positive = sum(1 for p in pnls if p > 0)
    prob_positive = n_positive / n_runs * 100
    print()
    prob_c = GREEN if prob_positive >= 60 else (YELLOW if prob_positive >= 40 else RED)
    print(f"  Probabilidad PnL positivo: {prob_c}{prob_positive:.1f}%{RESET}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backtest realista de CalvinBot con FOK rejections + slippage"
    )
    parser.add_argument("--csv",             default=DEFAULT_CSV, help="Ruta al CSV de trades")
    parser.add_argument("--mode",            default=None,        help="Filtro: REAL o DRY")
    parser.add_argument("--fok-reject-rate", type=float, default=DEFAULT_FOK_REJECT,
                        help=f"Probabilidad de rechazo FOK (default: {DEFAULT_FOK_REJECT})")
    parser.add_argument("--slippage",        type=float, default=DEFAULT_SLIPPAGE,
                        help=f"Slippage máximo por lado (default: {DEFAULT_SLIPPAGE})")
    parser.add_argument("--seed",            type=int,   default=None,
                        help="Semilla aleatoria para reproducibilidad")
    parser.add_argument("--runs",            type=int,   default=1,
                        help="Número de simulaciones Monte Carlo (default: 1)")
    args = parser.parse_args()

    trades = load_trades(args.csv, mode_filter=args.mode)
    if not trades:
        print(f"{RED}Sin trades para backtestear.{RESET}")
        sys.exit(1)

    print(f"  Cargados {len(trades)} trades de '{args.csv}'"
          + (f" (modo={args.mode})" if args.mode else ""))

    rng = random.Random(args.seed)

    if args.runs == 1:
        result = simulate_once(trades, args.fok_reject_rate, args.slippage, rng)
        print_single_run(result, len(trades), args.fok_reject_rate, args.slippage)
    else:
        results = [
            simulate_once(trades, args.fok_reject_rate, args.slippage, rng)
            for _ in range(args.runs)
        ]
        print_monte_carlo(results, len(trades), args.fok_reject_rate, args.slippage, args.runs)


if __name__ == "__main__":
    main()
