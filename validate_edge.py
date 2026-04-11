"""
validate_edge.py — Validación estadística del edge real de CalvinBot.

Carga el historial de trades del CSV, separa DRY de REAL, y determina si
existe una ventaja estadística genuina suficiente para operar con dinero real.

Criterios de PASE (todos deben cumplirse):
  1. Mínimo 500 trades en modo REAL (no DRY)
  2. Win rate (TP / total trades) >= 45%
  3. PnL neto positivo después de comisiones
  4. Profit factor (ganancias brutas / pérdidas brutas) > 1.10
  5. Max drawdown relativo < 40% del capital inicial

Uso:
    python validate_edge.py                    # usa calvin5_trades.csv por defecto
    python validate_edge.py --csv trades.csv   # archivo alternativo
    python validate_edge.py --min-trades 200   # umbral mínimo diferente
    python validate_edge.py --mode DRY         # evaluar también en modo DRY (advertencia)
"""

import argparse
import csv
import math
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTES
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CSV          = "calvin5_trades.csv"
MIN_TRADES_REQUIRED  = 500     # Mínimo para validación estadística fiable
WIN_RATE_THRESHOLD   = 0.45    # 45% — umbral mínimo de rentabilidad con R:R ~1.8
PROFIT_FACTOR_MIN    = 1.10    # Ganancias brutas > 1.10× las pérdidas brutas
MAX_DRAWDOWN_LIMIT   = 0.40    # Drawdown relativo máximo tolerable (40%)
POLYMARKET_FEE_PCT   = 0.03    # 3% en SELLs anticipados (ya debe estar en el CSV)

# Colores ANSI para consola
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ─────────────────────────────────────────────────────────────────────────────
#  CARGA Y PARSING DEL CSV
# ─────────────────────────────────────────────────────────────────────────────

def load_trades(csv_path: str, mode_filter: Optional[str] = None) -> List[Dict]:
    """
    Carga trades del CSV. Si mode_filter='REAL', devuelve solo trades reales.
    Cada trade es un dict con: pnl, fee_usd, exit_reason, mode, entry_price,
    exit_price, size_usd, entry_time, exit_time.
    """
    path = Path(csv_path)
    if not path.exists():
        print(f"{RED}ERROR: No se encontró el archivo '{csv_path}'{RESET}")
        sys.exit(1)

    trades = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                mode = row.get("mode", "DRY").strip()
                if mode_filter and mode != mode_filter:
                    continue

                pnl = float(row.get("pnl", 0) or 0)
                fee = float(row.get("fee_usd", 0) or 0)
                exit_reason = row.get("exit_reason", "").strip()
                entry_price = float(row.get("entry_price", 0) or 0)
                exit_price  = float(row.get("exit_price", 0) or 0)
                size_usd    = float(row.get("size_usd", 0) or 0)
                entry_time  = float(row.get("entry_time", 0) or 0)
                exit_time   = float(row.get("exit_time", 0) or 0)
                side        = row.get("side", "").strip()

                trades.append({
                    "pnl":          pnl,
                    "fee_usd":      fee,
                    "exit_reason":  exit_reason,
                    "mode":         mode,
                    "entry_price":  entry_price,
                    "exit_price":   exit_price,
                    "size_usd":     size_usd,
                    "entry_time":   entry_time,
                    "exit_time":    exit_time,
                    "side":         side,
                })
            except (ValueError, KeyError):
                continue

    return trades


# ─────────────────────────────────────────────────────────────────────────────
#  MÉTRICAS
# ─────────────────────────────────────────────────────────────────────────────

def compute_metrics(trades: List[Dict]) -> Dict:
    """
    Calcula todas las métricas de rendimiento sobre la lista de trades.
    Devuelve un dict con todas las métricas relevantes.
    """
    if not trades:
        return {}

    n = len(trades)

    # ── Clasificación básica ──────────────────────────────────────────────────
    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] < 0]
    flat   = [t for t in trades if t["pnl"] == 0]

    win_rate  = len(wins) / n
    loss_rate = len(losses) / n

    # ── PnL ───────────────────────────────────────────────────────────────────
    total_pnl      = sum(t["pnl"] for t in trades)
    total_fees     = sum(t["fee_usd"] for t in trades)
    gross_wins     = sum(t["pnl"] for t in wins)
    gross_losses   = abs(sum(t["pnl"] for t in losses))
    avg_win        = gross_wins  / len(wins)  if wins   else 0.0
    avg_loss       = gross_losses / len(losses) if losses else 0.0
    profit_factor  = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    # ── R:R ratio ─────────────────────────────────────────────────────────────
    rr_ratio = avg_win / avg_loss if avg_loss > 0 else float("inf")

    # ── Break-even win rate (para R:R actual) ─────────────────────────────────
    # WR_breakeven = 1 / (1 + RR)
    breakeven_wr = 1.0 / (1.0 + rr_ratio) if rr_ratio > 0 else 0.5

    # ── Drawdown (sobre equity curve a partir de 0) ───────────────────────────
    equity = 0.0
    peak   = 0.0
    max_dd = 0.0
    equity_curve = []
    for t in sorted(trades, key=lambda x: x["entry_time"]):
        equity += t["pnl"]
        equity_curve.append(equity)
        if equity > peak:
            peak = equity
        dd = (peak - equity) / max(abs(peak), 1e-6)  # relativo al pico
        if dd > max_dd:
            max_dd = dd

    # ── Sharpe-proxy (sin tasa libre de riesgo) ───────────────────────────────
    pnls = [t["pnl"] for t in trades]
    mean_pnl = total_pnl / n
    if n > 1:
        variance = sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)
        std_pnl  = math.sqrt(variance)
        sharpe   = mean_pnl / std_pnl if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0

    # ── Clasificación por razón de cierre ─────────────────────────────────────
    by_reason: Dict[str, int] = defaultdict(int)
    for t in trades:
        reason = t["exit_reason"] or "unknown"
        # normalizar: tp_full_+35.0% → tp_full, sl_drop → sl_drop, etc.
        base = reason.split()[0].split("+")[0].split("-")[0].rstrip("_")
        by_reason[base] += 1

    # ── Retorno medio por trade (%) ───────────────────────────────────────────
    avg_return_pct = (total_pnl / max(sum(t["size_usd"] for t in trades), 1e-6)) * 100

    # ── Holding time promedio (segundos) ─────────────────────────────────────
    hold_times = [
        t["exit_time"] - t["entry_time"]
        for t in trades
        if t["exit_time"] > t["entry_time"]
    ]
    avg_hold_s = sum(hold_times) / len(hold_times) if hold_times else 0

    # ── Expected Value (EV) por trade ─────────────────────────────────────────
    ev = (win_rate * avg_win) - (loss_rate * avg_loss)

    return {
        "n":               n,
        "wins":            len(wins),
        "losses":          len(losses),
        "flat":            len(flat),
        "win_rate":        win_rate,
        "loss_rate":       loss_rate,
        "total_pnl":       total_pnl,
        "total_fees":      total_fees,
        "gross_wins":      gross_wins,
        "gross_losses":    gross_losses,
        "avg_win":         avg_win,
        "avg_loss":        avg_loss,
        "profit_factor":   profit_factor,
        "rr_ratio":        rr_ratio,
        "breakeven_wr":    breakeven_wr,
        "ev":              ev,
        "max_drawdown":    max_dd,
        "equity_final":    equity,
        "sharpe":          sharpe,
        "by_reason":       dict(by_reason),
        "avg_return_pct":  avg_return_pct,
        "avg_hold_s":      avg_hold_s,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  EVALUACIÓN DE CRITERIOS
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_criteria(m: Dict, n_real: int, min_trades: int) -> List[Tuple[str, bool, str]]:
    """
    Evalúa cada criterio. Devuelve lista de (criterio, pasa, detalle).
    """
    results = []

    # 1. Volumen mínimo de trades
    results.append((
        f"Mínimo {min_trades} trades REAL",
        n_real >= min_trades,
        f"{n_real}/{min_trades} trades reales en historial"
    ))

    # 2. Win rate
    results.append((
        f"Win rate ≥ {WIN_RATE_THRESHOLD*100:.0f}%",
        m["win_rate"] >= WIN_RATE_THRESHOLD,
        f"{m['win_rate']*100:.1f}% ({m['wins']}W / {m['losses']}L)"
    ))

    # 3. PnL neto positivo
    results.append((
        "PnL neto positivo",
        m["total_pnl"] > 0,
        f"${m['total_pnl']:+.2f} (fees: ${m['total_fees']:.2f})"
    ))

    # 4. Profit factor
    pf = m["profit_factor"]
    pf_str = f"{pf:.2f}" if pf != float("inf") else "∞"
    results.append((
        f"Profit factor ≥ {PROFIT_FACTOR_MIN}",
        m["profit_factor"] >= PROFIT_FACTOR_MIN,
        f"PF = {pf_str} (ganancias ${m['gross_wins']:.2f} / pérdidas ${m['gross_losses']:.2f})"
    ))

    # 5. Max drawdown
    results.append((
        f"Max drawdown < {MAX_DRAWDOWN_LIMIT*100:.0f}%",
        m["max_drawdown"] < MAX_DRAWDOWN_LIMIT,
        f"{m['max_drawdown']*100:.1f}% del equity pico"
    ))

    return results


# ─────────────────────────────────────────────────────────────────────────────
#  REPORTE
# ─────────────────────────────────────────────────────────────────────────────

def print_report(trades: List[Dict], mode_label: str, min_trades: int) -> bool:
    """Imprime reporte completo. Devuelve True si pasa todos los criterios."""

    m = compute_metrics(trades)
    if not m:
        print(f"\n{YELLOW}Sin trades para evaluar en modo {mode_label}.{RESET}")
        return False

    n_real = len(trades)

    print(f"\n{'='*62}")
    print(f"{BOLD}  INFORME DE VALIDACIÓN — Modo {mode_label} ({n_real} trades){RESET}")
    print(f"{'='*62}")

    # ── Métricas generales ────────────────────────────────────────────────────
    print(f"\n{CYAN}  MÉTRICAS GENERALES{RESET}")
    print(f"  Trades totales:     {m['n']}")
    print(f"  Win / Loss / Flat:  {m['wins']} / {m['losses']} / {m['flat']}")
    print(f"  Win rate:           {m['win_rate']*100:.1f}%")
    print(f"  Break-even WR:      {m['breakeven_wr']*100:.1f}% (para R:R {m['rr_ratio']:.2f})")
    print(f"  Expected value:     ${m['ev']:+.4f} por trade")
    print(f"  PnL neto total:     ${m['total_pnl']:+.2f}")
    print(f"  Total fees pagadas: ${m['total_fees']:.2f}")
    print(f"  Retorno medio:      {m['avg_return_pct']:+.2f}% del stake")
    print(f"  Avg ganancia:       ${m['avg_win']:.3f}")
    print(f"  Avg pérdida:        ${m['avg_loss']:.3f}")
    print(f"  R:R ratio:          {m['rr_ratio']:.2f}")
    print(f"  Profit factor:      {m['profit_factor']:.2f}" if m['profit_factor'] != float('inf') else "  Profit factor:      ∞")
    print(f"  Sharpe (proxy):     {m['sharpe']:.3f}")
    print(f"  Max drawdown:       {m['max_drawdown']*100:.1f}%")
    print(f"  Equity final:       ${m['equity_final']:+.2f}")
    print(f"  Holding promedio:   {m['avg_hold_s']:.0f}s")

    # ── Distribución por razón de cierre ──────────────────────────────────────
    if m["by_reason"]:
        print(f"\n{CYAN}  DISTRIBUCIÓN POR RAZÓN DE CIERRE{RESET}")
        total = sum(m["by_reason"].values())
        for reason, count in sorted(m["by_reason"].items(), key=lambda x: -x[1]):
            pct = count / total * 100
            bar = "█" * int(pct / 5)
            print(f"  {reason:<25} {count:>4}  {pct:5.1f}%  {bar}")

    # ── Criterios de validación ───────────────────────────────────────────────
    print(f"\n{CYAN}  CRITERIOS DE VALIDACIÓN{RESET}")
    criteria = evaluate_criteria(m, n_real, min_trades)
    all_pass = True
    for criterion, passes, detail in criteria:
        icon   = f"{GREEN}✓{RESET}" if passes else f"{RED}✗{RESET}"
        status = f"{GREEN}PASA{RESET}" if passes else f"{RED}FALLA{RESET}"
        print(f"  {icon} [{status}]  {criterion}")
        print(f"         {detail}")
        if not passes:
            all_pass = False

    # ── Veredicto final ───────────────────────────────────────────────────────
    print(f"\n{'='*62}")
    if all_pass:
        print(f"{BOLD}{GREEN}  ✅ VEREDICTO: EDGE VALIDADO — Condiciones para operar con dinero real{RESET}")
        print(f"{GREEN}     El sistema muestra ventaja estadística consistente.{RESET}")
    else:
        failed = sum(1 for _, p, _ in criteria if not p)
        # Check if only failure is insufficient trades
        trade_count_pass = criteria[0][1]
        if not trade_count_pass and failed == 1:
            print(f"{BOLD}{YELLOW}  ⚠️  VEREDICTO: DATOS INSUFICIENTES — No hay suficientes trades REAL{RESET}")
            print(f"{YELLOW}     Acumula al menos {min_trades} trades reales antes de sacar conclusiones.{RESET}")
            print(f"{YELLOW}     Con {n_real} trades el intervalo de confianza es demasiado amplio.{RESET}")
        else:
            print(f"{BOLD}{RED}  ❌ VEREDICTO: EDGE NO VALIDADO — {failed} criterio(s) no se cumplen{RESET}")
            print(f"{RED}     NO operes con dinero real hasta resolver los criterios marcados con ✗.{RESET}")
    print(f"{'='*62}\n")

    return all_pass


def print_dry_warning():
    print(f"\n{YELLOW}⚠️  ADVERTENCIA: Estás evaluando trades en modo DRY (simulación).{RESET}")
    print(f"{YELLOW}   El modo DRY siempre llena a precio señal sin slippage real ni rechazos FOK.{RESET}")
    print(f"{YELLOW}   Los resultados DRY son sistemáticamente optimistas respecto al REAL.{RESET}")
    print(f"{YELLOW}   Acumula trades REAL antes de tomar decisiones sobre capital real.{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Valida el edge estadístico de CalvinBot sobre el historial de trades."
    )
    parser.add_argument("--csv",        default=DEFAULT_CSV, help="Ruta al CSV de trades")
    parser.add_argument("--min-trades", type=int, default=MIN_TRADES_REQUIRED,
                        help=f"Mínimo de trades REAL requeridos (default: {MIN_TRADES_REQUIRED})")
    parser.add_argument("--mode",       default="REAL", choices=["REAL", "DRY", "ALL"],
                        help="Filtrar por modo (default: REAL)")
    args = parser.parse_args()

    print(f"\n{BOLD}CalvinBot — Validador de Edge Estadístico{RESET}")
    print(f"Archivo: {args.csv} | Modo: {args.mode} | Mínimo: {args.min_trades} trades")

    # Cargar todos los trades para dar contexto
    all_trades  = load_trades(args.csv, mode_filter=None)
    real_trades = [t for t in all_trades if t["mode"] == "REAL"]
    dry_trades  = [t for t in all_trades if t["mode"] == "DRY"]

    print(f"\n  Total en CSV: {len(all_trades)} trades "
          f"({len(real_trades)} REAL | {len(dry_trades)} DRY)")

    passed = False

    if args.mode in ("REAL", "ALL"):
        if not real_trades:
            print(f"\n{YELLOW}Sin trades REAL en el historial. Ejecuta el bot en modo real primero.{RESET}")
        else:
            passed = print_report(real_trades, "REAL", args.min_trades)

    if args.mode in ("DRY", "ALL"):
        print_dry_warning()
        if dry_trades:
            print_report(dry_trades, "DRY (REFERENCIA, NO VINCULANTE)", min_trades=50)

    if args.mode == "ALL" and real_trades and dry_trades:
        real_wr = compute_metrics(real_trades).get("win_rate", 0)
        dry_wr  = compute_metrics(dry_trades).get("win_rate", 0)
        gap = dry_wr - real_wr
        print(f"\n{CYAN}  COMPARATIVA DRY vs REAL{RESET}")
        print(f"  Win rate DRY:   {dry_wr*100:.1f}%")
        print(f"  Win rate REAL:  {real_wr*100:.1f}%")
        if gap > 0.05:
            print(f"  {RED}Degradación DRY→REAL: -{gap*100:.1f}pp — mayor al 5% esperado{RESET}")
            print(f"  {RED}Indica slippage/rechazos FOK significativos en producción.{RESET}")
        else:
            print(f"  {GREEN}Degradación DRY→REAL: -{gap*100:.1f}pp — dentro de rango normal{RESET}")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
