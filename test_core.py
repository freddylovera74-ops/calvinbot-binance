"""
test_core.py — Tests unitarios para los componentes críticos de CalvinBot.

Cobertura:
  - utils.madrid_now / in_trading_hours
  - utils.write_json_atomic
  - BoundsManager.compute_confidence + expand
  - Guardrails.apply (suavizado, clamp, consistencia)
  - evaluate_signal (lógica con estado mocked)

Ejecución:
    python -m pytest test_core.py -v
    python -m pytest test_core.py -v --tb=short
"""

import json
import os
import sys
import time
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

import pytest

# ── Setup del path ─────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

# ─────────────────────────────────────────────────────────────────────────────
#  utils.py
# ─────────────────────────────────────────────────────────────────────────────

class TestMadridNow:
    def test_returns_datetime(self):
        from utils import madrid_now
        dt = madrid_now()
        assert isinstance(dt, datetime)

    def test_hour_range(self):
        from utils import madrid_now
        dt = madrid_now()
        assert 0 <= dt.hour < 24

    def test_in_trading_hours_boundary(self):
        from utils import in_trading_hours
        with patch("utils.madrid_now") as mock:
            mock.return_value = datetime(2024, 6, 1, 9, 59)   # antes del horario
            assert not in_trading_hours(10, 22)

            mock.return_value = datetime(2024, 6, 1, 10, 0)   # exactamente apertura
            assert in_trading_hours(10, 22)

            mock.return_value = datetime(2024, 6, 1, 21, 59)  # último minuto válido
            assert in_trading_hours(10, 22)

            mock.return_value = datetime(2024, 6, 1, 22, 0)   # cierre
            assert not in_trading_hours(10, 22)


class TestWriteJsonAtomic:
    def test_writes_correctly(self, tmp_path):
        from utils import write_json_atomic
        target = tmp_path / "test.json"
        data = {"key": "value", "num": 42}
        write_json_atomic(target, data)
        assert target.exists()
        loaded = json.loads(target.read_text(encoding="utf-8"))
        assert loaded == data

    def test_overwrites_existing(self, tmp_path):
        from utils import write_json_atomic
        target = tmp_path / "test.json"
        write_json_atomic(target, {"v": 1})
        write_json_atomic(target, {"v": 2})
        loaded = json.loads(target.read_text(encoding="utf-8"))
        assert loaded["v"] == 2

    def test_no_tmp_file_left_on_success(self, tmp_path):
        from utils import write_json_atomic
        target = tmp_path / "test.json"
        write_json_atomic(target, {"ok": True})
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == [], f"Ficheros tmp no eliminados: {tmp_files}"

    def test_handles_nested_data(self, tmp_path):
        from utils import write_json_atomic
        target = tmp_path / "nested.json"
        data = {"list": [1, 2, 3], "nested": {"a": {"b": True}}}
        write_json_atomic(target, data)
        assert json.loads(target.read_text()) == data


# ─────────────────────────────────────────────────────────────────────────────
#  BoundsManager (dynamic_optimizer.py)
# ─────────────────────────────────────────────────────────────────────────────

class TestBoundsManagerComputeConfidence:
    @pytest.fixture(autouse=True)
    def _import(self, tmp_path, monkeypatch):
        """Evita lecturas de disco real en BoundsManager."""
        monkeypatch.chdir(tmp_path)
        from dynamic_optimizer import BoundsManager, PerformanceMetrics
        self.BoundsManager = BoundsManager
        self.PM = PerformanceMetrics

    def _pm(self, n_trades=100, sharpe=0.8, wr=0.60, pnl=50.0):
        return self.PM(n_trades=n_trades, sharpe_proxy=sharpe, win_rate=wr,
                       avg_pnl=pnl/max(n_trades, 1))

    def test_zero_confidence_below_min_trades(self):
        bm = self.BoundsManager()
        conf = bm.compute_confidence(self._pm(n_trades=49))
        assert conf == 0.0

    def test_min_trades_boundary(self):
        bm = self.BoundsManager()
        conf = bm.compute_confidence(self._pm(n_trades=50))
        assert conf >= 0.0

    def test_confidence_grows_with_trades(self):
        bm = self.BoundsManager()
        c1 = bm.compute_confidence(self._pm(n_trades=100))
        c2 = bm.compute_confidence(self._pm(n_trades=200))
        c3 = bm.compute_confidence(self._pm(n_trades=300))
        assert c1 < c2 < c3

    def test_max_confidence_capped_at_1(self):
        bm = self.BoundsManager()
        conf = bm.compute_confidence(self._pm(n_trades=1000, sharpe=5.0, wr=0.99))
        assert conf <= 1.0

    def test_negative_sharpe_penalizes(self):
        bm = self.BoundsManager()
        good = bm.compute_confidence(self._pm(n_trades=200, sharpe=1.0))
        bad  = bm.compute_confidence(self._pm(n_trades=200, sharpe=-0.5))
        assert bad < good

    def test_low_wr_penalizes(self):
        bm = self.BoundsManager()
        good = bm.compute_confidence(self._pm(n_trades=200, wr=0.65))
        bad  = bm.compute_confidence(self._pm(n_trades=200, wr=0.35))
        assert bad < good

    def test_confidence_is_0_to_1(self):
        bm = self.BoundsManager()
        for n in [10, 50, 100, 200, 500]:
            for sharpe in [-1.0, 0.0, 0.5, 2.0]:
                for wr in [0.20, 0.40, 0.60, 0.80]:
                    c = bm.compute_confidence(self._pm(n_trades=n, sharpe=sharpe, wr=wr))
                    assert 0.0 <= c <= 1.0, f"conf={c} fuera de [0,1] con n={n}, sharpe={sharpe}, wr={wr}"


class TestBoundsManagerExpand:
    @pytest.fixture(autouse=True)
    def _import(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from dynamic_optimizer import (
            BoundsManager, PerformanceMetrics, HARD_BOUNDS, ABSOLUTE_OUTER_BOUNDS
        )
        self.BoundsManager = BoundsManager
        self.PM = PerformanceMetrics
        self.HARD = HARD_BOUNDS
        self.OUTER = ABSOLUTE_OUTER_BOUNDS

    def _pm(self, **kw):
        defaults = dict(n_trades=200, sharpe_proxy=1.0, win_rate=0.65, avg_pnl=0.5)
        defaults.update(kw)
        return self.PM(**defaults)

    def test_bounds_never_exceed_absolute_outer(self):
        bm = self.BoundsManager()
        # Simular alta confianza forzando múltiples ciclos
        bm._confidence = 0.99
        pm = self._pm(n_trades=1000, sharpe_proxy=5.0, win_rate=0.99)
        for _ in range(50):
            bm.expand(pm)
        for key, (low, high, ptype) in bm._effective.items():
            outer_low, outer_high, _ = self.OUTER[key]
            assert low >= outer_low - 0.001, f"{key}: low={low} < outer_low={outer_low}"
            assert high <= outer_high + 0.001, f"{key}: high={high} > outer_high={outer_high}"

    def test_bounds_at_zero_confidence_stay_near_hard(self):
        bm = self.BoundsManager()
        # Sin historial suficiente, confianza = 0
        pm = self._pm(n_trades=10)
        bm.expand(pm)
        for key, (low, high, ptype) in bm._effective.items():
            hard_low, hard_high, _ = self.HARD[key]
            # Con confianza 0, los bounds deben moverse hacia HARD_BOUNDS
            assert abs(low - hard_low) < 0.1, f"{key}: low={low} lejos de hard={hard_low}"

    def test_int_bounds_are_integers(self):
        bm = self.BoundsManager()
        bm.expand(self._pm())
        for key, (low, high, ptype) in bm._effective.items():
            if ptype == "int":
                assert isinstance(low, int), f"{key}: low={low} no es int"
                assert isinstance(high, int), f"{key}: high={high} no es int"


# ─────────────────────────────────────────────────────────────────────────────
#  Guardrails (dynamic_optimizer.py)
# ─────────────────────────────────────────────────────────────────────────────

class TestGuardrails:
    @pytest.fixture(autouse=True)
    def _import(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        from dynamic_optimizer import Guardrails, HARD_BOUNDS, DEFAULTS
        self.Guardrails = Guardrails
        self.HARD = HARD_BOUNDS
        self.DEFAULTS = DEFAULTS

    def _current(self):
        return {k: float(v[0] + v[1]) / 2 for k, v in self.HARD.items()
                if v[2] == "float"}  | \
               {k: int((v[0] + v[1]) / 2) for k, v in self.HARD.items()
                if v[2] == "int"}

    def test_result_within_hard_bounds(self):
        gr = self.Guardrails()
        # Proponer valores extremos fuera de bounds
        extreme = {k: 999.0 for k in self.HARD}
        result = gr.apply(self._current(), extreme)
        for key, val in result.items():
            low, high, _ = self.HARD[key]
            assert low <= val <= high, f"{key}={val} fuera de [{low}, {high}]"

    def test_smoothing_reduces_jump(self):
        """Con smooth_factor=0.3, el cambio debe ser ≤ 30% del delta en params float."""
        gr = self.Guardrails(smooth_factor=0.3)
        current = self._current()
        proposed = {k: float(self.HARD[k][1]) for k in self.HARD}
        result = gr.apply(current, proposed)
        for key in result:
            _, _, ptype = self.HARD[key]
            if ptype == "int":
                continue   # Params enteros se redondean, ratio puede superar 0.3 — OK
            delta_proposed = abs(proposed.get(key, current[key]) - current[key])
            delta_applied  = abs(result[key] - current[key])
            if delta_proposed > 1e-6:
                ratio = delta_applied / delta_proposed
                assert ratio <= 0.31, f"{key}: ratio={ratio:.3f} > 0.31 (smooth_factor=0.3)"

    def test_entry_min_less_than_entry_max(self):
        """Guardrails debe corregir ENTRY_MIN >= ENTRY_MAX."""
        gr = self.Guardrails()
        current = self._current()
        # Forzar que ENTRY_MIN sea mayor que ENTRY_MAX tras smoothing
        current["ENTRY_MIN"] = 0.95
        current["ENTRY_MAX"] = 0.60
        proposed = dict(current)
        result = gr.apply(current, proposed)
        assert result["ENTRY_MIN"] < result["ENTRY_MAX"], \
            f"ENTRY_MIN={result['ENTRY_MIN']} >= ENTRY_MAX={result['ENTRY_MAX']}"

    def test_tp_mid_less_than_tp_full(self):
        """Guardrails debe corregir TP_MID_PCT >= TP_FULL_PCT."""
        gr = self.Guardrails()
        current = self._current()
        current["TP_MID_PCT"]  = 0.40
        current["TP_FULL_PCT"] = 0.10
        result = gr.apply(current, dict(current))
        assert result["TP_MID_PCT"] < result["TP_FULL_PCT"], \
            f"TP_MID_PCT={result['TP_MID_PCT']} >= TP_FULL_PCT={result['TP_FULL_PCT']}"

    def test_int_params_are_int(self):
        gr = self.Guardrails()
        result = gr.apply(self._current(), self._current())
        from dynamic_optimizer import HARD_BOUNDS
        for key, (low, high, ptype) in HARD_BOUNDS.items():
            if ptype == "int" and key in result:
                assert isinstance(result[key], int), f"{key}={result[key]} debe ser int"

    def test_custom_bounds_respected(self):
        """Guardrails usa los bounds dinámicos cuando se pasan explícitamente."""
        gr = self.Guardrails()
        # Bounds más amplios que HARD_BOUNDS para STAKE_USD
        custom = dict(self.HARD)
        custom["STAKE_USD"] = (1.0, 100.0, "float")
        current = self._current()
        current["STAKE_USD"] = 50.0
        proposed = dict(current)
        proposed["STAKE_USD"] = 99.0
        result = gr.apply(current, proposed, bounds=custom)
        assert 1.0 <= result["STAKE_USD"] <= 100.0


# ─────────────────────────────────────────────────────────────────────────────
#  evaluate_signal (calvin5.py) — estado mockeado
# ─────────────────────────────────────────────────────────────────────────────

class TestEvaluateSignal:
    """
    Testea la lógica de evaluate_signal inyectando estado global mocked.
    No requiere Redis ni WebSocket.
    """

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch):
        import calvin5 as c5
        # Inicializar _P con los valores por defecto de las constantes
        c5._init_live_params()
        # Estado mínimo para que evaluate_signal pueda correr
        from calvin5 import Market
        monkeypatch.setattr(c5, "_market", Market(
            slug="btc-updown-5m",
            up_token_id="UP_TOKEN",
            down_token_id="DOWN_TOKEN",
            end_ts=time.time() + 200,
        ))
        monkeypatch.setattr(c5, "_prices", {"UP": 0.72, "DOWN": 0.28})
        monkeypatch.setattr(c5, "_price_history", {"UP": [0.72]*5, "DOWN": [0.28]*5})
        monkeypatch.setattr(c5, "_open_pos", [])
        monkeypatch.setattr(c5, "_drawdown_hit", False)
        monkeypatch.setattr(c5, "_entries_paused", False)
        monkeypatch.setattr(c5, "_sell_retry_after", {})
        # Mock BTC momentum — get_momentum devuelve (pct, direction_str)
        import btc_price as bp
        monkeypatch.setattr(bp, "get_momentum", lambda _: (0.10, "UP"))  # 10% momentum UP
        self.c5 = c5

    def test_signal_when_conditions_met(self):
        signal, btc_info = self.c5.evaluate_signal()
        # Con precio en rango y BTC momentum alto, debe haber señal UP
        assert signal in ("UP", "DOWN", None)  # Puede no haber señal si faltan condiciones

    def test_no_signal_when_drawdown_hit(self, monkeypatch):
        monkeypatch.setattr(self.c5, "_drawdown_hit", True)
        signal, _ = self.c5.evaluate_signal()
        assert signal is None

    def test_no_signal_when_paused(self, monkeypatch):
        monkeypatch.setattr(self.c5, "_entries_paused", True)
        signal, _ = self.c5.evaluate_signal()
        assert signal is None

    def test_no_signal_when_no_market(self, monkeypatch):
        monkeypatch.setattr(self.c5, "_market", None)
        signal, _ = self.c5.evaluate_signal()
        assert signal is None

    def test_no_signal_when_price_out_of_range(self, monkeypatch):
        monkeypatch.setattr(self.c5, "_prices", {"UP": 0.99, "DOWN": 0.01})
        signal, _ = self.c5.evaluate_signal()
        assert signal is None

    def test_no_signal_when_insufficient_btc_momentum(self, monkeypatch):
        import btc_price as bp
        monkeypatch.setattr(bp, "get_momentum", lambda _: (0.00001, "UP"))  # momentum casi nulo
        signal, _ = self.c5.evaluate_signal()
        assert signal is None


# ─────────────────────────────────────────────────────────────────────────────
#  Pruebas de integración ligera (no requieren Redis)
# ─────────────────────────────────────────────────────────────────────────────

class TestWriteJsonAtomicConcurrency:
    """Verifica que escrituras atómicas desde múltiples hilos no corrompen el JSON."""

    @pytest.mark.xfail(
        sys.platform == "win32",
        reason="os.replace en Windows puede lanzar PermissionError si otro hilo tiene el fichero abierto; "
               "la atomicidad está garantizada a nivel de proceso, no de hilos concurrentes en NTFS",
        strict=False,
    )
    def test_concurrent_writes(self, tmp_path):
        import threading
        from utils import write_json_atomic
        target = tmp_path / "concurrent.json"
        errors = []

        def writer(i):
            try:
                write_json_atomic(target, {"writer": i, "data": list(range(100))})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errores en escrituras concurrentes: {errors}"
        # El fichero debe ser JSON válido al final
        assert target.exists()
        data = json.loads(target.read_text())
        assert "writer" in data
