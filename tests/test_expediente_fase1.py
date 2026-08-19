"""
Tests Fase 1 — Resumen v2 y preferencias.

    SUPABASE_URL=https://dummy.supabase.co SUPABASE_KEY=dummy \
    pytest tests/test_expediente_fase1.py -v
"""

import os, sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SUPABASE_URL", "https://dummy.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy")

from fastapi import FastAPI
from fastapi.testclient import TestClient

import router_bid_resumen as mod
from router_bid_resumen import PESOS, PREFS_PERMITIDAS, _sec

app = FastAPI()
app.include_router(mod.bid_resumen_router)
client = TestClient(app, raise_server_exceptions=False)
AUTH = {"Authorization": "Bearer token-de-prueba"}


# ── D1: los pesos son la única fuente del % global ─────────

def test_pesos_suman_100():
    assert sum(PESOS.values()) == 100


def test_sec_acota_pct_entre_0_y_100():
    assert _sec("x", 1, 150, "ok", "m")["pct"] == 100
    assert _sec("x", 1, -20, "ok", "m")["pct"] == 0
    assert _sec("x", 1, 66.7, "ok", "m")["pct"] == 67  # redondeo del backend


# ── Seguridad: sin token → 401 en los 3 endpoints ──────────

@pytest.mark.parametrize("metodo,ruta", [
    ("GET",   "/api/bid/expediente/resumen-v2"),
    ("GET",   "/api/bid/expediente/prefs"),
    ("PATCH", "/api/bid/expediente/prefs"),
])
def test_sin_token_401(metodo, ruta):
    r = client.request(metodo, ruta)
    assert r.status_code == 401, f"{metodo} {ruta} devolvió {r.status_code}"


# ── Prefs: whitelist estricta ──────────────────────────────

@pytest.fixture
def como_usuario(monkeypatch):
    monkeypatch.setattr(mod, "_auth", lambda a: "user-uuid")
    monkeypatch.setattr(mod, "_empresa_id", lambda u: "empresa-uuid")
    yield


def test_pref_desconocida_422(como_usuario):
    r = client.patch("/api/bid/expediente/prefs",
                     json={"hackear": True}, headers=AUTH)
    assert r.status_code == 422


def test_prefs_permitidas_definidas():
    assert {"lente", "densidad", "proceso_activo", "seccion_activa"} == PREFS_PERMITIDAS
